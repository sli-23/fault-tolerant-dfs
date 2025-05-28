package surfstore

import (
	context "context"
	"fmt"
	"log"
	"sync"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	serverStatus      ServerStatus
	serverStatusMutex *sync.RWMutex
	term              int64
	log               []*UpdateOperation
	id                int64
	metaStore         *MetaStore
	commitIndex       int64

	raftStateMutex *sync.RWMutex

	rpcConns   []*grpc.ClientConn
	grpcServer *grpc.Server

	//New Additions
	peers           []string
	pendingRequests []*chan PendingRequest
	lastApplied     int64

	/*--------------- Chaos Monkey --------------*/
	unreachableFrom map[int64]bool
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// Ensure that the majority of servers are up
	status := s.serverStatus
	if status == ServerStatus_CRASHED {
		return nil, ErrServerCrashed
	}
	if status != ServerStatus_LEADER {
		return nil, ErrNotLeader
	}
	for {
		majority, _ := s.SendHeartbeat(ctx, empty)
		if majority.Flag {
			break
		}
	}
	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// Ensure that the majority of servers are up
	status := s.serverStatus
	if status == ServerStatus_CRASHED {
		return nil, ErrServerCrashed
	}
	if status != ServerStatus_LEADER {
		return nil, ErrNotLeader
	}
	for {
		majority, _ := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if majority.Flag {
			break
		}
	}
	return s.metaStore.GetBlockStoreMap(ctx, hashes)

}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// Ensure that the majority of servers are up
	status := s.serverStatus
	if status == ServerStatus_CRASHED {
		return nil, ErrServerCrashed
	}
	if status != ServerStatus_LEADER {
		return nil, ErrNotLeader
	}
	for {
		majority, _ := s.SendHeartbeat(ctx, empty)
		if majority.Flag {
			break
		}
	}
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)

}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// Ensure that the request gets replicated on majority of the servers.
	// Commit the entries and then apply to the state machine

	if err := s.checkStatus(); err != nil {
		return nil, err
	}

	pendingReq := make(chan PendingRequest)
	s.raftStateMutex.Lock()
	entry := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.log = append(s.log, &entry)
	l := int64(len(s.log))
	s.pendingRequests = append(s.pendingRequests, &pendingReq)

	//TODO: Think whether it should be last or first request
	reqId := len(s.pendingRequests) - 1
	s.raftStateMutex.Unlock()

	go s.sendPersistentHeartbeats(ctx, int64(reqId))

	response := <-pendingReq
	if response.err != nil || !response.success {
		return nil, response.err
	}

	//TODO:
	// Ensure that leader commits first and then applies to the state machine
	s.raftStateMutex.Lock()
	s.commitIndex = l - 1
	for s.lastApplied < s.commitIndex {
		entry := s.log[s.lastApplied+1]
		if entry.FileMetaData == nil {
			s.lastApplied += 1
			continue
		}
		_, err := s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		if err != nil {
			s.raftStateMutex.Unlock()
			return nil, ErrServerCrashedUnreachable
		}
		s.lastApplied += 1
	}
	s.raftStateMutex.Unlock()

	if entry.FileMetaData == nil {
		//print("nil update")
		return &Version{}, nil
	}
	return s.metaStore.UpdateFile(ctx, entry.FileMetaData)
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex or whose term
// doesn't match prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	s.raftStateMutex.RLock()
	peerTerm := s.term //server term
	peerId := s.id
	s.raftStateMutex.RUnlock()
	//check the status
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()

	s.raftStateMutex.Lock()
	if serverStatus == ServerStatus_CRASHED {
		dummyAppendEntriesOutput := AppendEntryOutput{
			Term:         peerTerm,
			ServerId:     peerId,
			Success:      false,
			MatchedIndex: -1,
		}
		fmt.Println("crashed", peerId)
		s.raftStateMutex.Unlock()
		return &dummyAppendEntriesOutput, ErrServerCrashedUnreachable
	}
	s.raftStateMutex.Unlock()

	//print(s.unreachableFrom)
	s.raftStateMutex.Lock()
	//fmt.Println("id", peerId, "len", s.unreachableFrom)
	if value, ok := s.unreachableFrom[input.LeaderId]; ok && value {
		//if s.unreachableFrom[input.LeaderId] {

		dummyAppendEntriesOutput := AppendEntryOutput{
			Term:         peerTerm,
			ServerId:     peerId,
			Success:      false,
			MatchedIndex: -1,
		}
		fmt.Println("unreachable", peerId)
		s.raftStateMutex.Unlock()
		return &dummyAppendEntriesOutput, ErrServerCrashedUnreachable
	}
	s.raftStateMutex.Unlock()
	success := true
	if peerTerm > input.Term {
		dummyAppendEntriesOutput := AppendEntryOutput{
			Term:         peerTerm,
			ServerId:     peerId,
			Success:      false,
			MatchedIndex: -1,
		}
		return &dummyAppendEntriesOutput, fmt.Errorf("wrong term")
	}
	if peerTerm <= input.Term {
		s.serverStatusMutex.Lock()
		s.serverStatus = ServerStatus_FOLLOWER
		s.serverStatusMutex.Unlock()

		s.raftStateMutex.Lock()
		s.term = input.Term
		s.raftStateMutex.Unlock()

		peerTerm = input.Term
	}

	//TODO: Change this per algorithm
	// fmt.Println("from", input.LeaderId, "to", s.id, "log", input.Entries)
	// fmt.Println("log before update", s.log)
	s.raftStateMutex.Lock()
	//s.log = input.Entries
	for ind, entry := range s.log {
		s.lastApplied = int64(ind - 1)
		if ind > len(input.Entries)-1 {
			s.log = s.log[:ind]
			input.Entries = make([]*UpdateOperation, 0)
			break
		}
		if entry != input.Entries[ind] {
			s.log = s.log[:ind]
			input.Entries = input.Entries[ind:]
			break
		}
		if ind == len(s.log)-1 { //all match
			input.Entries = input.Entries[len(s.log):]
		}
	}

	// 4. Append any new entries not already in the log
	s.log = append(s.log, input.Entries...)
	//fmt.Println("leadercommit", input.LeaderCommit, "scommit", s.commitIndex)
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if input.LeaderCommit > s.commitIndex {
		if input.LeaderCommit < int64(len(s.log)-1) {
			s.commitIndex = input.LeaderCommit
		} else {
			s.commitIndex = int64(len(s.log) - 1)
		}
	}
	//fmt.Println("log after update", s.log)

	//TODO: Change per algorithm
	fmt.Println("lastapply", s.lastApplied, "commit", s.commitIndex)
	for s.lastApplied < s.commitIndex {
		entry := s.log[s.lastApplied+1]
		if entry.FileMetaData == nil {
			s.lastApplied += 1
			continue
		}
		_, err := s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		if err != nil {
			s.raftStateMutex.Unlock()
			return nil, ErrServerCrashedUnreachable
		}
		s.lastApplied += 1
	}
	dummyAppendEntriesOutput := AppendEntryOutput{
		Term:         peerTerm,
		ServerId:     peerId,
		Success:      success,
		MatchedIndex: -1,
	}
	log.Println("Server", s.id, ": Sending output:", "Term", dummyAppendEntriesOutput.Term, "Id", dummyAppendEntriesOutput.ServerId, "Success", dummyAppendEntriesOutput.Success, "Matched Index", dummyAppendEntriesOutput.MatchedIndex)
	s.raftStateMutex.Unlock()

	return &dummyAppendEntriesOutput, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()

	if serverStatus == ServerStatus_CRASHED {
		return &Success{Flag: false}, ErrServerCrashed
	}

	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_LEADER
	log.Printf("Server %d has been set as a leader", s.id)
	s.serverStatusMutex.Unlock()

	s.raftStateMutex.Lock()
	s.term += 1
	s.raftStateMutex.Unlock()

	//TODO: update the state
	// entry := UpdateOperation{
	// 	Term:         s.term,
	// 	FileMetaData: nil,
	// }
	// s.log = append(s.log, &entry)
	_, err := s.UpdateFile(ctx, nil) // Pass nil as the argument for file metadata
	if err != nil {
		return &Success{Flag: false}, err
	}

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if err := s.checkStatus(); err != nil {
		return nil, err
	}

	s.raftStateMutex.RLock()
	reqId := len(s.pendingRequests) - 1
	s.raftStateMutex.RUnlock()

	s.sendPersistentHeartbeats(ctx, int64(reqId))

	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) MakeServerUnreachableFrom(ctx context.Context, servers *UnreachableFromServers) (*Success, error) {
	s.raftStateMutex.Lock()
	for _, serverId := range servers.ServerIds {
		s.unreachableFrom[serverId] = true
	}
	log.Printf("Server %d is unreachable from", s.unreachableFrom)
	s.raftStateMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_CRASHED
	log.Printf("Server %d is crashed", s.id)
	s.serverStatusMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_FOLLOWER
	s.serverStatusMutex.Unlock()

	s.raftStateMutex.Lock()
	s.unreachableFrom = make(map[int64]bool)
	s.raftStateMutex.Unlock()

	log.Printf("Server %d is restored to follower and reachable from all servers", s.id)

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.serverStatusMutex.RLock()
	s.raftStateMutex.RLock()
	state := &RaftInternalState{
		Status:      s.serverStatus,
		Term:        s.term,
		CommitIndex: s.commitIndex,
		Log:         s.log,
		MetaMap:     fileInfoMap,
	}
	s.raftStateMutex.RUnlock()
	s.serverStatusMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
