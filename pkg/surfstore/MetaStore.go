package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	infoMap := &FileInfoMap{FileInfoMap: m.FileMetaMap}
	return infoMap, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	FileName := fileMetaData.Filename
	newVer := fileMetaData.Version
	version := newVer
	_, err := m.FileMetaMap[FileName]
	if !err {
		m.FileMetaMap[FileName] = fileMetaData
	} else {
		if newVer == m.FileMetaMap[FileName].Version+1 {
			m.FileMetaMap[FileName] = fileMetaData

		} else {
			version = m.FileMetaMap[FileName].Version
		}
	}
	newVersion := &Version{Version: version}
	return newVersion, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	blockStoreMap := make(map[string]*BlockHashes)
	for _, Hash := range blockHashesIn.Hashes {
		Server := m.ConsistentHashRing.GetResponsibleServer(Hash)
		block, ok := blockStoreMap[Server]
		if !ok {
			blockStoreMap[Server] = &BlockHashes{Hashes: make([]string, 0)}
			block = blockStoreMap[Server]
		}
		blockStoreMap[Server].Hashes = append(block.Hashes, Hash)
	}
	blockMap := &BlockStoreMap{BlockStoreMap: blockStoreMap}
	return blockMap, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
