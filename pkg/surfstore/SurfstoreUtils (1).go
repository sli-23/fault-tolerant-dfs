package surfstore

import (
	"fmt"
	"log"
	"math"
	"os"
	reflect "reflect"
)

func ClientSync(client RPCClient) {
	//fmt.Print(client.BaseDir)
	//print("\n")
	metaFile, err := LoadMetaFromMetaFile(client.BaseDir)
	//print("local map before update\n")
	//PrintMetaMap(metaFile)

	if err != nil {
		log.Fatalf("failed to open")
	}
	files, err := os.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatalf("failed to open")
	}
	//Files := make([]string, 0)
	for _, file := range files {
		if file.Name() == DEFAULT_META_FILENAME || file.IsDir() {
			continue
		}
		//Files = append(Files, file.Name())
		if err != nil {
			log.Fatalf("failed to open")
		}
		fileInfo, _ := os.Stat(ConcatPath(client.BaseDir, file.Name()))
		BlockNumber := int(math.Ceil(float64(fileInfo.Size()) / float64(client.BlockSize)))
		//fmt.Println("blocksize: ", BlockNumber)
		newHash := make([]string, BlockNumber)
		fileData, err := os.Open(client.BaseDir + "/" + file.Name())
		if err != nil {
			fmt.Println("Error opening file: " + file.Name())
			os.Exit(1)
		}
		for i := 0; i < BlockNumber; i++ {

			blockData := make([]byte, client.BlockSize)
			n, _ := fileData.Read(blockData)
			blockData = blockData[:n]
			if n == 0 {
				newHash[i] = "-1"
			}

			newHash[i] = GetBlockHashString(blockData)
		}
		//fmt.Println("new: ", newHash)
		indexBlockHashList, ok := metaFile[file.Name()]
		if !ok {
			metaFile[file.Name()] = &FileMetaData{Filename: file.Name(), Version: 1, BlockHashList: newHash}
			//fmt.Println("create", file.Name())

		} else {
			if !reflect.DeepEqual(indexBlockHashList.BlockHashList, newHash) {
				metaFile[file.Name()].Version += 1
				metaFile[file.Name()].BlockHashList = newHash
				//fmt.Println("Updated ", file.Name())

			}

		}
	}
	//PrintMetaMap(metaFile)
	// delete

	for fileName, metadata := range metaFile {
		exist := false
		for _, file := range files {
			if file.Name() == fileName {
				exist = true
				break
			}
		}
		//if !Contains(Files, fileName) && !(len(metadata.BlockHashList) == 1 && metadata.BlockHashList[0] == "0") {
		if !exist && !(len(metadata.BlockHashList) == 1 && metadata.BlockHashList[0] == "0") {
			//fmt.Println("delete ", fileName)
			metadata.Version += 1
			metadata.BlockHashList = []string{"0"}
		}
	}
	// print("local map after update\n")
	// PrintMetaMap(metaFile)
	remoteFile := make(map[string]*FileMetaData)

	err = client.GetFileInfoMap(&remoteFile)
	// print("remote map before update\n")
	// PrintMetaMap(remoteFile)
	if err != nil {
		log.Fatalf("failed to open")
	}
	// var addr []string
	// err = client.GetBlockStoreAddrs(&addr)

	// if err != nil {
	// 	log.Fatalf("failed to open")
	// }

	// download
	for filename, remoteMetaData := range remoteFile {
		localMetaData, ok := metaFile[filename]
		if !ok {
			localMetaData = &FileMetaData{}
			err = download(client, remoteMetaData, localMetaData)
			metaFile[filename] = localMetaData
			if err != nil {
				log.Fatalf("failed to open")
			}
		} else {
			if remoteMetaData.Version >= localMetaData.Version {
				err := download(client, remoteMetaData, localMetaData)
				if err != nil {
					log.Fatalf("failed to open")
				}
			}

		}

	}
	// print("after download local map\n")
	// PrintMetaMap(metaFile)
	// upload

	for filename, localMetaData := range metaFile {

		remoteMetaData, ok := remoteFile[filename]
		if ok {
			if localMetaData.Version == remoteMetaData.Version+1 {
				err := upload(client, localMetaData)
				if err != nil {
					log.Fatalf("failed to open")
				}
			}
		} else {
			err = upload(client, localMetaData)
			if err != nil {
				log.Fatalf("failed to open")
			}
		}

	}
	//print("after upload remote map\n")
	client.GetFileInfoMap(&remoteFile)
	//PrintMetaMap(remoteFile)

	err = WriteMetaFile(metaFile, client.BaseDir)
	if err != nil {
		log.Fatalf("failed to open")
	}
}
func download(client RPCClient, remoteMetaData *FileMetaData, localMetaData *FileMetaData) error {
	remoteBlockList := remoteMetaData.GetBlockHashList()
	localBlockList := localMetaData.GetBlockHashList()
	//*localMetaData = *remoteMetaData
	*localMetaData = FileMetaData{
		Filename:      remoteMetaData.Filename,
		Version:       remoteMetaData.Version,
		BlockHashList: remoteMetaData.BlockHashList,
	}
	path := ConcatPath(client.BaseDir, remoteMetaData.Filename)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {

		log.Fatalf("failed to open")
	}
	// download blocks
	defer file.Close()
	if len(remoteMetaData.BlockHashList) == 1 && remoteMetaData.BlockHashList[0] == "0" {
		err = os.Remove(file.Name())
		if err != nil {
			log.Fatalf("failed to open")
		} else {
			return nil
		}
	}

	BlockStoreMap := make(map[string][]string)
	err = client.GetBlockStoreMap(remoteBlockList, &BlockStoreMap)
	if err != nil {
		return err
	}
	for index, hash := range remoteBlockList {
		exist := false
		for _, h := range localBlockList {
			if h == hash {
				exist = true
				break
			}
		}
		//if !Contains(localBlockList, hash) {
		if !exist {
			var block Block
			var S string
			flag := false
			for server := range BlockStoreMap {
				exist := false
				for _, h := range BlockStoreMap[server] {
					if h == hash {
						exist = true
						break
					}
				}
				//if Contains(BlockStoreMap[server], hash) {
				if exist {
					S = server
					flag = true
					break
				}
			}
			if !flag {
				return fmt.Errorf("can't get responsibleServer")
			}

			err := client.GetBlock(hash, S, &block)

			if err != nil {
				log.Fatalf("failed to open")
			}
			n, _ := file.WriteAt(block.BlockData, int64(index*client.BlockSize))
			err = file.Sync()
			if err != nil {
				log.Fatalf("failed to open")
			}

			if index == len(remoteBlockList)-1 {
				err := os.Truncate(path, int64(index*client.BlockSize+n))
				if err != nil {
					log.Fatalf("failed to open")
				}
			}
		}

	}
	return nil
}
func upload(client RPCClient, localMetaData *FileMetaData) error {
	//localBlockList := make //localMetaData.GetBlockHashList()
	file, err := os.Open(ConcatPath(client.BaseDir, localMetaData.Filename))

	var latestVersion int32
	if err != nil {
		// deleted
		if os.IsNotExist(err) {
			client.UpdateFile(localMetaData, &latestVersion)
			localMetaData.Version = latestVersion
			return nil
		} else {
			log.Fatalf("failed to open")
			return nil
		}
	}
	defer file.Close()
	BlockStoreMap := make(map[string][]string)
	localBlockList := []string{}
	localBlockList = localMetaData.GetBlockHashList()
	if err = client.GetBlockStoreMap(localBlockList, &BlockStoreMap); err != nil {
		return err
	}
	for _, hash := range localBlockList {
		var S string
		flag := false
		for server := range BlockStoreMap {
			exist := false
			for _, h := range BlockStoreMap[server] {
				if h == hash {
					exist = true
					break
				}
			}
			//if Contains(BlockStoreMap[server], hash) {
			if exist {
				S = server
				flag = true
				break
			}
		}
		if !flag {
			log.Fatalf("can't get Server")
			return nil
		}

		blockData := make([]byte, client.BlockSize)
		n, err := file.Read(blockData)
		if err != nil {
			log.Fatalf("failed to open")
		}
		blockData = blockData[:n]
		block := Block{BlockData: blockData, BlockSize: int32(n)}
		var success bool
		client.PutBlock(&block, S, &success)
		if !success {
			log.Fatalf("failed to open")
		}
	}
	err = client.UpdateFile(localMetaData, &latestVersion)
	if err != nil {
		return err
	} else if latestVersion == -1 { //dele
		localMetaData.Version = latestVersion
	}
	return nil

}

// func Contains(set []string, target string) bool {
// 	for _, element := range set {
// 		if element == target {
// 			return true
// 		}
// 	}
// 	return false
// }
