package research

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	leveldb_errors "github.com/syndtr/goleveldb/leveldb/errors"
	leveldb_opt "github.com/syndtr/goleveldb/leveldb/opt"
	"strconv"

	"path/filepath"
)

var contractRWDir = filepath.Join("contract-rw")
var contractRWDB *leveldb.DB

func OpenContractRWDB() {
	fmt.Println("stage1-substate: OpenContractRWDB")

	var err error
	var opt leveldb_opt.Options
	var path string

	// increase BlockCacheCapacity to 1GiB
	opt.BlockCacheCapacity = 1 * leveldb_opt.GiB
	// decrease OpenFilesCacheCapacity to avoid "Too many file opened" error
	opt.OpenFilesCacheCapacity = 50

	dbNameMap := map[string]*leveldb.DB{
		"contractrw": nil,
	}

	for name := range dbNameMap {
		var db *leveldb.DB
		path = filepath.Join(contractRWDir, name)
		db, err = leveldb.OpenFile(path, &opt)
		if _, corrupted := err.(*leveldb_errors.ErrCorrupted); corrupted {
			db, err = leveldb.RecoverFile(path, &opt)
		}
		if err != nil {
			panic(fmt.Errorf("error opening substate leveldb %s: %v", path, err))
		}

		fmt.Printf("stage1-substate: successfully opened %s leveldb\n", name)

		dbNameMap[name] = db
	}

	contractRWDB = dbNameMap["contractrw"]
}

func CloseContractRWDB() {
	defer fmt.Println("stage1-substate: CloseContractRWDB")

	dbNameMap := map[string]*leveldb.DB{
		"contractrw": contractRWDB,
	}

	for name, db := range dbNameMap {
		db.Close()
		fmt.Printf("stage1-substate: successfully closed %s leveldb\n", name)
	}
}

func GetRWSet(blockIndex uint64, txIndex uint64, id int) []byte {
	rwset, err := contractRWDB.Get([]byte(strconv.FormatUint(blockIndex, 10)+"-"+strconv.FormatUint(txIndex, 10)+"-"+strconv.Itoa(id)), nil)
	if err != nil {
		panic(fmt.Errorf("stage1-substate: error get blockIndex %d txIndex %v: %v", blockIndex, txIndex, err))
	}
	return rwset
}

func PutRWSet(blockIndex uint64, txIndex uint64, rwset []byte) {
	id := 0
	for {
		if ok, _ := contractRWDB.Has([]byte(strconv.FormatUint(blockIndex, 10)+"-"+strconv.FormatUint(txIndex, 10)+"-"+strconv.Itoa(id)), nil); ok {
			id += 1
		} else {
			break
		}
	}
	err := contractRWDB.Put([]byte(strconv.FormatUint(blockIndex, 10)+"-"+strconv.FormatUint(txIndex, 10)+"-"+strconv.Itoa(id)), rwset, nil)
	if err != nil {
		panic(fmt.Errorf("stage1-substate: error put blockIndex %d txIndex %v: %v", blockIndex, txIndex, err))
	}
}
