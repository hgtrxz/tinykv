package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// So you should do all read/write operations through engine_util provided methods.
// Please read util/engine_util/doc.go to learn more.

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
	config  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	// badger.DB
	kvPath := filepath.Join(conf.DBPath, "kv")
	DB := engine_util.CreateDB(kvPath, false)
	engines := engine_util.NewEngines(DB, nil, kvPath, "")
	return &StandAloneStorage{
		engines: engines,
		config:  conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.engines.Kv.Close(); err != nil {
		return err
	}
	return nil
}

// Reader
// You should use badger.Txn to implement the Reader function.
// Because the transaction handler provided by badger could provide a consistent snapshot of the keys and values.
// Don’t forget to call Discard() for badger.Txn and close all iterators before discarding.
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engines.Kv.NewTransaction(false)
	return NewStandaloneReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := new(engine_util.WriteBatch)
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			put := modify.Data.(storage.Put)
			if err := engine_util.PutCF(s.engines.Kv, put.Cf, put.Key, put.Value); err != nil {
				return err
			}
		case storage.Delete:
			del := modify.Data.(storage.Delete)
			if err := engine_util.DeleteCF(s.engines.Kv, del.Cf, del.Key); err != nil {
				return err
			}
		}
	}
	return s.engines.WriteKV(wb)
}
