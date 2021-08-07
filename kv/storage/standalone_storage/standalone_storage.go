package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	// base config
	addr   string
	path   string
	logger *log.Logger
	// storage engine
	engines *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	logger := log.New()
	logger.SetLevel(log.StringToLogLevel(conf.LogLevel))

	return &StandAloneStorage{
		logger: logger,
		addr:   conf.StoreAddr,
		path:   conf.DBPath,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.logger.Infof("stand alone storage start...")
	s.engines = engine_util.NewEngines(
		engine_util.CreateDB(s.path, false),
		nil,
		s.path,
		"",
	)
	s.logger.Infof("storage start success")
	s.logger.Infof("store address: %s", s.addr)
	s.logger.Infof("db path: %s", s.path)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.engines.Kv.Close()
	if err != nil {
		s.logger.Errorf("close KV engine failed && err=%+v", err)
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return s.GetReader(), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writeBatch := &engine_util.WriteBatch{}
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			writeBatch.SetCF(m.Cf(), m.Key(), m.Value())
		case storage.Delete:
			writeBatch.DeleteCF(m.Cf(), m.Key())
		}
	}
	return s.engines.WriteKV(writeBatch)
}

func (s *StandAloneStorage) GetReader() *reader {
	return &reader{
		txn: s.engines.Kv.NewTransaction(false),
	}
}

type reader struct {
	txn *badger.Txn
}

func (r *reader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *reader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *reader) Close() {
	r.txn.Discard()
}
