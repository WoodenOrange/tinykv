package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	eng *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	//只用kv
	kvpath := conf.DBPath
	kv := engine_util.CreateDB(kvpath, false)
	eng := &engine_util.Engines{
		KvPath: kvpath,
		Kv:     kv,
	}
	return &StandAloneStorage{eng}
}

func (s *StandAloneStorage) Start() error {

	return nil
}

func (s *StandAloneStorage) Stop() error {
	if s.eng != nil {
		return s.eng.Close()
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.eng.Kv.NewTransaction(false)
	return &standAloneStorageReader{txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	//开启批处理
	wb := new(engine_util.WriteBatch)
	for _, modify := range batch {
		//分类型对应操作
		switch modify.Data.(type) {
		case storage.Put:
			wb.SetCF(modify.Cf(), modify.Key(), modify.Value())
		case storage.Delete:
			wb.DeleteCF(modify.Cf(), modify.Key())
		default:
			return errors.New("unsupported modify type")
		}
	}
	return wb.WriteToDB(s.eng.Kv)
}

// standAloneStorageReader 实现 storage.StorageReader 接口
type standAloneStorageReader struct {
	txn *badger.Txn
}

func (r *standAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *standAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *standAloneStorageReader) Close() {
	r.txn.Discard()
}
