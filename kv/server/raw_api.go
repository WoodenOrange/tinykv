package server

import (
	"context"

	//"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	//打开reader
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	//读之
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	//如果键不存在
	if val == nil {
		return &kvrpcpb.RawGetResponse{Value: val, NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: val}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// 放入modify
	modify := storage.Modify{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		},
	}
	// 写之
	err := server.storage.Write(nil, []storage.Modify{modify})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// 放入modify
	modify := storage.Modify{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	}
	//写之
	err := server.storage.Write(nil, []storage.Modify{modify})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	//开reader
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	//开迭代器
	iter := reader.IterCF(req.Cf)
	defer iter.Close()

	//遍历加入
	var pairs []*kvrpcpb.KvPair
	count := uint32(0)
	for iter.Seek(req.StartKey); iter.Valid() && count < req.Limit; iter.Next() {
		item := iter.Item()
		key := item.KeyCopy(nil)
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		})
		count++
	}
	return &kvrpcpb.RawScanResponse{Kvs: pairs}, nil
}
