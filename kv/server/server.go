package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
// 读单key
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := new(kvrpcpb.GetResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		//region错误，装进response，客户端会重试
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		//非业务错误，直接返回
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	//查锁，如果被之前的锁锁住，返回锁信息
	if lock.IsLockedFor(req.Key, req.Version, resp) {
		return resp, nil
	}
	//读
	resp.Value, err = txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	resp.NotFound = resp.Value == nil
	return resp, nil
}

// Transactional API.
func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := new(kvrpcpb.PrewriteResponse)
	//收集所有key，用于latch
	keys := make([][]byte, 0, len(req.Mutations))
	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	//等待latch后再取快照，确保能看到前一个写请求的结果
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, mutation := range req.Mutations {
		//写冲突，commit_ts有晚于当前start_ts则将加入KeyError
		write, commitTS, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && commitTS >= req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
				StartTs: req.StartVersion, ConflictTs: commitTS, Key: mutation.Key, Primary: req.PrimaryLock,
			}})
			continue
		}
		//锁冲突，有其他事务的锁则将加入KeyError
		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts != req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Locked: lock.Info(mutation.Key)})
			continue
		}
		var kind mvcc.WriteKind

		//写入预写值和锁
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			kind = mvcc.WriteKindPut
			txn.PutValue(mutation.Key, mutation.Value)
		case kvrpcpb.Op_Del:
			kind = mvcc.WriteKindDelete
			txn.DeleteValue(mutation.Key)
		default:
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Abort: "unsupported prewrite operation"})
			continue
		}
		txn.PutLock(mutation.Key, &mvcc.Lock{
			Primary: req.PrimaryLock, Ts: req.StartVersion, Ttl: req.LockTtl, Kind: kind,
		})
	}
	if len(resp.Errors) != 0 {
		return resp, nil
	}
	server.Latches.Validate(txn, keys)
	//落盘
	if len(txn.Writes()) != 0 {
		if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
	}
	return resp, nil
}

// Transactional API.
// 批量提交：保留预写值，写入提交记录并删除本事务锁
func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := new(kvrpcpb.CommitResponse)
	if req.CommitVersion <= req.StartVersion {
		resp.Error = &kvrpcpb.KeyError{Abort: "commit version must be greater than start version"}
		return resp, nil
	}
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		//写过的key，如果是回滚，返回错误
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{Abort: "transaction already rolled back"}
				return resp, nil
			}
			//已提交，跳过
			continue
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			// 兼容现有 MissingPrewrite 测试：无锁、无记录时不做修改。
			continue
		}
		//被其他锁住，返回错误
		if lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{Retryable: "lock belongs to another transaction"}
			return resp, nil
		}
		//写write，删除锁
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: lock.Kind})
		txn.DeleteLock(key)
	}
	server.Latches.Validate(txn, req.Keys)
	//落盘
	if len(txn.Writes()) != 0 {
		if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
