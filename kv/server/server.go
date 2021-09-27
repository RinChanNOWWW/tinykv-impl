package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
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

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	log.Infof("RawGet cf=%v, key=%v", req.GetCf(), req.GetKey())
	rsp := new(kvrpcpb.RawGetResponse)
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		log.Errorf("RawGet failed && err=%+v", err)
		rsp.Error = err.Error()
		rsp.NotFound = true
		return rsp, nil
	}
	if len(val) == 0 {
		rsp.NotFound = true
	}
	rsp.Value = val
	return rsp, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	log.Infof("RawPut cf=%v, key=%v, value=%v", req.GetCf(), req.GetKey(), req.GetValue())
	rsp := new(kvrpcpb.RawPutResponse)
	modify := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	}
	err := server.storage.Write(req.Context, modify)
	if err != nil {
		log.Errorf("RawPut failed && err=%+v", err)
		rsp.Error = err.Error()
	}
	return rsp, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	log.Infof("RawDelete cf=%v, key=%v", req.GetCf(), req.GetKey())
	rsp := new(kvrpcpb.RawDeleteResponse)
	modify := []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	}
	err := server.storage.Write(req.Context, modify)
	if err != nil {
		log.Errorf("RawDelete failed && err=%+v", err)
		rsp.Error = err.Error()
	}
	return rsp, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	log.Infof("RawScan cf=%v, start=%v, limit=%v", req.GetCf(), req.GetStartKey(), req.GetLimit())
	rsp := new(kvrpcpb.RawScanResponse)
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	iter.Seek(req.StartKey)
	for i := uint32(0); iter.Valid() && i < req.Limit; i += 1 {
		item := iter.Item()
		key := item.Key()
		val, err := item.Value()
		if err != nil {
			log.Warnf("RawScan items error occurs && key=%v, err=%+v", key, err)
		}
		rsp.Kvs = append(rsp.Kvs,
			&kvrpcpb.KvPair{
				Key:   key,
				Value: val,
			},
		)
		iter.Next()
	}
	return rsp, nil
}

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
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	rsp := &kvrpcpb.GetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	if lock != nil && req.Version >= lock.Ts {
		rsp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			},
		}
		return rsp, nil
	}
	value, err := txn.GetValue(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	if value == nil {
		rsp.NotFound = true
	}
	rsp.Value = value
	return rsp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	rsp := &kvrpcpb.PrewriteResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	keyErrors := make([]*kvrpcpb.KeyError, 0)
	for _, mutation := range req.Mutations {
		write, ts, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				rsp.RegionError = regionErr.RequestErr
				return rsp, nil
			}
			return nil, err
		}
		// key already written
		if write != nil && req.StartVersion <= ts {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: ts,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}
		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				rsp.RegionError = regionErr.RequestErr
				return rsp, nil
			}
			return nil, err
		}
		// key already locked
		if lock != nil && lock.Ts != req.StartVersion {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         mutation.Key,
					LockTtl:     lock.Ttl,
				},
			})
			continue
		}
		// safe to write
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mutation.Key, mutation.Value)
			txn.PutLock(mutation.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      req.StartVersion,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindPut,
			})
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mutation.Key)
			txn.PutLock(mutation.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      req.StartVersion,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindDelete,
			})
		}
	}
	if len(keyErrors) > 0 {
		rsp.Errors = keyErrors
		return rsp, nil
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	return rsp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	rsp := &kvrpcpb.CommitResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				rsp.RegionError = regionErr.RequestErr
				return rsp, nil
			}
			return nil, err
		}
		if lock == nil {
			return rsp, nil
		}
		// key locked by another key, retry
		if lock.Ts != req.StartVersion {
			rsp.Error = &kvrpcpb.KeyError{
				Retryable: "true",
			}
			return rsp, nil
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	return rsp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	rsp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	kvs := make([]*kvrpcpb.KvPair, 0)
	for i := uint32(0); i < req.Limit; {
		k, v, err := scanner.Next()
		if k == nil && v == nil && err == nil {
			break
		}
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				rsp.RegionError = regionErr.RequestErr
				return rsp, nil
			}
			return nil, err
		}
		lock, err := txn.GetLock(k)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				rsp.RegionError = regionErr.RequestErr
				return rsp, nil
			}
			return nil, err
		}
		if lock != nil && req.Version >= lock.Ts {
			kvs = append(kvs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         k,
						LockTtl:     lock.Ttl,
					},
				},
			})
			i++
		} else if v != nil {
			kvs = append(kvs, &kvrpcpb.KvPair{Key: k, Value: v})
			i++
		}
	}
	rsp.Pairs = kvs
	return rsp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	rsp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	// If the transaction has previously been rolled back or committed,
	// return that information.
	write, ts, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			rsp.CommitVersion = ts
		}
		return rsp, nil
	}
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	if lock == nil {
		// The lock does not exist, TinyKV left a record of the rollback,
		// but did not have to delete a lock.
		txn.Rollback(req.PrimaryKey, false)
		rsp.Action = kvrpcpb.Action_LockNotExistRollback
		rsp.CommitVersion = 0
		rsp.LockTtl = 0
		if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				rsp.RegionError = regionErr.RequestErr
				return rsp, nil
			}
			return nil, err
		}
		return rsp, nil
	}
	// If the TTL of the transaction is exhausted,
	// abort that transaction and roll back the primary lock.
	if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) {
		txn.Rollback(req.PrimaryKey, true)
		rsp.Action = kvrpcpb.Action_TTLExpireRollback
		if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				rsp.RegionError = regionErr.RequestErr
				return rsp, nil
			}
			return nil, err
		}
		return rsp, nil
	}
	rsp.LockTtl = lock.Ttl
	return rsp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	rsp := &kvrpcpb.BatchRollbackResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		// if the transaction has already been committed
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				rsp.RegionError = regionErr.RequestErr
				return rsp, nil
			}
			return nil, err
		}
		if write != nil {
			if write.Kind != mvcc.WriteKindRollback {
				rsp.Error = &kvrpcpb.KeyError{
					Abort: "true",
				}
				return rsp, nil
			}
			// already rollback
			continue
		}
		// or keys are locked by a different transaction
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				rsp.RegionError = regionErr.RequestErr
				return rsp, nil
			}
			return nil, err
		}
		if lock != nil && lock.Ts != txn.StartTS {
			txn.Rollback(key, false)
			continue
		}
		value, err := txn.CurrentValue(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				rsp.RegionError = regionErr.RequestErr
				return rsp, nil
			}
			return nil, err
		}
		// missing prewrite
		if value == nil {
			txn.Rollback(key, true)
			continue
		}
		// If the keys were never locked,
		// no action is needed but it is not an error.
		if lock == nil {
			continue
		}
		txn.Rollback(key, true)

	}
	writes := txn.Writes()
	err = server.storage.Write(req.Context, writes)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	return rsp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	rsp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	defer reader.Close()
	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	keys := make([][]byte, 0)
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		value, err := item.Value()
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				rsp.RegionError = regionErr.RequestErr
				return rsp, nil
			}
			return nil, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				rsp.RegionError = regionErr.RequestErr
				return rsp, nil
			}
			return nil, err
		}
		if lock.Ts == req.StartVersion {
			keys = append(keys, item.Key())
		}
	}
	if len(keys) == 0 {
		return rsp, nil
	}
	// If commit_version is 0,
	// TinyKV will rollback all locks.
	// If commit_version is greater than 0
	// it will commit those locks with the given commit timestamp.
	if req.CommitVersion == 0 {
		r, err := server.KvBatchRollback(context.TODO(), &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		rsp.Error = r.Error
		rsp.RegionError = r.RegionError
		return rsp, err
	}
	r, err := server.KvCommit(context.TODO(), &kvrpcpb.CommitRequest{
		Context:       req.Context,
		StartVersion:  req.StartVersion,
		Keys:          keys,
		CommitVersion: req.CommitVersion,
	})
	rsp.Error = r.Error
	rsp.RegionError = r.RegionError
	return rsp, err

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
