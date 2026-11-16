package mvcc

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Scanner 合并提交记录和锁的 key，保证只有预写锁的 key 也能报告冲突。
type Scanner struct {
	// Your Data Here (4C).
	txn    *MvccTxn
	writes engine_util.DBIterator
	locks  engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scan := &Scanner{txn: txn, writes: txn.Reader.IterCF(engine_util.CfWrite), locks: txn.Reader.IterCF(engine_util.CfLock)}
	scan.writes.Seek(EncodeKey(startKey, TsMax))
	scan.locks.Seek(startKey)
	return scan
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	if scan.writes != nil {
		scan.writes.Close()
		scan.writes = nil
	}
	if scan.locks != nil {
		scan.locks.Close()
		scan.locks = nil
	}
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.writes == nil {
		return nil, nil, nil
	}
	for scan.writes.Valid() || scan.locks.Valid() {
		var key []byte
		if scan.writes.Valid() {
			key = DecodeUserKey(scan.writes.Item().Key())
		}
		if scan.locks.Valid() && (key == nil || bytes.Compare(scan.locks.Item().Key(), key) < 0) {
			key = scan.locks.Item().KeyCopy(nil)
		}
		// 移动迭代器前复制 key，避免底层复用缓冲区。
		key = append([]byte{}, key...)
		for scan.writes.Valid() && bytes.Equal(DecodeUserKey(scan.writes.Item().Key()), key) {
			scan.writes.Next()
		}
		if scan.locks.Valid() && bytes.Equal(scan.locks.Item().Key(), key) {
			scan.locks.Next()
		}
		lock, err := scan.txn.GetLock(key)
		if err != nil {
			return key, nil, err
		}
		pair := new(kvrpcpb.KvPair)
		if lock.IsLockedFor(key, scan.txn.StartTS, pair) {
			return key, nil, &KeyError{KeyError: *pair.Error}
		}
		// 复用 4A 的版本选择规则，回滚标记不会遮蔽旧版本。
		value, err := scan.txn.GetValue(key)
		if err != nil {
			return key, nil, err
		}
		if value != nil {
			return key, value, nil
		}
	}
	return nil, nil, nil
}
