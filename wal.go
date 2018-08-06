package raftwal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/hashicorp/raft"
	"github.com/y0ssar1an/q"
)

const (
	metaPageSize = 4096
)

var ErrKeyNotFound = errors.New("not found")

type WAL struct {
	meta       map[string][]byte
	path       string
	f          *os.File
	index      []uint32
	minIndex   uint64
	nextOffset int64
	mu         sync.Mutex
	pool       sync.Pool
}

func New(dir string) (*WAL, error) {
	path := dir + "/raft.wal"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	// Pre-allocate 128MB for now
	err = fileutil.Preallocate(f, 128*1024*1024, true)
	if err != nil {
		return nil, err
	}
	m := make(map[string][]byte)
	metaBytes := make([]byte, metaPageSize)
	_, err = f.ReadAt(metaBytes, 0)
	if err != nil {
		return nil, err
	}

	if metaBytes[0] == '{' {
		json.Unmarshal(metaBytes, m)
	}

	return &WAL{
		meta:       m,
		path:       path,
		f:          f,
		index:      make([]uint32, 0, 128*1024),
		nextOffset: metaPageSize,
		pool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 0, 1024))
			},
		},
	}, nil
}

func (w *WAL) Close() error {
	return w.f.Close()
}

// FirstIndex returns the first index written. 0 for no entries.
func (w *WAL) FirstIndex() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.minIndex > 0 {
		return w.minIndex, nil
	}
	if len(w.index) == 0 {
		return 0, nil
	}
	return 1, nil
}

// LastIndex returns the last index written. 0 for no entries.
func (w *WAL) LastIndex() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.index) == 0 {
		return 0, nil
	}
	// Index is off-by-one: index[0] is log.Index == 1
	return uint64(len(w.index)), nil
}

// GetLog gets a log entry at a given index.
func (w *WAL) GetLog(index uint64, log *raft.Log) error {
	w.mu.Lock()

	if index > uint64(len(w.index)) || index < w.minIndex {
		w.mu.Unlock()
		return raft.ErrLogNotFound
	}

	offset := w.index[int(index-1)]
	w.mu.Unlock()

	return w.readAt(offset, log)
}

func (w *WAL) readAt(offset uint32, log *raft.Log) error {
	of64 := int64(offset)

	// Read 4 byte length prefix
	var prefix [4]byte
	_, err := w.f.ReadAt(prefix[:], of64)
	if err != nil {
		return err
	}

	len := binary.BigEndian.Uint32(prefix[:])

	if len < 17 {
		return errors.New("corrupt")
	}

	buf := make([]byte, len)
	_, err = w.f.ReadAt(buf, of64+4)
	if err != nil {
		return err
	}

	log.Index = binary.BigEndian.Uint64(buf[0:8])
	log.Term = binary.BigEndian.Uint64(buf[8:16])
	log.Type = raft.LogType(buf[16])
	log.Data = buf[17:]
	return nil
}

// StoreLog stores a log entry.
func (w *WAL) StoreLog(log *raft.Log) error {
	return w.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries.
func (w *WAL) StoreLogs(logs []*raft.Log) error {
	length := 0
	for _, l := range logs {
		length += 4 + 8 + 8 + 1 + len(l.Data)
	}

	buf := w.pool.Get().(*bytes.Buffer)
	defer w.pool.Put(buf)
	offset := int64(0)

	w.mu.Lock()
	defer w.mu.Unlock()

	// r, err := w.f.Seek(w.nextOffset, 0)
	// if err != nil {
	// 	return err
	// }
	// if r != w.nextOffset {
	// 	return fmt.Errorf("failed to seek")
	// }

	for _, l := range logs {
		w.index = append(w.index, uint32(w.nextOffset+offset))

		// err = binary.Write(w.f, binary.BigEndian, uint32(8+8+1+len(l.Data)))
		// if err != nil {
		// 	return err
		// }
		// err = binary.Write(w.f, binary.BigEndian, l.Index)
		// if err != nil {
		// 	return err
		// }
		// err = binary.Write(w.f, binary.BigEndian, l.Term)
		// if err != nil {
		// 	return err
		// }
		// err = binary.Write(w.f, binary.BigEndian, l.Type)
		// if err != nil {
		// 	return err
		// }
		// n, err := w.f.Write(l.Data)
		// if err != nil {
		// 	return err
		// }
		// if n != len(l.Data) {
		// 	return fmt.Errorf("didn't write all the bytes")
		// }

		err := binary.Write(buf, binary.BigEndian, uint32(8+8+1+len(l.Data)))
		if err != nil {
			return err
		}
		err = binary.Write(buf, binary.BigEndian, l.Index)
		if err != nil {
			return err
		}
		err = binary.Write(buf, binary.BigEndian, l.Term)
		if err != nil {
			return err
		}
		err = binary.Write(buf, binary.BigEndian, l.Type)
		if err != nil {
			return err
		}
		n, err := buf.Write(l.Data)
		if err != nil {
			return err
		}
		if n != len(l.Data) {
			return fmt.Errorf("didn't write all the bytes")
		}

		// Encode length prefix
		// binary.BigEndian.PutUint32(buf[offset:offset+4],
		// 	uint32(8+8+1+len(l.Data)))
		// // Encode index
		// binary.BigEndian.PutUint64(buf[offset+4:offset+12], l.Index)
		// // Encode length prefix
		// binary.BigEndian.PutUint64(buf[offset+12:offset+20], l.Term)
		// buf[offset+20] = uint8(l.Type)
		// copy(buf[offset+21:], l.Data)
		offset += 4 + 8 + 8 + 1 + int64(len(l.Data))
	}
	q.Q("Writing logs", length, len(logs))
	// if len(logs) == 1 {
	// 	typ := 1024
	// 	if len(logs[0].Data) > 0 {
	// 		typ = int(logs[0].Data[0] & 127)
	// 	}
	// 	q.Q("Log 1", logs[0].Type, typ)
	// }
	_, err := w.f.WriteAt(buf.Bytes(), w.nextOffset)
	if err != nil {
		return err
	}
	err = fileutil.Fdatasync(w.f)
	//err = w.f.Sync()
	if err != nil {
		return err
	}
	w.nextOffset += int64(length)
	return nil
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (w *WAL) DeleteRange(min, max uint64) error {
	w.mu.Lock()
	w.mu.Unlock()
	w.minIndex = max + 1
	return nil
}

func (w *WAL) writeMeta() error {
	bytes, err := json.Marshal(w.meta)
	if err != nil {
		return err
	}
	if len(bytes) > metaPageSize {
		return fmt.Errorf("too big to write")
	}
	_, err = w.f.WriteAt(bytes, 0)
	fileutil.Fdatasync(w.f)
	return err
}

func (w *WAL) Set(key []byte, val []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.meta[string(key)] = val
	return w.writeMeta()
}

// Get returns the value for key, or an empty byte slice if key was not found.
func (w *WAL) Get(key []byte) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	v, ok := w.meta[string(key)]
	if ok {
		return v, nil
	}
	return nil, ErrKeyNotFound
}

func (w *WAL) SetUint64(key []byte, val uint64) error {
	var bs [8]byte
	binary.BigEndian.PutUint64(bs[:], val)
	return w.Set(key, bs[:])
}

// GetUint64 returns the uint64 value for key, or 0 if key was not found.
func (w *WAL) GetUint64(key []byte) (uint64, error) {
	v, err := w.Get(key)
	if err != nil {
		return 0, err
	}
	if len(v) != 8 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(v), nil
}
