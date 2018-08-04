package raftwal

import (
	"encoding/binary"
	"os"
	"sync"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/hashicorp/raft"
)

const (
	headerSize = 4096
)

type WAL struct {
	path       string
	f          *os.File
	index      []uint32
	minIndex   uint64
	nextOffset int64
	mu         sync.Mutex
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
	return &WAL{
		path:  path,
		f:     f,
		index: make([]uint32, 0, 128*1024),
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

	buf := make([]byte, length)
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

		// Encode length prefix
		binary.BigEndian.PutUint32(buf[offset:offset+4],
			uint32(8+8+1+len(l.Data)))
		// Encode index
		binary.BigEndian.PutUint64(buf[offset+4:offset+12], l.Index)
		// Encode length prefix
		binary.BigEndian.PutUint64(buf[offset+12:offset+20], l.Term)
		buf[offset+20] = uint8(l.Type)
		copy(buf[offset+21:], l.Data)
		offset += 4 + 8 + 8 + 1 + int64(len(l.Data))
	}

	_, err := w.f.WriteAt(buf, w.nextOffset)
	if err != nil {
		return err
	}
	//err = fileutil.Fdatasync(w.f)
	err = w.f.Sync()
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
