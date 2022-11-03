// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/immutable"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-wal/types"
)

var (
	_ raft.LogStore    = &WAL{}
	_ raft.StableStore = &WAL{}

	ErrNotFound = types.ErrNotFound
	ErrCorrupt  = types.ErrCorrupt
	ErrSealed   = types.ErrSealed
	ErrClosed   = types.ErrClosed

	DefaultSegmentSize = 64 * 1024 * 1024
)

// WAL is a write-ahead log suitable for github.com/hashicorp/raft.
type WAL struct {
	closed uint32 // atomically accessed to keep it first in struct for alignment.

	dir         string
	codec       Codec
	sf          types.SegmentFiler
	metaDB      types.MetaStore
	log         hclog.Logger
	segmentSize int

	// metrics is allocated once on creation then it's elements are accessed
	// atomically after that.
	metrics []uint64

	// s is the current state of the WAL files. It is an immutable snapshot that
	// can be accessed without a lock when reading. We only support a single
	// writer so all methods that mutate either the WAL state or append to the
	// tail of the log must hold the writeMu until they complete all changes.
	s atomic.Value

	// writeMu must be held when modifying s or while appending to the tail.
	// Although we take care never to let readers block writer, we still only
	// allow a single writer to be updating the meta state at once. The mutex must
	// be held before s is loaded until all modifications to s or appends to the
	// tail are complete.
	writeMu sync.Mutex
}

type walOpt func(*WAL)

// Open attempts to open the WAL stored in dir. If there are no existing WAL
// files a new WAL will be initialized there. The dir must already exist and be
// readable and writable to the current process. If existing files are found,
// recovery is attempted. If recovery is not possible an error is returned,
// otherwise the returned *WAL is in a state ready for use.
func Open(dir string, opts ...walOpt) (*WAL, error) {
	w := &WAL{
		dir:     dir,
		metrics: make([]uint64, numMetrics),
	}
	// Apply options
	for _, opt := range opts {
		opt(w)
	}
	if err := w.applyDefaultsAndValidate(); err != nil {
		return nil, err
	}

	// Load or create metaDB
	persisted, err := w.metaDB.Load(w.dir)
	if err != nil {
		return nil, err
	}

	newState := state{
		segments:      &immutable.SortedMap[uint64, segmentState]{},
		nextSegmentID: persisted.NextSegmentID,
	}

	// Get the set of all persisted segments so we can prune it down to just the
	// unused ones as we go.
	toDelete, err := w.sf.List()
	if err != nil {
		return nil, err
	}

	// Build the state
	recoveredTail := false
	for i, si := range persisted.Segments {

		// Verify we can decode the entries.
		// TODO: support multiple decoders to allow rotating codec.
		if si.Codec != w.codec.ID() {
			return nil, fmt.Errorf("segment with BasedIndex=%d uses an unknown codec", si.BaseIndex)
		}

		// We want to keep this segment since it's still in the metaDB list!
		delete(toDelete, si.ID)

		if si.SealTime.IsZero() {
			// This is an unsealed segment. It _must_ be the last one. Safety check!
			if i < len(persisted.Segments)-1 {
				return nil, fmt.Errorf("unsealed segment is not at tail")
			}

			// Try to recover this segment
			sw, err := w.sf.RecoverTail(si)
			if err != nil {
				return nil, err
			}
			// Set the tail and "reader" for this segment
			ss := segmentState{
				SegmentInfo: si,
				r:           sw,
			}
			newState.tail = sw
			newState.segments = newState.segments.Set(si.BaseIndex, ss)
			recoveredTail = true

			// We're done with this loop, break here to avoid nesting all the rest of
			// the logic!
			break
		}

		// This is a sealed segment

		// Open segment reader
		sr, err := w.sf.Open(si)
		if err != nil {
			return nil, err
		}

		// Store the open reader to get logs from
		ss := segmentState{
			SegmentInfo: si,
			r:           sr,
		}
		newState.segments = newState.segments.Set(si.BaseIndex, ss)
	}

	if !recoveredTail {
		// There was no unsealed segment at the end. This can only really happen
		// when the log is empty with zero segments (either on creation or after a
		// truncation that removed all segments) since we otherwise never allow the
		// state to have a sealed tail segment. But this logic works regardless!

		// Create a new segment. We use baseIndex of 1 even though the first append
		// might be much higher - we'll allow that since we know we have no records
		// yet and so lastIndex will also be 0.
		si := w.newSegment(newState.nextSegmentID, 1)
		newState.nextSegmentID++
		ss := segmentState{
			SegmentInfo: si,
		}
		newState.segments = newState.segments.Set(si.BaseIndex, ss)

		// Persist the new meta to "commit" it even before we create the file so we
		// don't attempt to recreate files with duplicate IDs on a later failure.
		if err := w.metaDB.CommitState(newState.Persistent()); err != nil {
			return nil, err
		}

		// Create the new segment file
		w, err := w.sf.Create(si)
		if err != nil {
			return nil, err
		}
		newState.tail = w
		// Update the segment in memory so we have a reader for the new segment. We
		// don't need to commit again as this isn't changing the persisted metadata
		// about the segment.
		ss.r = w
		newState.segments = newState.segments.Set(si.BaseIndex, ss)
	}

	// Store the in-memory state (it was already persisted if we modified it
	// above) there are no readers yet since we are constructing a new WAL so we
	// don't need to jump through the mutateState hoops yet!
	w.s.Store(&newState)

	// Delete any unused segment files left over after a crash.
	w.deleteSegments(toDelete)

	return w, nil
}

// stateTxn represents a transaction body that mutates the state under the
// writeLock. s is already a shallow copy of the current state that may be
// mutated as needed. If a nil error is returned, s will be atomically set as
// the new state. If a non-nil finalizer func is returned it will be atomically
// attached to the old state after it's been replaced but before the write lock
// is released. The finalizer will be called exactly once when all current
// readers have released the old state.
type stateTxn func(s *state) (func(), error)

func (w *WAL) loadState() *state {
	return w.s.Load().(*state)
}

// mutateState executes a stateTxn. writeLock MUST be held while calling this.
func (w *WAL) mutateStateLocked(tx stateTxn) error {
	s := w.loadState()
	s.acquire()
	defer s.release()

	newS := s.clone()
	fn, err := tx(&newS)
	if err != nil {
		return err
	}

	// Commit updates to meta
	if err := w.metaDB.CommitState(newS.Persistent()); err != nil {
		return err
	}

	w.s.Store(&newS)
	s.finalizer.Store(fn)
	return nil
}

// acquireState should be used by all readers to fetch the current state. The
// returned release func must be called when no further accesses to state or the
// data within it will be performed to free old files that may have been
// truncated concurrently.
func (w *WAL) acquireState() (*state, func()) {
	s := w.loadState()
	return s, s.acquire()
}

// newSegment creates a types.SegmentInfo with the passed ID and baseIndex, filling in
// the segment parameters based on the current WAL configuration.
func (w *WAL) newSegment(ID, baseIndex uint64) types.SegmentInfo {
	return types.SegmentInfo{
		ID:        ID,
		BaseIndex: baseIndex,
		MinIndex:  baseIndex,
		SizeLimit: uint32(w.segmentSize),

		// TODO make these configurable
		Codec:      CodecBinaryV1,
		CreateTime: time.Now(),
	}
}

// FirstIndex returns the first index written. 0 for no entries.
func (w *WAL) FirstIndex() (uint64, error) {
	if err := w.checkClosed(); err != nil {
		return 0, err
	}
	s, release := w.acquireState()
	defer release()
	return s.firstIndex(), nil
}

// LastIndex returns the last index written. 0 for no entries.
func (w *WAL) LastIndex() (uint64, error) {
	if err := w.checkClosed(); err != nil {
		return 0, err
	}
	s, release := w.acquireState()
	defer release()
	return s.lastIndex(), nil
}

// GetLog gets a log entry at a given index.
func (w *WAL) GetLog(index uint64, log *raft.Log) error {
	if err := w.checkClosed(); err != nil {
		return err
	}
	s, release := w.acquireState()
	defer release()
	w.incr("log_entries_read", 1)

	raw, err := s.getLog(index)
	if err != nil {
		return err
	}
	w.incr("log_entry_bytes_read", uint64(len(raw.Bs)))
	defer raw.Close()

	// Decode the log
	return w.codec.Decode(raw.Bs, log)
}

// StoreLog stores a log entry.
func (w *WAL) StoreLog(log *raft.Log) error {
	return w.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries.
func (w *WAL) StoreLogs(logs []*raft.Log) error {
	if err := w.checkClosed(); err != nil {
		return err
	}
	if len(logs) < 1 {
		return nil
	}

	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	s, release := w.acquireState()
	defer release()

	// Verify monotonicity since we assume it
	lastIdx := s.lastIndex()

	// Special case, if the log is currently empty and this is the first append,
	// we allow any starting index. But in most cases we've already created a
	// segment with BaseIndex of 1 to start using. That means we need to update
	// the metadata for the segment to set MinIndex. This is the only time we will
	// force a metaDB sync on a StoreLogs call so it seems OK to ensure that
	// MetaDB correctly reflects the range of logs stored. The alternative would
	// be not to allocate the segment file at all until now but that would be even
	// more expensive!
	if lastIdx == 0 && logs[0].Index > 1 {
		txn := func(s *state) (func(), error) {
			seg1, ok := s.segments.Get(1)
			if !ok {
				// Can't happen!
				return nil, fmt.Errorf("invalid internal state! %w", ErrCorrupt)
			}
			// Note that we're mutating a copy of types.SegmentInfo since it's stored by value
			// not reference.
			seg1.MinIndex = logs[0].Index
			s.segments = s.segments.Set(seg1.BaseIndex, seg1)
			return nil, nil
		}

		if err := w.mutateStateLocked(txn); err != nil {
			return err
		}
		// Now we can append!
	}

	// Encode logs
	nBytes := uint64(0)
	encoded := make([]types.LogEntry, len(logs))
	for i, l := range logs {
		if lastIdx > 0 && l.Index != (lastIdx+1) {
			return fmt.Errorf("non-monotonic log entries: tried to append index %d after %d", logs[0].Index, lastIdx)
		}
		// Need a new buffer each time because Data is just a slice so if we re-use
		// buffer then all end up pointing to the same underlying data which
		// contains only the final log value!
		var buf bytes.Buffer
		if err := w.codec.Encode(l, &buf); err != nil {
			return err
		}
		encoded[i].Data = buf.Bytes()
		encoded[i].Index = l.Index
		lastIdx = l.Index
		nBytes += uint64(len(encoded[i].Data))
	}
	if err := s.tail.Append(encoded); err != nil {
		return err
	}
	w.incr("log_appends", 1)
	w.incr("log_entries_written", uint64(len(encoded)))
	w.incr("log_entry_bytes_written", nBytes)

	// Check if we need to roll logs
	sealed, indexStart, err := s.tail.Sealed()
	if err != nil {
		return err
	}
	if sealed {
		go func() {
			w.writeMu.Lock()
			defer w.writeMu.Unlock()
			if err := w.rotateSegmentLocked(indexStart); err != nil {
				// TODO work out how to handle async errors
			}
		}()
	}
	return nil
}

// DeleteRange deletes a range of log entries. The range is inclusive.
// Implements raft.LogStore. Note that we only support deleting ranges that are
// a suffix or prefix of the log.
func (w *WAL) DeleteRange(min uint64, max uint64) error {
	if err := w.checkClosed(); err != nil {
		return err
	}
	if min > max {
		// Empty inclusive range.
		return nil
	}

	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	s, release := w.acquireState()
	defer release()

	// Work out what type of truncation this is.
	first, last := s.firstIndex(), s.lastIndex()
	switch {
	// |min----max|
	//               |first====last|
	// or
	//                |min----max|
	// |first====last|
	case max < first || min > last:
		// None of the range exists at all so a no-op
		return nil

	// |min----max|
	//      |first====last|
	// or
	// |min--------------max|
	//      |first====last|
	// or
	//   |min--max|
	//   |first====last|
	case min <= first: // max >= first implied by the first case not matching
		// Note we allow head truncations where max > last which effectively removes
		// the entire log.
		return w.truncateHeadLocked(max + 1)

	//    |min----max|
	// |first====last|
	// or
	//  |min--------------max|
	// |first====last|
	case max >= last: // min <= last implied by first case not matching
		return w.truncateTailLocked(min - 1)

	//    |min----max|
	// |first========last|
	default:
		// Everything else is a neither a suffix nor prefix so unsupported.
		return fmt.Errorf("only suffix or prefix ranges may be deleted from log")
	}
}

// Set implements raft.StableStore
func (w *WAL) Set(key []byte, val []byte) error {
	if err := w.checkClosed(); err != nil {
		return err
	}
	w.incr("stable_sets", 1)
	return w.metaDB.SetStable(key, val)
}

// Get implements raft.StableStore
func (w *WAL) Get(key []byte) ([]byte, error) {
	if err := w.checkClosed(); err != nil {
		return nil, err
	}
	w.incr("stable_gets", 1)
	return w.metaDB.GetStable(key)
}

// SetUint64 implements raft.StableStore. We assume the same key space as Set
// and Get so the caller is responsible for ensuring they don't call both Set
// and SetUint64 for the same key.
func (w *WAL) SetUint64(key []byte, val uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], val)
	return w.Set(key, buf[:])
}

// GetUint64 implements raft.StableStore. We assume the same key space as Set
// and Get. We assume that the key was previously set with `SetUint64` and
// returns an undefined value (possibly with nil error) if not.
func (w *WAL) GetUint64(key []byte) (uint64, error) {
	raw, err := w.Get(key)
	if err != nil {
		return 0, err
	}
	if len(raw) == 0 {
		// Not set, return zero per interface contract
		return 0, nil
	}
	// At least a tiny bit of checking is possible
	if len(raw) != 8 {
		return 0, fmt.Errorf("GetUint64 called on a non-uint64 key")
	}
	return binary.LittleEndian.Uint64(raw), nil
}

func (w *WAL) rotateSegmentLocked(indexStart uint64) error {
	txn := func(newState *state) (func(), error) {
		// Mark current tail as sealed in segments
		tail := newState.getTailInfo()
		if tail == nil {
			// Can't happen
			return nil, fmt.Errorf("no tail found during rotate")
		}

		// Note that tail is a copy since it's a value type. Even though this is a
		// pointer here it's pointing to a copy on the heap that was made in
		// getTailInfo above, so we can mutate it safely and update the immutable
		// state with our version.
		tail.SealTime = time.Now()
		tail.MaxIndex = newState.tail.LastIndex()
		tail.IndexStart = indexStart
		w.setGauge("last_segment_age_seconds", uint64(tail.SealTime.Sub(tail.CreateTime).Seconds()))

		// Update the old tail with the seal time etc.
		newState.segments = newState.segments.Set(tail.BaseIndex, *tail)

		return nil, w.createNextSegment(newState)
	}
	w.incr("segment_rotations", 1)
	return w.mutateStateLocked(txn)
}

// createNextSegment is passes a mutable copy of the new state ready to have a
// new segment appended. newState must be a copy, taken under write lock which
// is still held by the caller and its segments map must contain all non-tail
// segments that should be in the log, all must be sealed at this point. The new
// segment's baseIndex will be the current last-segment's MaxIndex (or 0 if
// non).
func (w *WAL) createNextSegment(newState *state) error {
	// Find existing sealed tail
	tail := newState.getTailInfo()

	// If there is no tail, next baseIndex is 1
	nextBaseIndex := uint64(1)
	if tail != nil {
		nextBaseIndex = tail.MaxIndex + 1
	}

	// Create a new segment
	newTail := w.newSegment(newState.nextSegmentID, nextBaseIndex)
	newState.nextSegmentID++
	ss := segmentState{
		SegmentInfo: newTail,
	}
	newState.segments = newState.segments.Set(newTail.BaseIndex, ss)
	newState.nextSegmentID++

	// Create the file in parallel since we will recover based on the committed
	// state in meta db either re-creating or deleting unnecessary files as needed
	// after a crash. This parallelizes the fsyncs to different files to minimize
	// latency blocking next write.
	type createReturn struct {
		w   types.SegmentWriter
		err error
	}
	ch := make(chan createReturn)
	go func() {
		// Now create the new segment for writing.
		sw, err := w.sf.Create(newTail)
		ch <- createReturn{sw, err}
	}()

	// Commit the new meta to disk before we create new files so that we never
	// leave a file with ID >= the persisted nextSegmentID lying around.
	if err := w.metaDB.CommitState(newState.Persistent()); err != nil {
		return err
	}

	// Now create the new segment for writing.
	cr := <-ch
	if cr.err != nil {
		return cr.err
	}
	newState.tail = cr.w

	// Also cache the reader/log getter which is also the writer. We don't bother
	// reopening read only since we assume we have exclusive access anyway and
	// only use this read-only interface once the segment is sealed.
	ss.r = newState.tail

	// We need to re-insert it since newTail is a copy not a reference
	newState.segments = newState.segments.Set(newTail.BaseIndex, ss)

	return nil
}

func (w *WAL) truncateHeadLocked(newMin uint64) error {
	txn := stateTxn(func(newState *state) (func(), error) {
		// Iterate the segments to find any that are entirely deleted.
		toDelete := make(map[uint64]uint64)
		toClose := make([]io.Closer, 0, 1)
		it := newState.segments.Iterator()
		var head *segmentState
		nTruncated := uint64(0)
		for !it.Done() {
			_, seg, _ := it.Next()

			maxIdx := seg.MaxIndex
			// If the segment is the tail (unsealed) or a sealed segment that contains
			// this new min then we've found the new head.
			if seg.SealTime.IsZero() {
				maxIdx = newState.lastIndex()
				// This is the tail, check if it actually has any content to keep
				if maxIdx >= newMin {
					head = &seg
					break
				}
			} else if seg.MaxIndex >= newMin {
				head = &seg
				break
			}

			toDelete[seg.ID] = seg.BaseIndex
			toClose = append(toClose, seg.r)
			newState.segments = newState.segments.Delete(seg.BaseIndex)
			nTruncated += (maxIdx - seg.MinIndex + 1) // +1 becuase MaxIndex is inclusive
		}

		// There may not be any segments (left) but if there are, update the new
		// head's MinIndex.
		if head != nil {
			// new
			nTruncated += (newMin - head.MinIndex)
			head.MinIndex = newMin
			newState.segments = newState.segments.Set(head.BaseIndex, *head)
		} else {
			// If there is no head any more, then there is no tail either! We should
			// create a new blank one ready for use when we next append like we do
			// during initialization.
			if err := w.createNextSegment(newState); err != nil {
				return nil, err
			}
		}
		w.incr("head_truncations", nTruncated)

		// Return a finalizer that will be called when all readers are done with the
		// segments in the current state to close and delete old segments.
		fin := func() {
			w.closeSegments(toClose)
			w.deleteSegments(toDelete)
		}
		return fin, nil
	})

	return w.mutateStateLocked(txn)
}

func (w *WAL) truncateTailLocked(newMax uint64) error {
	txn := stateTxn(func(newState *state) (func(), error) {
		// Reverse iterate the segments to find any that are entirely deleted.
		toDelete := make(map[uint64]uint64)
		toClose := make([]io.Closer, 0, 1)
		it := newState.segments.Iterator()
		it.Last()

		nTruncated := uint64(0)
		for !it.Done() {
			_, seg, _ := it.Prev()

			if seg.BaseIndex <= newMax {
				// We're done
				break
			}

			maxIdx := seg.MaxIndex
			if seg.SealTime.IsZero() {
				maxIdx = newState.lastIndex()
			}

			toDelete[seg.ID] = seg.BaseIndex
			toClose = append(toClose, seg.r)
			newState.segments = newState.segments.Delete(seg.BaseIndex)
			nTruncated += (maxIdx - seg.MinIndex + 1) // +1 becuase MaxIndex is inclusive
		}

		tail := newState.getTailInfo()
		if tail != nil {
			maxIdx := tail.MaxIndex

			// Check that the tail is sealed (it won't be if we didn't need to remove
			// the actual partial tail above).
			if tail.SealTime.IsZero() {
				tail.SealTime = time.Now()
				maxIdx = newState.lastIndex()
			}
			// Update the MaxIndex

			nTruncated += (maxIdx - newMax)
			tail.MaxIndex = newMax

			// And update the tail in the new state
			newState.segments = newState.segments.Set(tail.BaseIndex, *tail)
		}

		// Create the new tail segment
		if err := w.createNextSegment(newState); err != nil {
			return nil, err
		}
		w.incr("tail_truncations", nTruncated)

		// Return a finalizer that will be called when all readers are done with the
		// segments in the current state to close and delete old segments.
		fin := func() {
			w.closeSegments(toClose)
			w.deleteSegments(toDelete)
		}
		return fin, nil
	})

	return w.mutateStateLocked(txn)
}

func (w *WAL) deleteSegments(toDelete map[uint64]uint64) {
	for ID, baseIndex := range toDelete {
		if err := w.sf.Delete(baseIndex, ID); err != nil {
			// This is not fatal. We can continue just old files might need manual
			// cleanup somehow.
			w.log.Error("failed to delete old segment", "baseIndex", baseIndex, "id", ID, "err", err)
		}
	}
}

func (w *WAL) closeSegments(toClose []io.Closer) {
	for _, c := range toClose {
		if c != nil {
			if err := c.Close(); err != nil {
				// Shouldn't happen!
				w.log.Error("error closing old segment file", "err", err)
			}
		}
	}
}

func (w *WAL) checkClosed() error {
	closed := atomic.LoadUint32(&w.closed)
	if closed != 0 {
		return ErrClosed
	}
	return nil
}

// Close closes all open files related to the WAL. The WAL is in an invalid
// state and should not be used again after this is called. It is safe (though a
// no-op) to call it multiple times and concurrent reads and writes will either
// complete safely or get ErrClosed returned depending on sequencing. Generally
// reads and writes should be stopped before calling this to avoid propagating
// errors to users during shutdown but it's safe from a data-race perspective.
func (w *WAL) Close() error {
	// Only close once
	old := atomic.SwapUint32(&w.closed, 1)
	if old == 0 {
		// Wait for writes
		w.writeMu.Lock()
		defer w.writeMu.Unlock()

		// Replace state with nil state
		s := w.loadState()
		s.acquire()
		defer s.release()

		w.s.Store(&state{})

		// Old state might be still in use by readers, attach closers to all open
		// segment files.
		toClose := make([]io.Closer, 0, s.segments.Len())
		it := s.segments.Iterator()
		for !it.Done() {
			_, seg, _ := it.Next()
			if seg.r != nil {
				toClose = append(toClose, seg.r)
			}
		}
		// Store finalizer to run once all readers are done. There can't be an
		// existing finalizer since this was the active state read under a write
		// lock and finalizers are only set on states that have been replaced under
		// that same lock.
		s.finalizer.Store(func() {
			w.closeSegments(toClose)
		})
	}
	return nil
}
