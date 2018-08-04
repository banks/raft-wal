package raftwal

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/hashicorp/raft"
)

func testWAL(t testing.TB) *WAL {
	d, err := ioutil.TempDir("", "wal")
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Successfully creates and returns a w
	w, err := New(d)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	return w
}

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func TestWAL_FirstIndex(t *testing.T) {
	w := testWAL(t)
	defer w.Close()
	defer os.RemoveAll(w.path)

	// Should get 0 index on empty log
	idx, err := w.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := w.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the first Raft index
	idx, err = w.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestWAL_LastIndex(t *testing.T) {
	w := testWAL(t)
	defer w.Close()
	defer os.RemoveAll(w.path)

	// Should get 0 index on empty log
	idx, err := w.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := w.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the last Raft index
	idx, err = w.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestWAL_GetLog(t *testing.T) {
	w := testWAL(t)
	defer w.Close()
	//defer os.RemoveAll(w.path)

	t.Logf("wal file: %s", w.path)

	log := new(raft.Log)

	// Should return an error on non-existent log
	if err := w.GetLog(1, log); err != raft.ErrLogNotFound {
		t.Fatalf("expected raft log not found error, got: %v", err)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := w.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Should return the proper log
	if err := w.GetLog(2, log); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(log, logs[1]) {
		t.Fatalf("bad: %#v", log)
	}
}

func TestWAL_SetLog(t *testing.T) {
	w := testWAL(t)
	defer w.Close()
	defer os.RemoveAll(w.path)

	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	// Attempt to w the log
	if err := w.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Retrieve the log again
	result := new(raft.Log)
	if err := w.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Fatalf("bad: %v", result)
	}
}

func TestWAL_SetLogs(t *testing.T) {
	w := testWAL(t)
	defer w.Close()
	defer os.RemoveAll(w.path)

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}

	// Attempt to w the logs
	if err := w.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure we stored them all
	result1, result2 := new(raft.Log), new(raft.Log)
	if err := w.GetLog(1, result1); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[0], result1) {
		t.Fatalf("bad: %#v", result1)
	}
	if err := w.GetLog(2, result2); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[1], result2) {
		t.Fatalf("bad: %#v", result2)
	}
}

func TestWAL_DeleteRange(t *testing.T) {
	w := testWAL(t)
	defer w.Close()
	defer os.RemoveAll(w.path)

	// Create a set of logs
	log1 := testRaftLog(1, "log1")
	log2 := testRaftLog(2, "log2")
	log3 := testRaftLog(3, "log3")
	logs := []*raft.Log{log1, log2, log3}

	// Attempt to w the logs
	if err := w.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Attempt to delete a range of logs
	if err := w.DeleteRange(1, 2); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the logs were deleted
	if err := w.GetLog(1, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log1")
	}
	if err := w.GetLog(2, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log2")
	}
}

/*func TestWAL_Set_Get(t *testing.T) {
	w := testWAL(t)
	defer w.Close()
	defer os.RemoveAll(w.path)

	// Returns error on non-existent key
	if _, err := w.Get([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("hello"), []byte("world")

	// Try to set a k/v pair
	if err := w.Set(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Try to read it back
	val, err := w.Get(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !bytes.Equal(val, v) {
		t.Fatalf("bad: %v", val)
	}
}

func TestWAL_SetUint64_GetUint64(t *testing.T) {
	w := testWAL(t)
	defer w.Close()
	defer os.RemoveAll(w.path)

	// Returns error on non-existent key
	if _, err := w.GetUint64([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("abc"), uint64(123)

	// Attempt to set the k/v pair
	if err := w.SetUint64(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Read back the value
	val, err := w.GetUint64(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if val != v {
		t.Fatalf("bad: %v", val)
	}
}
*/
