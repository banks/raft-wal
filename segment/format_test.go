// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package segment

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/hashicorp/go-wal"
	"github.com/stretchr/testify/require"
)

func TestFileHeaderCodec(t *testing.T) {
	cases := []struct {
		name         string
		info         wal.SegmentInfo
		bufSize      int
		corrupt      func([]byte) []byte
		wantWriteErr string
		wantReadErr  string
	}{
		{
			name: "basic encoding/decoding",
			info: wal.SegmentInfo{
				BaseIndex: 1234,
				ID:        4321,
				BlockSize: DefaultBlockSize,
				NumBlocks: DefaultNumBlocks,
			},
		},
		{
			name: "short buf writing",
			info: wal.SegmentInfo{
				BaseIndex: 1234,
				ID:        4321,
				BlockSize: DefaultBlockSize,
				NumBlocks: DefaultNumBlocks,
			},
			bufSize:      10,
			wantWriteErr: "short buffer",
		},
		{
			name: "short buf reading",
			info: wal.SegmentInfo{
				BaseIndex: 1234,
				ID:        4321,
				BlockSize: DefaultBlockSize,
				NumBlocks: DefaultNumBlocks,
			},
			corrupt: func(buf []byte) []byte {
				return buf[0:5]
			},
			wantReadErr: "short buffer",
		},
		{
			name: "bad magic reading",
			info: wal.SegmentInfo{
				BaseIndex: 1234,
				ID:        4321,
				BlockSize: DefaultBlockSize,
				NumBlocks: DefaultNumBlocks,
			},
			corrupt: func(buf []byte) []byte {
				buf[0] = 0xff
				return buf
			},
			wantReadErr: "corrupt",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			len := fileHeaderLen
			if tc.bufSize > 0 {
				len = tc.bufSize
			}
			buf := make([]byte, len)

			err := writeFileHeader(buf, tc.info)

			if tc.wantWriteErr != "" {
				require.ErrorContains(t, err, tc.wantWriteErr)
				return
			}
			require.NoError(t, err)

			if tc.corrupt != nil {
				buf = tc.corrupt(buf)
			}

			got, err := readFileHeader(buf)
			if tc.wantReadErr != "" {
				require.ErrorContains(t, err, tc.wantReadErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, got)

			require.Equal(t, tc.info, *got)
		})
	}
}

func TestFileHeaderCodecFuzz(t *testing.T) {
	fuzz := fuzz.New()

	var info wal.SegmentInfo
	var buf [fileHeaderLen]byte
	for i := 0; i < 1000; i++ {
		fuzz.Fuzz(&info)
		err := writeFileHeader(buf[:], info)
		require.NoError(t, err)

		t.Logf("% x", buf[:])

		err = validateFileHeader(buf[:], info)
		require.NoError(t, err)
	}
}

func TestFrameCodecFuzz(t *testing.T) {
	fuzz := fuzz.New()

	var len uint16
	var buf [math.MaxUint16 + frameHeaderLen]byte
	var val = []byte(strings.Repeat("A Value!", math.MaxUint16/8))
	var fh frameHeader
	for i := 0; i < 1000; i++ {
		fuzz.Fuzz(&len)

		fh.typ = FrameFirst
		fh.len = uint32(len)
		fh.entryLenOrCRC = uint32(2 * len)

		expectLen := encodedFrameSize(int(len))

		// Note length of val is not the same as fh.len which is what should be
		// used.
		err := writeFrame(buf[:expectLen], fh, val)
		require.NoError(t, err)

		// We mostly care about the start and end...
		if expectLen > 64 {
			t.Logf("% x [...] % x (%d)", buf[0:16], buf[expectLen-16:expectLen], expectLen)
		} else {
			t.Logf("% x", buf[:expectLen])
		}

		// Verify the last padLen bytes are zero
		for i := padLen(int(len)); i > 0; i-- {
			require.Equal(t, byte(0), buf[expectLen-i],
				"expected last %d bytes to be padding. Byte %d of %d isn't zero.",
				padLen(int(len)), expectLen-i, expectLen)
		}

		got, err := readFrameHeader(buf[:])
		require.NoError(t, err)
		require.Equal(t, fh, got)
	}
}

func TestPadLen(t *testing.T) {
	fuzz := fuzz.New()
	var len uint32

	for i := 0; i < 1000; i++ {
		fuzz.Fuzz(&len)

		got := padLen(int(len))

		t.Log("len", len)

		// Test basic properties of padLen
		require.Less(t, got, frameHeaderLen, "padding must be less than the whole header len")
		require.GreaterOrEqual(t, got, 0, "padding must be positive")
		require.Equal(t, 0, (got+int(len))%frameHeaderLen, "padding plus length must be a multiple of header len")
	}
}

func TestWriteIndexFrame(t *testing.T) {
	// TestFrameCodecFuzz covers most of the bases for the actual header encoding
	// etc. This just needs to test the index encoding.
	var index [1024]uint32

	for i := range index {
		// Write offsets as if each record is exactly 64 bytes
		index[i] = uint32(i * 64)
	}

	buf := make([]byte, indexFrameSize(len(index)))

	err := writeIndexFrame(buf, index[:])
	require.NoError(t, err)

	//t.Log(index, buf)

	// Validate that the encoded index after the header is what we expect
	offset := frameHeaderLen
	for i := range index {
		got := binary.LittleEndian.Uint32(buf[offset:])
		require.Equal(t, uint32(i*64), got, "unexpected index value at offset %d", i)
		offset += 4
	}
}

func TestBlockTrailerCodecFuzz(t *testing.T) {
	fuzz := fuzz.New()

	var buf [1024]byte
	var bt blockTrailer
	for i := 0; i < 1000; i++ {
		fuzz.Fuzz(&bt)

		// Note we write it at the _end_ of the buffer
		err := writeBlockTrailer(buf[1024-blockTrailerLen:], bt)
		require.NoError(t, err)

		// We mostly care about the start end...
		t.Logf("[...] % x", buf[1024-blockTrailerLen:])

		got, err := readBlockTrailer(buf[:])
		require.NoError(t, err)
		require.Equal(t, bt, got)
	}
}

func TestNextBlockStart(t *testing.T) {
	blockSize := uint32(1024)

	require.Equal(t, 1024, int(nextBlockStart(blockSize, 0)))
	require.Equal(t, 1024, int(nextBlockStart(blockSize, 1)))
	require.Equal(t, 1024, int(nextBlockStart(blockSize, 1022)))
	require.Equal(t, 2048, int(nextBlockStart(blockSize, 1024)))
}