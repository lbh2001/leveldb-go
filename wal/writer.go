package wal

import (
	"encoding/binary"
	"io"
)

const (
	blockSize  = 32 * 1024
	headerSize = 7
)

const (
	ChunkTypeFull   byte = 1
	ChunkTypeFirst  byte = 2
	ChunkTypeMiddle byte = 3
	ChunkTypeLast   byte = 4
)

type LogWriter struct {
	writer    io.Writer
	seqNumber uint64
	buf       [blockSize]byte
	low, high int
	written   int
	isFirst   bool
	err       error
	pending   bool
}

func NewLogWriter(w io.Writer) *LogWriter {
	return &LogWriter{
		writer:  w,
		buf:     [blockSize]byte{},
		low:     0,
		high:    0,
		written: 0,
		isFirst: true,
	}
}

func (lw *LogWriter) setHeader(isLast bool) {
	if isLast {
		if lw.isFirst {
			lw.buf[lw.low+6] = ChunkTypeFull
		} else {
			lw.buf[lw.low+6] = ChunkTypeLast
		}
	} else {
		if lw.isFirst {
			lw.buf[lw.low+6] = ChunkTypeFirst
		} else {
			lw.buf[lw.low+6] = ChunkTypeMiddle
		}
	}
	binary.BigEndian.PutUint16(lw.buf[lw.low+4:lw.low+6], uint16(lw.high-lw.low-headerSize))
	binary.BigEndian.PutUint32(lw.buf[lw.low:lw.low+4], NewCRC(lw.buf[lw.low+6:lw.high]).Value())
}

func (lw *LogWriter) write() {
	_, lw.err = lw.writer.Write(lw.buf[lw.written:])
	lw.low, lw.high, lw.written = 0, headerSize, 0
}

func (lw *LogWriter) writePending() {
	if lw.err != nil {
		return
	}
	if lw.pending {
		lw.setHeader(true)
		lw.pending = false
	}
	_, lw.err = lw.writer.Write(lw.buf[lw.written:lw.high])
	lw.written = lw.high
}

func (lw *LogWriter) Next() (io.Writer, error) {
	lw.seqNumber++
	if lw.err != nil {
		return nil, lw.err
	}
	if lw.pending {
		lw.setHeader(true)
	}
	lw.low, lw.high = lw.high, lw.high+headerSize
	if lw.high > blockSize {
		for i := lw.low; i < blockSize; i++ {
			lw.buf[i] = 0
		}
		lw.write()
		if lw.err != nil {
			return nil, lw.err
		}
	}
	lw.isFirst = true
	lw.pending = true
	return &singleWriter{lw: lw, seqNumber: lw.seqNumber}, nil
}

type singleWriter struct {
	lw        *LogWriter
	seqNumber uint64
}

func (sw *singleWriter) Write(p []byte) (int, error) {
	return 0, nil
}

// TODO
