// Copyright (c) 2026 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package zapcore

import (
	"errors"
	"io"
	"sync"

	"go.uber.org/multierr"
)

// ErrHistoryClosed is returned by [HistoryCore.Export] when called after the
// history has been closed.
var ErrHistoryClosed = errors.New("zapcore: history core is closed")

// historyEntry retains a complete log entry together with the fields it was
// written with. Stored field slices are freshly copied on insertion and never
// mutated afterwards, so concurrent exports can read them safely.
type historyEntry struct {
	Entry
	fields []Field
}

// historyState is the mutable ring-buffer state shared by a HistoryCore and
// every core derived from it through With, mirroring how zaptest/observer
// shares its log collection.
type historyState struct {
	mu sync.RWMutex

	ring []historyEntry // len == capacity; unused slots hold the zero value
	head int            // index of the oldest retained entry when size > 0
	size int            // number of retained entries, 0 <= size <= cap(ring)

	closed bool
}

// HistoryCore is a Core that retains the most recent bounded number of
// complete log entries in an in-memory ring buffer, in addition to whatever
// other Cores do with the same entries (combine it with [NewTee] to keep the
// existing log outputs).
//
// The retained window can later be exported to an arbitrary output target
// with [HistoryCore.Export], selecting entries by level. This is useful when
// a failure (e.g. an Error or Fatal entry) needs the request context that led
// up to it: the window keeps the preceding entries even when they were not
// written to a persistent destination at the time.
//
// Export is a non-destructive snapshot: it never clears or disables the
// window, so entries written during and after an export are still captured.
//
// HistoryCore is safe for concurrent use. Writes, exports, Sync and Close may
// run simultaneously without data races; every retained or exported entry is
// complete and entries are delivered in write order.
type HistoryCore struct {
	LevelEnabler

	state   *historyState
	context []Field // fields added with With; copied per derived core
}

var (
	_ Core           = (*HistoryCore)(nil)
	_ leveledEnabler = (*HistoryCore)(nil)
)

// NewHistoryCore creates a HistoryCore that retains at most capacity of the
// most recent entries enabled by enab. A capacity of zero or less creates a
// core that records nothing; Check/Write still behave correctly and it can be
// composed freely.
//
// Combine it with the regular output Cores to retain history in addition to
// normal logging:
//
//	history := zapcore.NewHistoryCore(zapcore.DebugLevel, 256)
//	core := zapcore.NewTee(regularCore, history)
func NewHistoryCore(enab LevelEnabler, capacity int) *HistoryCore {
	if capacity < 0 {
		capacity = 0
	}
	return &HistoryCore{
		LevelEnabler: enab,
		state: &historyState{
			ring: make([]historyEntry, capacity),
		},
	}
}

// Level reports the minimum enabled level, so the history integrates with
// [LevelOf] and level-aware wrappers (samplers, level filters).
func (h *HistoryCore) Level() Level {
	return LevelOf(h.LevelEnabler)
}

// With returns a derived HistoryCore that shares the same ring buffer but
// carries additional context fields. The context slice is copied so that the
// caller cannot mutate retained entries through it later.
func (h *HistoryCore) With(fields []Field) Core {
	if len(fields) == 0 {
		return h
	}
	context := make([]Field, 0, len(h.context)+len(fields))
	context = append(context, h.context...)
	context = append(context, fields...)
	return &HistoryCore{
		LevelEnabler: h.LevelEnabler,
		state:        h.state,
		context:      context,
	}
}

// Check registers the history core for the entry when its level is enabled.
func (h *HistoryCore) Check(ent Entry, ce *CheckedEntry) *CheckedEntry {
	if h.Enabled(ent.Level) {
		return ce.AddCore(ent, h)
	}
	return ce
}

// Write retains a complete, independent copy of the entry and of both its
// context fields and its log-site fields. Once an entry is stored it is never
// mutated, which is what makes concurrent exports safe.
func (h *HistoryCore) Write(ent Entry, fields []Field) error {
	st := h.state
	st.mu.Lock()
	if st.closed || len(st.ring) == 0 {
		st.mu.Unlock()
		return nil
	}

	merged := make([]Field, 0, len(h.context)+len(fields))
	merged = append(merged, h.context...)
	merged = append(merged, fields...)

	if st.size < len(st.ring) {
		st.ring[(st.head+st.size)%len(st.ring)] = historyEntry{
			Entry:  ent,
			fields: merged,
		}
		st.size++
	} else {
		// Window is full: overwrite the oldest entry and advance the head.
		st.ring[st.head] = historyEntry{
			Entry:  ent,
			fields: merged,
		}
		st.head = (st.head + 1) % len(st.ring)
	}
	st.mu.Unlock()
	return nil
}

// Sync reports nil: the history window holds entries in memory and owns no
// buffered output. Export syncs the supplied target itself.
func (h *HistoryCore) Sync() error {
	return nil
}

// Close shuts the history window. Entries retained so far stay available for
// inspection through Len, but further writes are discarded and Export returns
// [ErrHistoryClosed]. Closing an already-closed core is a no-op.
//
// Close does not close any WriteSyncer previously passed to Export; callers
// own the lifecycle of their export targets.
func (h *HistoryCore) Close() error {
	h.state.mu.Lock()
	h.state.closed = true
	h.state.mu.Unlock()
	return nil
}

// Len returns the number of entries currently retained in the window.
func (h *HistoryCore) Len() int {
	h.state.mu.RLock()
	n := h.state.size
	h.state.mu.RUnlock()
	return n
}

// Export writes a non-destructive snapshot of the retained entries selected by
// levels to ws using enc, in oldest-to-newest write order, and then syncs ws
// so the dump is durable. It returns the number of exported entries.
//
// Every selected entry is encoded on its own and written to completion (a
// writer that accepts only part of a line per Write call is handled), so
// entries never get truncated. Because the export works on a snapshot taken
// under the window lock, it does not block subsequent logging once taken and
// can never cause entries written during or after the export to be lost.
//
// When several goroutines export to the same WriteSyncer concurrently, pass a
// [Lock]-wrapped (or otherwise concurrency-safe) syncer to keep whole entries
// from interleaving.
//
// A nil levels enabler matches nothing; enc and ws must be non-nil.
func (h *HistoryCore) Export(enc Encoder, ws WriteSyncer, levels LevelEnabler) (int, error) {
	if enc == nil {
		return 0, errors.New("zapcore: Export requires a non-nil Encoder")
	}
	if ws == nil {
		return 0, errors.New("zapcore: Export requires a non-nil WriteSyncer")
	}

	snapshot, err := h.snapshot(levels)
	if err != nil {
		return 0, err
	}

	// Clone the encoder: callers may keep using enc, and each export must be
	// independent of concurrent exports.
	enc = enc.Clone()

	exported := 0
	for i := range snapshot {
		buf, encErr := enc.EncodeEntry(snapshot[i].Entry, snapshot[i].fields)
		if encErr != nil {
			err = multierr.Append(err, encErr)
			continue
		}
		if writeErr := writeEntry(ws, buf.Bytes()); writeErr != nil {
			buf.Free()
			err = multierr.Append(err, writeErr)
			break
		}
		buf.Free()
		exported++
	}
	// Flush whatever made it through even if a write failed.
	err = multierr.Append(err, ws.Sync())
	return exported, err
}

// snapshot copies the entries selected by levels in oldest-to-newest order.
// Only the slice headers are copied: the underlying field arrays are created
// per entry at write time and never mutated afterwards.
func (h *HistoryCore) snapshot(levels LevelEnabler) ([]historyEntry, error) {
	h.state.mu.RLock()
	defer h.state.mu.RUnlock()

	if h.state.closed {
		return nil, ErrHistoryClosed
	}
	if levels == nil || h.state.size == 0 {
		return nil, nil
	}

	capacity := len(h.state.ring)
	snapshot := make([]historyEntry, 0, h.state.size)
	for i := 0; i < h.state.size; i++ {
		idx := (h.state.head + i) % capacity
		ent := h.state.ring[idx]
		// Copy the slice header into our own backing array; the fields
		// themselves are treated as immutable.
		if levels.Enabled(ent.Level) {
			snapshot = append(snapshot, ent)
		}
	}
	return snapshot, nil
}

// writeEntry writes the whole payload to ws, looping over short writes. The
// encoder always terminates an entry with a line ending, so a completed write
// is a complete entry that cannot interleave with itself.
func writeEntry(ws WriteSyncer, p []byte) error {
	for len(p) > 0 {
		n, err := ws.Write(p)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		if n > len(p) {
			n = len(p)
		}
		p = p[n:]
	}
	return nil
}
