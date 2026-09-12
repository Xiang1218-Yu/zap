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

package zapcore_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	//revive:disable:dot-imports
	. "go.uber.org/zap/zapcore"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lockedBuffer is a concurrency-safe in-memory WriteSyncer. Each entry is
// delivered through a single Write call while the lock is held, so complete
// entries never interleave with concurrent exports.
type lockedBuffer struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	syncs atomic.Int32
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) Sync() error {
	b.syncs.Add(1)
	return nil
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *lockedBuffer) lines(t *testing.T) []string {
	t.Helper()
	out := strings.Split(strings.TrimRight(b.String(), "\n"), "\n")
	if len(out) == 1 && out[0] == "" {
		return nil
	}
	return out
}

// chunkWriter accepts at most chunk bytes per Write call, forcing short
// writes, but ultimately retains every byte it is given.
type chunkWriter struct {
	mu    sync.Mutex
	chunk int
	buf   bytes.Buffer
}

func (w *chunkWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(p)
	if n > w.chunk {
		n = w.chunk
	}
	return w.buf.Write(p[:n])
}

func (w *chunkWriter) Sync() error { return nil }

func (w *chunkWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func strField(key, val string) Field {
	return Field{Key: key, Type: StringType, String: val}
}

func intField(key string, val int) Field {
	return Field{Key: key, Type: Int64Type, Integer: int64(val)}
}

func historyEncoder() Encoder {
	cfg := testEncoderConfig()
	cfg.TimeKey = ""
	return NewJSONEncoder(cfg)
}

func historyWrite(core Core, lvl Level, msg string, fields ...Field) {
	if ce := core.Check(Entry{Level: lvl, Message: msg}, nil); ce != nil {
		ce.Write(fields...)
	}
}

func TestHistoryCoreRetainsMostRecentWindow(t *testing.T) {
	h := NewHistoryCore(DebugLevel, 3)

	for i := 0; i < 5; i++ {
		historyWrite(h, InfoLevel, fmt.Sprintf("msg-%d", i))
	}
	require.Equal(t, 3, h.Len(), "window must be bounded by its capacity")

	sink := &lockedBuffer{}
	n, err := h.Export(historyEncoder(), sink, DebugLevel)
	require.NoError(t, err)
	require.Equal(t, 3, n)

	lines := sink.lines(t)
	require.Len(t, lines, 3)
	for i, line := range lines {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &m), "exported entry must be complete JSON: %q", line)
		assert.Equal(t, fmt.Sprintf("msg-%d", i+2), m["msg"], "entries must be exported oldest-to-newest")
	}
}

func TestHistoryCoreRetainsCompleteEntriesAndFields(t *testing.T) {
	h := NewHistoryCore(DebugLevel, 4)
	derived := h.With([]Field{strField("ctx", "v1")}).With([]Field{intField("n", 7)})

	historyWrite(
		derived,
		ErrorLevel,
		"boom",
		strField("site", "handler"),
	)
	// Entry-level data (stack) must be retained too.
	require.NoError(t, h.Write(
		Entry{Level: WarnLevel, Message: "with-stack", Stack: "goroutine 1 [running]:"},
		[]Field{strField("inline", "x")},
	))

	sink := &lockedBuffer{}
	n, err := h.Export(historyEncoder(), sink, DebugLevel)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	lines := sink.lines(t)
	require.Len(t, lines, 2)

	var first map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &first))
	assert.Equal(t, "boom", first["msg"])
	assert.Equal(t, "v1", first["ctx"], "With context must be exported")
	assert.Equal(t, float64(7), first["n"], "chained With context must be exported")
	assert.Equal(t, "handler", first["site"], "log-site fields must be exported")

	var second map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &second))
	assert.Equal(t, "with-stack", second["msg"])
	assert.Equal(t, "goroutine 1 [running]:", second["stacktrace"])
	assert.Equal(t, "x", second["inline"])
}

func TestHistoryCoreExportFiltersByLevel(t *testing.T) {
	h := NewHistoryCore(DebugLevel, 8)
	levels := []Level{DebugLevel, InfoLevel, WarnLevel, ErrorLevel}
	for _, lvl := range levels {
		historyWrite(h, lvl, lvl.String())
	}

	// Threshold enabler: warn and above.
	sink := &lockedBuffer{}
	n, err := h.Export(historyEncoder(), sink, WarnLevel)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	for _, line := range sink.lines(t) {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &m))
		assert.Contains(t, []string{"warn", "error"}, m["level"])
	}

	// Exact level set: debug and error only.
	sink2 := &lockedBuffer{}
	n, err = h.Export(historyEncoder(), sink2, ExactLevels(DebugLevel, ErrorLevel))
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	got := []string{}
	for _, line := range sink2.lines(t) {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &m))
		got = append(got, m["level"].(string))
	}
	assert.Equal(t, []string{"debug", "error"}, got, "filter must preserve write order")
}

func TestHistoryCoreExportIsNonDestructive(t *testing.T) {
	h := NewHistoryCore(DebugLevel, 8)
	historyWrite(h, InfoLevel, "before-1")
	historyWrite(h, InfoLevel, "before-2")

	first := &lockedBuffer{}
	n, err := h.Export(historyEncoder(), first, DebugLevel)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	assert.Equal(t, 2, h.Len(), "export must not clear the window")

	// A second export reproduces the same snapshot.
	second := &lockedBuffer{}
	n, err = h.Export(historyEncoder(), second, DebugLevel)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	assert.Equal(t, first.String(), second.String())

	// Entries written after an export must still be captured.
	historyWrite(h, InfoLevel, "after-export")
	assert.Equal(t, 3, h.Len())

	third := &lockedBuffer{}
	n, err = h.Export(historyEncoder(), third, DebugLevel)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	var m map[string]any
	lines := third.lines(t)
	require.NoError(t, json.Unmarshal([]byte(lines[2]), &m))
	assert.Equal(t, "after-export", m["msg"])
}

func TestHistoryCoreZeroCapacity(t *testing.T) {
	h := NewHistoryCore(DebugLevel, 0)
	historyWrite(h, InfoLevel, "ignored")
	require.NoError(t, h.Write(Entry{Level: InfoLevel, Message: "ignored-too"}, nil))
	assert.Equal(t, 0, h.Len())

	sink := &lockedBuffer{}
	n, err := h.Export(historyEncoder(), sink, DebugLevel)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Empty(t, sink.String())

	// Negative capacity is clamped instead of panicking.
	h2 := NewHistoryCore(DebugLevel, -3)
	assert.Equal(t, 0, h2.Len())
}

func TestHistoryCoreExportHandlesShortWritesWithoutTruncation(t *testing.T) {
	h := NewHistoryCore(DebugLevel, 16)
	for i := 0; i < 20; i++ {
		historyWrite(h, InfoLevel, fmt.Sprintf("message-number-%d", i), intField("i", i))
	}

	chunked := &chunkWriter{chunk: 5}
	n, err := h.Export(historyEncoder(), chunked, DebugLevel)
	require.NoError(t, err)
	require.Equal(t, 16, n)

	reference := &lockedBuffer{}
	_, err = h.Export(historyEncoder(), reference, DebugLevel)
	require.NoError(t, err)

	// Every byte must survive a writer that takes 5 bytes at a time.
	assert.Equal(t, reference.String(), chunked.String())

	for _, line := range strings.Split(strings.TrimRight(chunked.String(), "\n"), "\n") {
		assert.True(t, json.Valid([]byte(line)), "entry truncated or torn: %q", line)
		assert.True(t, strings.HasSuffix(line, "}"), "entry must be a complete line: %q", line)
	}
}

func TestHistoryCoreExportSyncsTarget(t *testing.T) {
	h := NewHistoryCore(DebugLevel, 4)
	historyWrite(h, InfoLevel, "x")
	sink := &lockedBuffer{}
	_, err := h.Export(historyEncoder(), sink, DebugLevel)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, sink.syncs.Load(), int32(1), "export must flush the target")
}

func TestHistoryCoreExportValidatesArguments(t *testing.T) {
	h := NewHistoryCore(DebugLevel, 4)
	_, err := h.Export(nil, &lockedBuffer{}, DebugLevel)
	assert.Error(t, err)
	_, err = h.Export(historyEncoder(), nil, DebugLevel)
	assert.Error(t, err)

	// A nil level filter matches nothing instead of panicking.
	historyWrite(h, InfoLevel, "x")
	n, err := h.Export(historyEncoder(), &lockedBuffer{}, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestHistoryCoreCallerFieldMutationsDoNotCorruptWindow(t *testing.T) {
	h := NewHistoryCore(DebugLevel, 2)
	callerFields := []Field{strField("k", "original")}
	historyWrite(h, InfoLevel, "msg", callerFields...)
	// The caller reuses and mutates the backing array after Write returns.
	callerFields[0] = strField("k", "mutated")

	sink := &lockedBuffer{}
	_, err := h.Export(historyEncoder(), sink, DebugLevel)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(sink.lines(t)[0]), &m))
	assert.Equal(t, "original", m["k"], "retained fields must be independent copies")
}

func TestHistoryCoreClose(t *testing.T) {
	h := NewHistoryCore(DebugLevel, 4)
	historyWrite(h, InfoLevel, "kept")
	require.Equal(t, 1, h.Len())

	require.NoError(t, h.Close())
	require.NoError(t, h.Close(), "double close must be a no-op")

	// Writes after close are dropped without error.
	require.NoError(t, h.Write(Entry{Level: InfoLevel, Message: "dropped"}, nil))
	assert.Equal(t, 1, h.Len())

	_, err := h.Export(historyEncoder(), &lockedBuffer{}, DebugLevel)
	assert.ErrorIs(t, err, ErrHistoryClosed)
}

func TestHistoryCoreConcurrentWriteExportSyncClose(t *testing.T) {
	const (
		capacity  = 128
		writers   = 8
		perWriter = 200
	)
	h := NewHistoryCore(DebugLevel, capacity)
	derived := h.With([]Field{strField("ctx", "shared")})

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sink := &lockedBuffer{}
			for i := 0; i < perWriter; i++ {
				lvl := Level((i + id) % 4) // debug..error
				historyWrite(derived, lvl, fmt.Sprintf("w%d-%d", id, i), intField("i", i))
				if i%37 == 0 {
					// Export must be safe concurrently with writes and syncs.
					_, _ = h.Export(historyEncoder(), sink, ExactLevels(InfoLevel, ErrorLevel))
				}
				if i%53 == 0 {
					_ = h.Sync()
				}
			}
			// Final per-goroutine dump: validate every exported line is a
			// complete JSON entry and never carries the context field of a
			// different entry.
			for _, line := range sink.lines(t) {
				require.True(t, json.Valid([]byte(line)), "torn export: %q", line)
				var m map[string]any
				require.NoError(t, json.Unmarshal([]byte(line), &m))
				assert.Equal(t, "shared", m["ctx"])
				assert.Contains(t, []string{"info", "error"}, m["level"])
			}
		}(w)
	}
	wg.Wait()

	require.Equal(t, capacity, h.Len(), "oldest entries must have been evicted, none lost early")

	// Post-storm write must be captured despite all the concurrent exports.
	historyWrite(derived, InfoLevel, "sentinel-after-storm")
	sink := &lockedBuffer{}
	n, err := h.Export(historyEncoder(), sink, ExactLevels(InfoLevel))
	require.NoError(t, err)
	require.Equal(t, n, len(sink.lines(t)))
	var found bool
	for _, line := range sink.lines(t) {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &m))
		if m["msg"] == "sentinel-after-storm" {
			found = true
		}
	}
	assert.True(t, found, "records written after exports must not be lost")

	require.NoError(t, h.Close())
	_, err = h.Export(historyEncoder(), &lockedBuffer{}, DebugLevel)
	assert.ErrorIs(t, err, ErrHistoryClosed)
}

func TestHistoryCoreConcurrentClose(t *testing.T) {
	h := NewHistoryCore(DebugLevel, 64)
	var wg sync.WaitGroup

	// Writers keep going across Close: no panic, no corruption.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				historyWrite(h, Level(i%4), fmt.Sprintf("w%d-%d", id, i))
			}
		}(w)
	}
	// Exporters race the close; both a successful dump and ErrHistoryClosed
	// are acceptable, anything else (panic, other error) is a failure.
	for e := 0; e < 4; e++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_, err := h.Export(historyEncoder(), &lockedBuffer{}, InfoLevel)
				if err != nil {
					assert.ErrorIs(t, err, ErrHistoryClosed)
				}
				_ = h.Sync()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Millisecond)
		assert.NoError(t, h.Close())
	}()
	wg.Wait()

	_, err := h.Export(historyEncoder(), &lockedBuffer{}, DebugLevel)
	assert.ErrorIs(t, err, ErrHistoryClosed)
}

// TestHistoryCoreExportWriterErrorPropagates ensures write failures surface
// while the window itself remains intact and usable.
func TestHistoryCoreExportWriterErrorPropagates(t *testing.T) {
	h := NewHistoryCore(DebugLevel, 4)
	historyWrite(h, InfoLevel, "x")
	historyWrite(h, InfoLevel, "y")

	wantErr := errors.New("disk gone")
	ws := AddSync(&failAfterOneWriter{err: wantErr})
	_, err := h.Export(historyEncoder(), ws, DebugLevel)
	require.Error(t, err)
	assert.Contains(t, err.Error(), wantErr.Error())

	// Window is unaffected by a failed export.
	assert.Equal(t, 2, h.Len())
	sink := &lockedBuffer{}
	n, err := h.Export(historyEncoder(), sink, DebugLevel)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

type failAfterOneWriter struct {
	calls int
	err   error
}

func (w *failAfterOneWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls > 1 {
		return 0, w.err
	}
	return len(p), nil
}

var _ io.Writer = (*failAfterOneWriter)(nil)
