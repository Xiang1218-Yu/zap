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
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	//revive:disable:dot-imports
	. "go.uber.org/zap/zapcore"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newBranchCore() (*lockedBuffer, Core) {
	sink := &lockedBuffer{}
	return sink, NewCore(historyEncoder(), Lock(sink), DebugLevel)
}

func writeLevel(core Core, lvl Level, msg string, fields ...Field) {
	if ce := core.Check(Entry{Level: lvl, Message: msg}, nil); ce != nil {
		ce.Write(fields...)
	}
}

func levelsIn(t *testing.T, sink *lockedBuffer) map[string][]string {
	t.Helper()
	byLevel := map[string][]string{}
	for _, line := range sink.lines(t) {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &m), "malformed line: %q", line)
		lvl := m["level"].(string)
		byLevel[lvl] = append(byLevel[lvl], m["msg"].(string))
	}
	return byLevel
}

func mapKeys(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// incidentRouter wires console (debug/info), running file (warn), alert file
// (error/dpanic/panic) and the default target (everything else).
func incidentRouter() (Core, *lockedBuffer, *lockedBuffer, *lockedBuffer, *lockedBuffer) {
	consoleSink, console := newBranchCore()
	runningSink, running := newBranchCore()
	alertSink, alert := newBranchCore()
	defaultSink, def := newBranchCore()

	router := NewLevelRouterCore(def, map[Level]Core{
		DebugLevel:  console,
		InfoLevel:   console,
		WarnLevel:   running,
		ErrorLevel:  alert,
		DPanicLevel: alert,
		PanicLevel:  alert,
	})
	return router, consoleSink, runningSink, alertSink, defaultSink
}

func TestLevelRouterRoutesEachLevelExactlyOnce(t *testing.T) {
	router, consoleSink, runningSink, alertSink, defaultSink := incidentRouter()

	all := []Level{DebugLevel, InfoLevel, WarnLevel, ErrorLevel, DPanicLevel, PanicLevel, FatalLevel}
	for i, lvl := range all {
		writeLevel(router, lvl, fmt.Sprintf("m-%d", i))
	}

	console := levelsIn(t, consoleSink)
	running := levelsIn(t, runningSink)
	alert := levelsIn(t, alertSink)
	def := levelsIn(t, defaultSink)

	assert.ElementsMatch(t, []string{"debug", "info"}, mapKeys(console))
	assert.ElementsMatch(t, []string{"warn"}, mapKeys(running))
	assert.ElementsMatch(t, []string{"error", "dpanic", "panic"}, mapKeys(alert))
	assert.ElementsMatch(t, []string{"fatal"}, mapKeys(def), "unmatched level must go to default")

	total := len(consoleSink.lines(t)) + len(runningSink.lines(t)) +
		len(alertSink.lines(t)) + len(defaultSink.lines(t))
	assert.Equal(t, len(all), total, "every entry routed exactly once, no duplicates")
}

func TestLevelRouterNilFallbackAndMissingRoutes(t *testing.T) {
	sink, console := newBranchCore()
	router := NewLevelRouterCore(nil, map[Level]Core{InfoLevel: console})

	// Unmatched levels are discarded through the implicit nop default.
	writeLevel(router, DebugLevel, "dropped-debug")
	writeLevel(router, WarnLevel, "dropped-warn")
	writeLevel(router, InfoLevel, "kept")

	lines := sink.lines(t)
	require.Len(t, lines, 1)
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &m))
	assert.Equal(t, "kept", m["msg"])
}

func TestLevelRouterEnabledAndMinimumLevel(t *testing.T) {
	sinkInfo := &lockedBuffer{}
	console := NewCore(historyEncoder(), Lock(sinkInfo), InfoLevel)
	sinkWarn := &lockedBuffer{}
	warnOnly := NewCore(historyEncoder(), Lock(sinkWarn), WarnLevel)

	router := NewLevelRouterCore(warnOnly, map[Level]Core{
		InfoLevel: console,
	})

	assert.False(t, router.Enabled(DebugLevel), "no branch enables debug")
	assert.True(t, router.Enabled(InfoLevel))
	assert.True(t, router.Enabled(WarnLevel), "fallback enables warn")
	assert.Equal(t, InfoLevel, LevelOf(router), "minimum enabled level across targets")
}

func TestLevelRouterWithFields(t *testing.T) {
	router, consoleSink, _, alertSink, defaultSink := incidentRouter()
	withCtx := router.With([]Field{strField("request", "r-42")})

	writeLevel(withCtx, InfoLevel, "contextual-info")
	writeLevel(withCtx, ErrorLevel, "contextual-error")
	writeLevel(router, InfoLevel, "no-context")

	for _, line := range consoleSink.lines(t) {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &m))
		if m["msg"] == "contextual-info" {
			assert.Equal(t, "r-42", m["request"])
		} else {
			assert.NotContains(t, m, "request")
		}
	}
	var alertLine map[string]any
	require.NoError(t, json.Unmarshal([]byte(alertSink.lines(t)[0]), &alertLine))
	assert.Equal(t, "r-42", alertLine["request"])
	assert.Empty(t, defaultSink.lines(t), "derived router must not change routing")
}

type syncCountCore struct {
	Core
	syncs atomic.Int32
}

func (c *syncCountCore) Sync() error {
	c.syncs.Add(1)
	return c.Core.Sync()
}

func TestLevelRouterSyncsEachDistinctCoreOnce(t *testing.T) {
	_, sharedBranch := newBranchCore()
	shared := &syncCountCore{Core: sharedBranch}
	_, otherBranch := newBranchCore()
	other := &syncCountCore{Core: otherBranch}
	_, fallbackBranch := newBranchCore()
	fallback := &syncCountCore{Core: fallbackBranch}

	// Same core registered for two levels, another core for one level, plus
	// the fallback: three distinct cores.
	router := NewLevelRouterCore(fallback, map[Level]Core{
		DebugLevel: shared,
		InfoLevel:  shared,
		WarnLevel:  other,
	})

	require.NoError(t, router.Sync())
	assert.Equal(t, int32(1), shared.syncs.Load(), "shared target synced once despite two routes")
	assert.Equal(t, int32(1), other.syncs.Load())
	assert.Equal(t, int32(1), fallback.syncs.Load())
}

func TestLevelRouterInBranchMultiOutput(t *testing.T) {
	// A routed branch may itself fan out to multiple outputs; the router must
	// still keep levels from leaking into other branches.
	sinkA, coreA := newBranchCore()
	sinkB, coreB := newBranchCore()
	multi := NewTee(coreA, coreB)
	_, fallback := newBranchCore()

	router := NewLevelRouterCore(fallback, map[Level]Core{
		InfoLevel: multi,
	})
	writeLevel(router, InfoLevel, "dup-within-branch")
	writeLevel(router, WarnLevel, "fallback-only")

	assert.Len(t, sinkA.lines(t), 1)
	assert.Len(t, sinkB.lines(t), 1)
}

func TestLevelRouterConcurrentWriteSyncWith(t *testing.T) {
	router, consoleSink, runningSink, alertSink, defaultSink := incidentRouter()

	const writers = 8
	const perWriter = 300
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			withFields := router.With([]Field{intField("w", id)})
			for i := 0; i < perWriter; i++ {
				lvl := Level((i+id)%7) - 1 // debug..fatal
				msg := fmt.Sprintf("w%d-%d", id, i)
				if i%2 == 0 {
					writeLevel(withFields, lvl, msg)
				} else {
					writeLevel(router, lvl, msg)
				}
				if i%41 == 0 {
					_ = router.Sync()
				}
			}
		}(w)
	}
	wg.Wait()
	require.NoError(t, router.Sync())

	allowed := map[string]bool{
		"debug": true, "info": true, "warn": true,
		"error": true, "dpanic": true, "panic": true, "fatal": true,
	}
	sinks := []*lockedBuffer{consoleSink, runningSink, alertSink, defaultSink}
	total := 0
	for _, sink := range sinks {
		for _, line := range sink.lines(t) {
			var m map[string]any
			require.True(t, json.Valid([]byte(line)), "torn line: %q", line)
			require.NoError(t, json.Unmarshal([]byte(line), &m))
			lvl, ok := m["level"].(string)
			if !assert.True(t, ok, "line missing level: %q", line) {
				t.Fatalf("bad line: %q", line)
			}
			assert.True(t, allowed[lvl], "unexpected level %q in %q", lvl, line)
			total++
		}
	}
	assert.Equal(t, writers*perWriter, total, "no entry lost or double-routed under concurrency")
}

func TestLevelRouterWithBufferedWriteSyncerFlush(t *testing.T) {
	raw := &lockedBuffer{}
	bws := &BufferedWriteSyncer{WS: raw, Size: 128, FlushInterval: time.Hour}

	branch := NewCore(historyEncoder(), bws, DebugLevel)
	router := NewLevelRouterCore(NewNopCore(), map[Level]Core{InfoLevel: branch})

	const writers = 4
	const perWriter = 100
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				writeLevel(router, InfoLevel, fmt.Sprintf("w%d-%d", id, i))
				if i%20 == 0 {
					_ = router.Sync()
				}
			}
		}(w)
	}
	wg.Wait()
	require.NoError(t, bws.Stop())

	assert.Len(t, raw.lines(t), writers*perWriter, "flush must deliver every entry exactly once")
}

func TestLevelRouterWithSampler(t *testing.T) {
	sink, console := newBranchCore()
	router := NewLevelRouterCore(NewNopCore(), map[Level]Core{
		InfoLevel: console,
	})
	sampled := NewSamplerWithOptions(router, time.Hour, 2, 0)

	for i := 0; i < 5; i++ {
		writeLevel(sampled, InfoLevel, "repeat")
	}
	require.NoError(t, sampled.Sync())

	// First 2 pass; the sampler drops the other 3 before routing.
	assert.Len(t, sink.lines(t), 2, "sampling must still compose with the router")
}

// TestHistoryRoutingIncidentScenario wires the history window, the level
// router, sampling and fields together: normal outputs keep working by level,
// and on failure the preceding context can be exported to another target
// without disturbing subsequent logging.
func TestHistoryRoutingIncidentScenario(t *testing.T) {
	router, consoleSink, runningSink, alertSink, defaultSink := incidentRouter()
	history := NewHistoryCore(DebugLevel, 64)

	// Sampling wraps the whole tee: dropped entries must reach neither the
	// routed outputs nor the history window.
	sampled := NewSamplerWithOptions(NewTee(router, history), time.Hour, 3, 0)
	logger := sampled.With([]Field{strField("request", "req-1")})

	// Repetitive info traffic: only the first 3 are sampled.
	for i := 0; i < 7; i++ {
		writeLevel(logger, InfoLevel, "repeated-info")
	}
	// Unique context leading up to the failure.
	writeLevel(logger, DebugLevel, "enter-handler")
	writeLevel(logger, WarnLevel, "slow-downstream")
	writeLevel(logger, ErrorLevel, "downstream-unavailable")

	// Incident response: dump the context entries (debug/info/warn) retained
	// before the error to a separate alert export target.
	alertExport := &lockedBuffer{}
	n, err := history.Export(historyEncoder(), alertExport, ExactLevels(DebugLevel, InfoLevel, WarnLevel))
	require.NoError(t, err)
	assert.Equal(t, 5, n, "3 sampled infos + debug + warn")

	msgs := make([]string, 0, n)
	for _, line := range alertExport.lines(t) {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &m))
		assert.Equal(t, "req-1", m["request"], "exported context must carry With fields")
		msgs = append(msgs, m["msg"].(string))
	}
	assert.Equal(t, []string{
		"repeated-info", "repeated-info", "repeated-info",
		"enter-handler", "slow-downstream",
	}, msgs, "export preserves write order and samples consistently")

	// Normal routed outputs: exact per-level delivery, sampling applied.
	assert.Len(t, consoleSink.lines(t), 4, "3 sampled infos + the debug entry reach console")
	consoleByLevel := levelsIn(t, consoleSink)
	assert.Len(t, consoleByLevel["info"], 3, "sampled info count")
	assert.Len(t, consoleByLevel["debug"], 1)
	assert.Len(t, runningSink.lines(t), 1)
	assert.Len(t, alertSink.lines(t), 1, "error goes to the alert file")
	assert.Empty(t, defaultSink.lines(t))

	// The export must not consume the window: subsequent records keep flowing.
	writeLevel(logger, InfoLevel, "after-incident")
	again := &lockedBuffer{}
	_, err = history.Export(historyEncoder(), again, ExactLevels(InfoLevel))
	require.NoError(t, err)
	var found bool
	for _, line := range again.lines(t) {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &m))
		if m["msg"] == "after-incident" {
			found = true
		}
	}
	assert.True(t, found, "records after an export must not be lost")
}
