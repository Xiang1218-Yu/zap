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

import "go.uber.org/multierr"

// exactLevelsEnabler enables only the levels explicitly present in its set,
// unlike a plain [Level] which enables the level and everything above it.
type exactLevelsEnabler struct {
	set map[Level]struct{}
}

// Enabled reports true only when lvl is one of the levels the enabler was
// created with.
func (e exactLevelsEnabler) Enabled(lvl Level) bool {
	_, ok := e.set[lvl]
	return ok
}

// ExactLevels returns a [LevelEnabler] that enables exactly the supplied
// levels (and no others). It is useful for selecting specific levels, e.g.
// exporting only debug and info entries from a [HistoryCore]:
//
//	history.Export(enc, alertSink, zapcore.ExactLevels(zapcore.DebugLevel, zapcore.InfoLevel))
func ExactLevels(levels ...Level) LevelEnabler {
	set := make(map[Level]struct{}, len(levels))
	for _, lvl := range levels {
		set[lvl] = struct{}{}
	}
	return exactLevelsEnabler{set: set}
}

// levelRouterCore routes every entry to exactly one underlying Core, chosen by
// the entry's level: an explicit route registered for that level wins,
// otherwise the entry goes to the default core. It is the building block for
// sending, for example, debug/info to the console, warnings to a running log
// file, errors to an alert file, and anything else to the default target.
//
// Unlike [NewTee], an entry is never delivered to more than one target.
type levelRouterCore struct {
	fallback Core
	routes   [_numLevels]Core
	// targets lists each distinct underlying Core exactly once, fallback
	// first. It drives Sync fan-out (once per Core, not once per level) and
	// the per-target With mapping.
	targets []Core
}

var (
	_ Core           = (*levelRouterCore)(nil)
	_ leveledEnabler = (*levelRouterCore)(nil)
)

// NewLevelRouterCore creates a Core that routes each entry to exactly one
// underlying Core according to its level: the Core registered in routes for
// that level, or fallback when no route exists (or the route is nil).
//
// fallback is the default target and must be non-nil; pass [NewNopCore] to
// discard unmatched levels. Only the standard levels (DebugLevel through
// FatalLevel) are routable through the map; the same Core may be registered
// for multiple levels and is synced only once per [Core.Sync] call.
//
// Example:
//
//	core := zapcore.NewLevelRouterCore(defaultCore, map[zapcore.Level]zapcore.Core{
//	    zapcore.DebugLevel: consoleCore,
//	    zapcore.InfoLevel:  consoleCore,
//	    zapcore.WarnLevel:  runningFileCore,
//	    zapcore.ErrorLevel: alertFileCore,
//	})
func NewLevelRouterCore(fallback Core, routes map[Level]Core) Core {
	if fallback == nil {
		fallback = NewNopCore()
	}

	r := &levelRouterCore{fallback: fallback}

	// Cores are compared by interface identity. A linear scan is used instead
	// of a map because Core values may be unhashable (e.g. multiCore is a
	// slice); the number of targets is small.
	r.targets = append(r.targets, fallback)

	for lvl := _minLevel; lvl <= _maxLevel; lvl++ {
		target := routes[lvl]
		if target == nil {
			continue
		}
		r.routes[lvl-_minLevel] = target
		if indexOfCore(r.targets, target) < 0 {
			r.targets = append(r.targets, target)
		}
	}
	return r
}

// indexOfCore returns the first index of target in cores, comparing by
// interface identity, or -1 when absent.
func indexOfCore(cores []Core, target Core) int {
	for i, core := range cores {
		if core == target {
			return i
		}
	}
	return -1
}

// target returns the single Core an entry at lvl must be delivered to.
func (c *levelRouterCore) target(lvl Level) Core {
	if lvl >= _minLevel && lvl <= _maxLevel {
		if target := c.routes[lvl-_minLevel]; target != nil {
			return target
		}
	}
	return c.fallback
}

// Enabled reports whether the single target selected for lvl enables it.
func (c *levelRouterCore) Enabled(lvl Level) bool {
	return c.target(lvl).Enabled(lvl)
}

// Level reports the smallest enabled level across all targets, so the router
// composes with [LevelOf] and level-aware wrappers.
func (c *levelRouterCore) Level() Level {
	minLvl := InvalidLevel
	for _, target := range c.targets {
		if lvl := LevelOf(target); lvl < minLvl {
			minLvl = lvl
		}
	}
	return minLvl
}

// Check delegates to exactly one target Core; the router never registers
// itself, so an entry cannot be written to two targets ("duplicate routing").
func (c *levelRouterCore) Check(ent Entry, ce *CheckedEntry) *CheckedEntry {
	return c.target(ent.Level).Check(ent, ce)
}

// Write delivers the entry to its single selected target. Under the normal
// Check/Write flow this method is not reached (the target registers itself),
// but direct callers still get unambiguous, single-destination routing.
func (c *levelRouterCore) Write(ent Entry, fields []Field) error {
	return c.target(ent.Level).Write(ent, fields)
}

// With applies the context fields to each distinct underlying Core exactly
// once and shares the derived Core between all levels that shared the
// original.
func (c *levelRouterCore) With(fields []Field) Core {
	if len(fields) == 0 {
		return c
	}

	// Derive each distinct target once; clones align 1:1 with c.targets.
	clones := make([]Core, len(c.targets))
	for i, target := range c.targets {
		clones[i] = target.With(fields)
	}

	clone := &levelRouterCore{
		fallback: clones[indexOfCore(c.targets, c.fallback)],
		targets:  clones,
	}
	for i := range c.routes {
		if t := c.routes[i]; t != nil {
			clone.routes[i] = clones[indexOfCore(c.targets, t)]
		}
	}
	return clone
}

// Sync flushes every distinct underlying Core exactly once, regardless of how
// many levels route to it.
func (c *levelRouterCore) Sync() error {
	var err error
	for _, target := range c.targets {
		err = multierr.Append(err, target.Sync())
	}
	return err
}
