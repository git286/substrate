//go:build linux

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os/exec"
	"strings"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	"github.com/agent-substrate/substrate/internal/ateattr"
)

const deadVMMMsg = "Sandbox VMM exited while the actor is hosted"

// captureWarnings routes the default logger to a buffer for the test.
func captureWarnings(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// deadVMMRecords returns the dead-VMM records in logs, decoded.
func deadVMMRecords(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if rec["msg"] == deadVMMMsg {
			out = append(out, rec)
		}
	}
	return out
}

func readOOMKillsReturning(n uint64, ok bool, err error) func(string) (uint64, bool, error) {
	return func(string) (uint64, bool, error) { return n, ok, err }
}

// TestSweepWarnsOnceWhenVMMExits: a hosted actor whose cloud-hypervisor is gone
// is warned about once per activation, with its leaf's OOM kills when they can
// be read, and not at all while a lifecycle RPC is tearing it down.
func TestSweepWarnsOnceWhenVMMExits(t *testing.T) {
	for _, tc := range []struct {
		name string
		// hasVM is false for an actor still booting or restoring.
		hasVM     bool
		exited    bool
		busy      bool
		readOOM   func(string) (uint64, bool, error) // nil: a worker without actor leaves
		wantWarns int
		wantOOM   any // nil: no ate.sandbox.oom_kills key
	}{
		{name: "OOM-killed VMM warns once", hasVM: true, exited: true, readOOM: readOOMKillsReturning(3, true, nil), wantWarns: 1, wantOOM: float64(3)},
		{name: "killed VMM warns with zero OOM kills", hasVM: true, exited: true, readOOM: readOOMKillsReturning(0, true, nil), wantWarns: 1, wantOOM: float64(0)},
		{name: "unreadable count is left out", hasVM: true, exited: true, readOOM: readOOMKillsReturning(0, false, errors.New("boom")), wantWarns: 1},
		{name: "worker without actor leaves leaves the count out", hasVM: true, exited: true, wantWarns: 1},
		{name: "teardown in progress is skipped", hasVM: true, exited: true, busy: true, readOOM: readOOMKillsReturning(3, true, nil)},
		{name: "running VMM does not warn", hasVM: true, readOOM: readOOMKillsReturning(0, true, nil)},
		{name: "actor with no VM yet does not warn", exited: true, readOOM: readOOMKillsReturning(0, true, nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureWarnings(t)
			s := newStatsService(&fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5_000_000)}}, "app_ovl")
			s.vmmExited = func(*runningActor) bool { return tc.exited }
			s.readOOMKills = tc.readOOM
			if tc.hasVM {
				s.setRunningVM(testActor.UID, &runningActor{})
			}
			if tc.busy {
				if !s.locks.Lock(context.Background(), testActor.UID) {
					t.Fatal("could not take the actor lock")
				}
				defer s.locks.Unlock(testActor.UID)
			}

			// Two sweeps: the second must not repeat the warning.
			for range 2 {
				got, err := sweepAndList(s)
				if err != nil {
					t.Fatalf("sweepAndList() error = %v, want nil", err)
				}
				if len(got.GetSamples()) != 1 {
					t.Fatalf("sweepAndList() = %v, want one sample", got)
				}
			}

			recs := deadVMMRecords(t, logs)
			if len(recs) != tc.wantWarns {
				t.Fatalf("got %d dead-VMM warnings, want %d; logs:\n%s", len(recs), tc.wantWarns, logs)
			}
			if tc.wantWarns == 0 {
				return
			}
			rec := recs[0]
			if rec["level"] != "WARN" {
				t.Errorf("level = %v, want WARN", rec["level"])
			}
			if got := rec[string(ateattr.ActorUIDKey)]; got != testActor.UID {
				t.Errorf("%s = %v, want %q", ateattr.ActorUIDKey, got, testActor.UID)
			}
			if got := rec[string(ateattr.TemplateNameKey)]; got != testActor.TemplateName {
				t.Errorf("%s = %v, want %q", ateattr.TemplateNameKey, got, testActor.TemplateName)
			}
			got, present := rec[string(ateattr.SandboxOOMKillsKey)]
			if tc.wantOOM == nil {
				if present {
					t.Errorf("%s = %v, want it left out", ateattr.SandboxOOMKillsKey, got)
				}
			} else if got != tc.wantOOM {
				t.Errorf("%s = %v, want %v", ateattr.SandboxOOMKillsKey, got, tc.wantOOM)
			}
		})
	}
}

// TestSweepWarnsAgainForANewActivation: the once-per-activation guard belongs
// to the hosted record, so a re-hosted actor whose new VMM dies is reported.
func TestSweepWarnsAgainForANewActivation(t *testing.T) {
	logs := captureWarnings(t)
	s := newStatsService(&fakeAgent{}, "app_ovl")
	s.vmmExited = func(*runningActor) bool { return true }
	s.setRunningVM(testActor.UID, &runningActor{})
	s.sweepUsage(context.Background())

	unhostTestActor(s, testActor.UID)
	hostTestActor(s, testActor, &guestStatsTarget{actorUID: testActor.UID, agent: &fakeAgent{}, workloadIDs: []string{"app_ovl"}})
	s.setRunningVM(testActor.UID, &runningActor{})
	s.sweepUsage(context.Background())

	if n := len(deadVMMRecords(t, logs)); n != 2 {
		t.Fatalf("got %d dead-VMM warnings, want 2; logs:\n%s", n, logs)
	}
}

// TestVMMProcessExited checks the real process check against a child that the
// test reaps behind os/exec's back, as ateom-microvm's child reaper does.
func TestVMMProcessExited(t *testing.T) {
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	vm := &runningActor{chCmd: cmd}
	if vmmProcessExited(vm) {
		t.Fatal("vmmProcessExited() = true for a running process")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	var ws unix.WaitStatus
	if _, err := unix.Wait4(cmd.Process.Pid, &ws, 0, nil); err != nil {
		t.Fatal(err)
	}
	if !vmmProcessExited(vm) {
		t.Fatal("vmmProcessExited() = false for a reaped process")
	}
	if vmmProcessExited(&runningActor{}) {
		t.Error("vmmProcessExited() = true for a VM with no process")
	}
}
