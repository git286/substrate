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
	"log/slog"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/contextlogging"
)

func TestParseRunscLine(t *testing.T) {
	tests := []struct {
		line      string
		wantLevel slog.Level
		wantMsg   string
	}{
		{"W1005 15:09:31.726680      25 container.go:1824] gVisor startup is slower", slog.LevelWarn, "W1005 15:09:31.726680      25 container.go:1824] gVisor startup is slower"},
		{"I1005 15:09:31.788184     201 cli.go:317] Exiting with status: 0", slog.LevelInfo, "I1005 15:09:31.788184     201 cli.go:317] Exiting with status: 0"},
		{"E1005 15:09:31.000000       1 x.go:1] boom", slog.LevelError, "E1005 15:09:31.000000       1 x.go:1] boom"},
		{"F1005 15:09:31.000000       1 x.go:1] fatal", slog.LevelError, "F1005 15:09:31.000000       1 x.go:1] fatal"},
		{`{"msg":"restore failed","level":"warning","time":"2026-10-05T15:09:31Z"}`, slog.LevelWarn, "restore failed"},
		{`{"msg":"hi","level":"debug"}`, slog.LevelDebug, "hi"},
		{`{"ociVersion":"1.0.2","id":"_pause","status":"running"}`, slog.LevelInfo, `{"ociVersion":"1.0.2","id":"_pause","status":"running"}`},
		{"Wrong word, not glog", slog.LevelInfo, "Wrong word, not glog"},
		{"W", slog.LevelInfo, "W"},
	}
	for _, tt := range tests {
		gotLevel, gotMsg := parseRunscLine(tt.line)
		if gotLevel != tt.wantLevel || gotMsg != tt.wantMsg {
			t.Errorf("parseRunscLine(%q) = (%v, %q), want (%v, %q)", tt.line, gotLevel, gotMsg, tt.wantLevel, tt.wantMsg)
		}
	}
}

func decodeRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var recs []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("failed to parse log record %q: %v", line, err)
		}
		recs = append(recs, rec)
	}
	return recs
}

func TestLogRunscOutput(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(contextlogging.NewHandler(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	ctx := contextlogging.WithAttrs(context.Background(), slog.String("ate.actor.name", "a1"), slog.String("ate.actor.uid", "uid-1"))
	attrs := []slog.Attr{slog.String("ate.actor.uid", "uid-1"), slog.String("runsc.command", "create"), slog.String("container", "_pause")}
	long := strings.Repeat("x", maxRunscLogLine+100)
	in := "W1005 15:09:31.726680      25 container.go:1824] slow\r\n\n" + long + "\nlast line without newline"

	logRunscOutput(ctx, logger, strings.NewReader(in), attrs)

	recs := decodeRecords(t, &buf)
	if len(recs) != 3 {
		t.Fatalf("got %d records, want 3: %s", len(recs), buf.String())
	}
	for i, rec := range recs {
		for k, want := range map[string]string{"ate.actor.uid": "uid-1", "ate.actor.name": "a1", "runsc.command": "create", "container": "_pause"} {
			if rec[k] != want {
				t.Errorf("record %d: %s = %v, want %q", i, k, rec[k], want)
			}
		}
	}
	if recs[0]["level"] != "WARN" || recs[0]["msg"] != "W1005 15:09:31.726680      25 container.go:1824] slow" {
		t.Errorf("record 0 = %v, want a WARN with the line, CR stripped", recs[0])
	}
	if got := len(recs[1]["msg"].(string)); got != maxRunscLogLine {
		t.Errorf("long line logged with %d bytes, want it cut to %d", got, maxRunscLogLine)
	}
	if recs[2]["msg"] != "last line without newline" {
		t.Errorf("record 2 msg = %v, want the unterminated last line", recs[2]["msg"])
	}
	if n := strings.Count(buf.String(), `"ate.actor.uid"`); n != 3 {
		t.Errorf("ate.actor.uid appears %d times over 3 records, want once each", n)
	}
}

// TestRunscOutputDoesNotWaitForInheritors pins why runscOutput hands out a
// pipe: a child that outlives the command, as the sandbox a root-container
// create starts does, keeps the descriptor, and the command must still return.
func TestRunscOutputDoesNotWaitForInheritors(t *testing.T) {
	out, done := runscOutput(context.Background(), "uid-1", "create", "_pause")
	defer done()
	cmd := exec.Command("sh", "-c", "echo started; (sleep 30 &) ")
	cmd.Stdout = out
	cmd.Stderr = out
	finished := make(chan error, 1)
	go func() { finished <- cmd.Run() }()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("command: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("command did not return while a background child held its output")
	}
}
