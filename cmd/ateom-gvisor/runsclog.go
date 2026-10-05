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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

// maxRunscLogLine bounds one logged line of runsc output; the rest of a longer
// line is dropped rather than buffered.
const maxRunscLogLine = 16 << 10

// runscOutput returns the file to give a runsc invocation as its stdout and
// stderr, and a func that releases ateom's copy once the invocation has
// returned. Each line written to it is logged as an ateom record carrying the
// actor's uid, the runsc subcommand, and the container, plus whatever identity
// ctx carries.
//
// runsc's own logging otherwise lands on the pod log as bare glog lines, and a
// worker runs one runsc per actor at a time: identical lines from N actors
// cannot be told apart.
//
// A pipe, not an io.Writer: the sandbox a root-container create or restore
// starts inherits the descriptor and keeps it for its lifetime, and os/exec
// would wait for a non-file writer's copy to finish, which is to say for the
// sandbox to exit. The reader runs until every holder has closed its end.
//
// Not for an application container's create, start, or restore: there the
// descriptors become the container's own stdio, which goes to the actor log
// pipe instead.
func runscOutput(ctx context.Context, actorUID, command, container string) (*os.File, func()) {
	pr, pw, err := os.Pipe()
	if err != nil {
		slog.WarnContext(ctx, "Failed to open a pipe for runsc output; writing it to stderr unattributed",
			slog.String("runsc.command", command), slog.Any("err", err))
		return os.Stderr, func() {}
	}
	attrs := []slog.Attr{
		slog.String(string(ateattr.ActorUIDKey), actorUID),
		slog.String("runsc.command", command),
		slog.String("container", container),
	}
	go func() {
		defer pr.Close()
		logRunscOutput(context.WithoutCancel(ctx), slog.Default(), pr, attrs)
	}()
	return pw, func() { _ = pw.Close() }
}

// logRunscOutput logs each line read from r to logger until EOF.
func logRunscOutput(ctx context.Context, logger *slog.Logger, r io.Reader, attrs []slog.Attr) {
	br := bufio.NewReader(r)
	var line []byte
	for {
		chunk, isPrefix, err := br.ReadLine()
		if len(line) < maxRunscLogLine {
			line = append(line, chunk[:min(len(chunk), maxRunscLogLine-len(line))]...)
		}
		if err == nil && isPrefix {
			continue
		}
		if len(line) > 0 {
			level, msg := parseRunscLine(string(line))
			logger.LogAttrs(ctx, level, msg, attrs...)
		}
		line = line[:0]
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, os.ErrClosed) {
				logger.DebugContext(ctx, "Stopped reading runsc output", slog.Any("err", err))
			}
			return
		}
	}
}

// parseRunscLine returns the level and message to log line at. runsc writes
// glog text ("W1005 15:09:31.726680  25 file.go:1] ...") with
// --alsologtostderr and JSON with -log-format json; the severity is kept so a
// warning stays a warning. Anything else is logged whole at info.
func parseRunscLine(line string) (slog.Level, string) {
	if strings.HasPrefix(line, "{") {
		var rec struct {
			Msg   string `json:"msg"`
			Level string `json:"level"`
		}
		if json.Unmarshal([]byte(line), &rec) == nil && rec.Msg != "" {
			return runscLevel(rec.Level), rec.Msg
		}
	}
	if len(line) > 5 && strings.ContainsRune("IWEF", rune(line[0])) && isDigits(line[1:5]) {
		return runscLevel(line[:1]), line
	}
	return slog.LevelInfo, line
}

func runscLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "d", "debug":
		return slog.LevelDebug
	case "w", "warning", "warn":
		return slog.LevelWarn
	case "e", "error", "f", "fatal":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func isDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
