// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ateinterceptors

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"google.golang.org/grpc"

	"github.com/agent-substrate/substrate/internal/contextlogging"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

func TestActorLogContextUnaryInterceptor(t *testing.T) {
	tests := []struct {
		name string
		req  any
		want map[string]any
	}{
		{
			name: "full attribution",
			req: &ateompb.RunWorkloadRequest{
				Atespace:              "space",
				ActorName:             "a1",
				ActorUid:              "uid-1",
				ActorTemplateAtespace: "tspace",
				ActorTemplateName:     "tmpl",
			},
			want: map[string]any{
				"ate.atespace":          "space",
				"ate.actor.name":        "a1",
				"ate.actor.uid":         "uid-1",
				"ate.template.atespace": "tspace",
				"ate.template.name":     "tmpl",
			},
		},
		{
			name: "uid only",
			req:  &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-2"},
			want: map[string]any{"ate.actor.uid": "uid-2"},
		},
		{
			name: "no actor",
			req:  &ateompb.GetActiveWorkloadStatsRequest{},
			want: map[string]any{},
		},
		{
			name: "empty uid adds nothing",
			req:  &ateompb.CheckpointWorkloadRequest{ActorName: "a1"},
			want: map[string]any{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(contextlogging.NewHandler(slog.NewJSONHandler(&buf, nil)))
			handler := func(ctx context.Context, _ any) (any, error) {
				logger.InfoContext(ctx, "in handler")
				return nil, nil
			}
			if _, err := ActorLogContextUnaryInterceptor(context.Background(), tt.req, &grpc.UnaryServerInfo{}, handler); err != nil {
				t.Fatalf("interceptor: %v", err)
			}
			var rec map[string]any
			if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
				t.Fatalf("failed to parse log record %q: %v", buf.String(), err)
			}
			for _, k := range []string{"time", "level", "msg"} {
				delete(rec, k)
			}
			if len(rec) != len(tt.want) {
				t.Errorf("record attrs = %v, want %v", rec, tt.want)
			}
			for k, v := range tt.want {
				if rec[k] != v {
					t.Errorf("%s = %v, want %v", k, rec[k], v)
				}
			}
		})
	}
}
