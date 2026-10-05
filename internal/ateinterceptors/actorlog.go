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

package ateinterceptors

import (
	"context"
	"log/slog"

	"google.golang.org/grpc"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/contextlogging"
	"github.com/agent-substrate/substrate/internal/resources"
)

// actorAttributed is a request that names the actor it acts on in full.
type actorAttributed interface {
	GetAtespace() string
	GetActorName() string
	GetActorUid() string
	GetActorTemplateAtespace() string
	GetActorTemplateName() string
}

// actorUIDOnly is a request that names its actor by uid alone.
type actorUIDOnly interface {
	GetActorUid() string
}

// ActorLogContextUnaryInterceptor puts the identity of the actor a request
// names on the handler's context, so every record the handler logs through a
// contextlogging handler carries the ate.actor.* keys. A worker hosts many
// actors, and its log interleaves them: a record without the keys cannot be
// told apart from its siblings'. Chain it before InternalServerUnaryInterceptor
// so the "Handle RPC" record carries them too.
func ActorLogContextUnaryInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	return handler(contextlogging.WithAttrs(ctx, actorLogAttrs(req)...), req)
}

func actorLogAttrs(req any) []slog.Attr {
	switch r := req.(type) {
	case actorAttributed:
		if r.GetActorUid() == "" {
			return nil
		}
		return ateattr.ActorLogAttrs(resources.ActorAttribution{
			Ref:              resources.ActorRef{Atespace: r.GetAtespace(), Name: r.GetActorName()},
			UID:              r.GetActorUid(),
			TemplateAtespace: r.GetActorTemplateAtespace(),
			TemplateName:     r.GetActorTemplateName(),
		})
	case actorUIDOnly:
		if r.GetActorUid() == "" {
			return nil
		}
		return []slog.Attr{slog.String(string(ateattr.ActorUIDKey), r.GetActorUid())}
	default:
		return nil
	}
}
