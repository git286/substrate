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

// Package protoredact masks the fields of a protobuf message that are marked
// [debug_redact = true] in the schema, so the message can be logged without
// exposing the secrets it carries. The label is set next to the field in the
// .proto file; this package is the reader for it, since the Go protobuf
// runtime does not act on the option itself.
package protoredact

import (
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Placeholder replaces the value of a singular string field marked
// debug_redact. Names and structure are kept so a log line still shows which
// fields were set.
const Placeholder = "[REDACTED]"

// ForLog returns a value safe to log. A proto message is cloned and every
// field carrying the debug_redact option is masked in the copy; any other
// value is returned unchanged. The original message is never modified.
func ForLog(v any) any {
	msg, ok := v.(proto.Message)
	if !ok {
		return v
	}
	return Clone(msg)
}

// Clone returns m with every debug_redact field masked, without modifying m.
//
// When m's type cannot reach a debug_redact field, which holds for most of our
// RPC messages (Actor, ListActorsResponse, Worker, ...), m itself is returned:
// there is nothing to mask, so nothing to copy. Callers must treat the result
// as read-only. Otherwise a deep copy is made and masked.
func Clone(m proto.Message) proto.Message {
	if m == nil || !HasRedactedFields(m.ProtoReflect().Descriptor()) {
		return m
	}
	clone := proto.Clone(m)
	Redact(clone.ProtoReflect())
	return clone
}

// redactedTypes memoizes HasRedactedFields per message descriptor. The answer
// depends only on the schema, which is fixed for the life of the process, so
// it is computed once per type.
var redactedTypes sync.Map // protoreflect.MessageDescriptor -> bool

// HasRedactedFields reports whether a message of type md can carry a
// debug_redact field, directly or through any nested message, list element
// or map value, however deep. The answer is memoized per descriptor.
func HasRedactedFields(md protoreflect.MessageDescriptor) bool {
	if v, ok := redactedTypes.Load(md); ok {
		return v.(bool)
	}
	r := hasRedactedFields(md, map[protoreflect.MessageDescriptor]bool{})
	redactedTypes.Store(md, r)
	return r
}

// hasRedactedFields is the uncached walk. seen guards recursive schemas
// (a message type that contains itself).
func hasRedactedFields(md protoreflect.MessageDescriptor, seen map[protoreflect.MessageDescriptor]bool) bool {
	if seen[md] {
		return false
	}
	seen[md] = true
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if IsRedacted(fd) {
			return true
		}
		// fd.Message() is the nested type for message and group fields and
		// the entry type for maps, whose value field is checked in turn.
		if sub := fd.Message(); sub != nil && hasRedactedFields(sub, seen) {
			return true
		}
	}
	return false
}

// IsRedacted reports whether fd carries [debug_redact = true].
func IsRedacted(fd protoreflect.FieldDescriptor) bool {
	opts, ok := fd.Options().(*descriptorpb.FieldOptions)
	return ok && opts.GetDebugRedact()
}

// Redact masks, in place, every populated field of msg that carries the
// debug_redact option, recursing through nested messages, lists and map
// values. Singular string fields are replaced with Placeholder; any other
// kind (bytes, repeated, map, message, ...) is cleared, because no honest
// placeholder exists for them. Sub-messages whose type cannot carry a
// debug_redact field are not visited.
func Redact(msg protoreflect.Message) {
	if !HasRedactedFields(msg.Descriptor()) {
		return
	}
	msg.Range(func(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if IsRedacted(fd) {
			if fd.Kind() == protoreflect.StringKind && !fd.IsList() && !fd.IsMap() {
				msg.Set(fd, protoreflect.ValueOfString(Placeholder))
			} else {
				msg.Clear(fd)
			}
			return true
		}
		switch {
		case fd.IsMap():
			// A map field reports MessageKind (its entry type) but its value
			// is a Map, so this case must come before the message case.
			if fd.MapValue().Kind() == protoreflect.MessageKind && HasRedactedFields(fd.MapValue().Message()) {
				value.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					Redact(mv.Message())
					return true
				})
			}
		case fd.IsList():
			if (fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind) && HasRedactedFields(fd.Message()) {
				list := value.List()
				for i := 0; i < list.Len(); i++ {
					Redact(list.Get(i).Message())
				}
			}
		case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
			if HasRedactedFields(fd.Message()) {
				Redact(value.Message())
			}
		}
		return true
	})
}
