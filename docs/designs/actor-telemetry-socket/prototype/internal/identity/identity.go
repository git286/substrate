// Package identity models the actor identity that ateom holds for the
// activation currently running in its worker pod.
//
// In Substrate, ateom receives this identity directly in the activation RPC
// (RunWorkloadRequest / RestoreWorkloadRequest carry atespace, actor_name,
// actor_uid, actor_template_namespace, actor_template_name — see
// internal/proto/ateompb/ateom.proto). It is *not* read back out of
// /run/ate/actor-id: that file holds only the actor name
// (cmd/atelet/main.go:807) and does not exist at all on the micro-VM runtime
// (cmd/ateom-microvm/spec.go:92).
package identity

import "sync"

// Attribute keys. Same set as the log labels in internal/actorlog/logger.go so
// the three signals join.
const (
	KeyServiceInstanceID = "service.instance.id"
	KeyActorUID          = "ate.dev/actor_uid"
	KeyActorName         = "ate.dev/actor_name"
	KeyAtespace          = "ate.dev/actor_atespace"
	KeyTemplateNamespace = "ate.dev/actor_template_namespace"
	KeyTemplateName      = "ate.dev/actor_template_name"
	KeyContainerName     = "ate.dev/container_name"

	// Host facts. Deliberately named as host facts, not workload identity.
	// They are attached to traces and logs only — never to metrics, because a
	// metric label that changes on migration re-fragments the series this
	// design exists to keep whole.
	KeyWorkerPod  = "ate.dev/worker_pod"
	KeyWorkerNode = "ate.dev/worker_node"

	// ReservedPrefix is the namespace an actor may not write into. Anything an
	// actor emits under it is stripped before stamping.
	ReservedPrefix = "ate.dev/"
)

// Actor is the trusted identity of one activation.
type Actor struct {
	UID               string
	Name              string
	Atespace          string
	TemplateNamespace string
	TemplateName      string
	ContainerName     string
}

// Worker describes the host this ateom runs on.
type Worker struct {
	PodName  string
	NodeName string
}

// Registry holds the actor currently activated on this worker. A worker hosts
// at most one actor at a time (docs/glossary.md), which is what makes "who
// sent this" have exactly one answer.
type Registry struct {
	mu  sync.RWMutex
	cur *Actor
}

func (r *Registry) Activate(a Actor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := a
	r.cur = &cp
}

func (r *Registry) Deactivate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cur = nil
}

// Current returns the activated actor, if any.
func (r *Registry) Current() (Actor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.cur == nil {
		return Actor{}, false
	}
	return *r.cur, true
}
