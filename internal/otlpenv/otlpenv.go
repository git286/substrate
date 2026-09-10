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

// Package otlpenv resolves the transport half of the standard
// OTEL_EXPORTER_OTLP_* environment variables: the endpoint's scheme, the
// INSECURE override, and the CA and client certificate files.
//
// The OpenTelemetry SDK reads these itself inside its exporters. Substrate needs
// the same answer in two places the SDK does not reach. serverboot has to know
// whether any OTLP variable is set at all, because a process with none keeps
// the plaintext default the exporters always had rather than the SDK's TLS
// default. The atelet relay dials the collector with a plain gRPC client instead
// of an SDK exporter, and must agree with the exporters beside it on whether
// that connection is TLS.
//
// The rules are the SDK's: an http or unix scheme means plaintext, https or no
// scheme means TLS, a parseable OTEL_EXPORTER_OTLP_INSECURE overrides the
// scheme, any certificate variable forces TLS over both, and the
// signal-specific variable wins over the generic one.
package otlpenv

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Signal selects which signal-specific variables take precedence.
type Signal string

const (
	Traces  Signal = "TRACES"
	Metrics Signal = "METRICS"
)

const prefix = "OTEL_EXPORTER_OTLP_"

// transportSuffixes are the variables that decide where and how an exporter
// connects. Headers, compression, and timeouts are deliberately not among them:
// they shape a connection without implying one is configured.
var transportSuffixes = []string{"ENDPOINT", "INSECURE", "CERTIFICATE", "CLIENT_CERTIFICATE", "CLIENT_KEY"}

// Configured reports whether any endpoint or TLS variable is set for either
// signal. When it is false the SDK would dial its default of localhost:4317
// over TLS, which the collectors developers run beside a `go run` do not speak;
// callers keep plaintext for that case only.
func Configured() bool {
	for _, suffix := range transportSuffixes {
		for _, signal := range []Signal{"", Traces, Metrics} {
			if strings.TrimSpace(os.Getenv(name(signal, suffix))) != "" {
				return true
			}
		}
	}
	return false
}

// Transport describes the connection an exporter or the relay makes to the
// collector, as resolved from the environment for one signal.
type Transport struct {
	// Endpoint is the raw endpoint variable value, "" when unset.
	Endpoint string
	// Insecure is true when the connection is plaintext.
	Insecure bool
	// Reason names the rule that decided Insecure, for the startup log line.
	Reason string
	// Warning is set when the variables describe a connection that cannot
	// work, so the startup log can say so instead of leaving it to per-export
	// errors.
	Warning string
	// CAFile is the PEM bundle the collector's certificate must chain to, or ""
	// for the system roots.
	CAFile string
	// ClientCertFile and ClientKeyFile are the client certificate pair for mTLS,
	// or "" when the connection presents none.
	ClientCertFile string
	ClientKeyFile  string
}

// Resolve reads the transport variables for signal.
func Resolve(signal Signal) Transport {
	if !Configured() {
		return Transport{
			Insecure: true,
			Reason:   "no " + prefix + "* variable is set, keeping plaintext to the SDK default localhost:4317",
		}
	}

	var t Transport
	var endpointVar string
	t.Endpoint, endpointVar = lookup(signal, "ENDPOINT")
	switch scheme := schemeOf(t.Endpoint); {
	case t.Endpoint == "":
		t.Reason = "no endpoint variable is set, so the SDK default localhost:4317 is dialed with TLS"
	case scheme == "http" || scheme == "unix":
		t.Insecure = true
		t.Reason = endpointVar + " has an " + scheme + " scheme"
	case scheme == "https":
		t.Reason = endpointVar + " has an https scheme"
	default:
		// url.Parse reads "collector:4317" as scheme "collector" with an empty
		// host, so the SDK exporters dial an empty address over TLS and every
		// export fails. The relay parses host:port itself and dials it with
		// TLS, so the two at least agree on the transport.
		t.Reason = endpointVar + " has no http or https scheme, which the SDK treats as TLS"
		t.Warning = endpointVar + "=" + t.Endpoint + " has no scheme: the OpenTelemetry SDK reads it as an empty host and exports will fail; use http:// for plaintext or https:// for TLS"
	}

	if v, insecureVar := lookup(signal, "INSECURE"); v != "" {
		// The SDK ignores a value it cannot parse (it logs and moves on), so an
		// unparseable override leaves the scheme's decision standing here too.
		if b, err := strconv.ParseBool(v); err == nil {
			t.Insecure = b
			t.Reason = insecureVar + "=" + v
		}
	}

	var caVar, certVar string
	t.CAFile, caVar = lookup(signal, "CERTIFICATE")
	t.ClientCertFile, t.ClientKeyFile, certVar = clientPair(signal)
	// The SDK builds TLS credentials as soon as it has a CA bundle or a client
	// certificate, and those credentials take precedence over its insecure flag.
	// A certificate variable therefore means TLS whatever the scheme or the
	// INSECURE variable said.
	if forced := firstNonEmpty(caVar, certVar); forced != "" {
		t.Insecure = false
		t.Reason = forced + " is set, which makes the SDK use TLS regardless of the scheme and " + prefix + "INSECURE"
	}
	if !t.Insecure && t.Warning == "" && strings.Contains(t.Endpoint, gkeManagedCollectorHost) {
		t.Warning = "TLS is configured toward the GKE managed OpenTelemetry collector, whose receiver is plaintext only; the handshake will fail and nothing will be exported. Use an http:// endpoint for it, or point at a collector that terminates TLS"
	}
	return t
}

// gkeManagedCollectorHost is the service the GKE managed OpenTelemetry addon
// exposes. Its OTLP receiver has no TLS listener, so TLS toward it can never
// succeed; naming the case at startup beats a stream of handshake errors.
const gkeManagedCollectorHost = ".gke-managed-otel.svc"

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// TLSConfig builds the TLS configuration for t, loading the files it names.
// It returns nil for a plaintext transport.
func (t Transport) TLSConfig() (*tls.Config, error) {
	if t.Insecure {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if t.CAFile != "" {
		pem, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read OTLP collector CA bundle: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("OTLP collector CA bundle %q holds no PEM certificate", t.CAFile)
		}
		cfg.RootCAs = pool
	}
	if t.ClientCertFile != "" {
		cert, err := tls.LoadX509KeyPair(t.ClientCertFile, t.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load OTLP client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// Credentials returns the gRPC transport credentials for t.
func (t Transport) Credentials() (credentials.TransportCredentials, error) {
	if t.Insecure {
		return insecure.NewCredentials(), nil
	}
	cfg, err := t.TLSConfig()
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(cfg), nil
}

// LogValue renders t as a log group. File paths are not secrets; their
// contents are never logged.
func (t Transport) LogValue() slog.Value {
	attrs := []slog.Attr{
		slog.Bool("insecure", t.Insecure),
		slog.String("reason", t.Reason),
	}
	if t.Endpoint != "" {
		attrs = append(attrs, slog.String("endpoint", t.Endpoint))
	}
	if t.CAFile != "" {
		attrs = append(attrs, slog.String("ca_file", t.CAFile))
	}
	if t.ClientCertFile != "" {
		attrs = append(attrs, slog.String("client_cert_file", t.ClientCertFile))
	}
	if t.Warning != "" {
		attrs = append(attrs, slog.String("warning", t.Warning))
	}
	return slog.GroupValue(attrs...)
}

func name(signal Signal, suffix string) string {
	if signal == "" {
		return prefix + suffix
	}
	return prefix + string(signal) + "_" + suffix
}

// lookup returns the trimmed value of the signal-specific variable when set,
// else the generic one, along with the name of the variable that supplied it.
func lookup(signal Signal, suffix string) (value, from string) {
	for _, s := range []Signal{signal, ""} {
		n := name(s, suffix)
		if v := strings.TrimSpace(os.Getenv(n)); v != "" {
			return v, n
		}
	}
	return "", ""
}

// clientPair mirrors the SDK, which only accepts a certificate and key that
// come from the same tier: a signal-specific certificate with only a generic
// key is not a pair. It also returns the name of the certificate variable the
// pair came from.
func clientPair(signal Signal) (certFile, keyFile, from string) {
	for _, s := range []Signal{signal, ""} {
		certVar := name(s, "CLIENT_CERTIFICATE")
		cert := strings.TrimSpace(os.Getenv(certVar))
		key := strings.TrimSpace(os.Getenv(name(s, "CLIENT_KEY")))
		if cert != "" && key != "" {
			return cert, key, certVar
		}
	}
	return "", "", ""
}

// schemeOf returns the lowercased URL scheme of endpoint, or "" when it has
// none. The SDK's url.Parse would read "collector:4317" as scheme "collector",
// which lands in the same TLS branch as no scheme, so the two are not
// distinguished here.
func schemeOf(endpoint string) string {
	if !strings.Contains(endpoint, "://") {
		return ""
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Scheme)
}
