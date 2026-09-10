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

package otlpenv

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// clearEnv unsets every variable this package reads so a test starts from the
// unconfigured state regardless of the developer's shell.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, suffix := range transportSuffixes {
		for _, signal := range []Signal{"", Traces, Metrics} {
			t.Setenv(name(signal, suffix), "")
		}
	}
}

func TestConfigured(t *testing.T) {
	clearEnv(t)
	if Configured() {
		t.Fatal("Configured() = true with no variable set")
	}
	for _, n := range []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"OTEL_EXPORTER_OTLP_INSECURE",
		"OTEL_EXPORTER_OTLP_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_METRICS_CLIENT_KEY",
	} {
		t.Run(n, func(t *testing.T) {
			clearEnv(t)
			t.Setenv(n, "x")
			if !Configured() {
				t.Errorf("Configured() = false with %s set", n)
			}
		})
	}
	t.Run("headers alone do not configure a transport", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-api-key=secret")
		if Configured() {
			t.Error("Configured() = true with only OTEL_EXPORTER_OTLP_HEADERS set")
		}
	})
	t.Run("whitespace is unset", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "   ")
		if Configured() {
			t.Error("Configured() = true with a blank endpoint")
		}
	})
}

func TestResolve(t *testing.T) {
	for _, tc := range []struct {
		name         string
		env          map[string]string
		signal       Signal
		wantInsecure bool
		wantReason   string
		wantEndpoint string
		wantWarning  string
	}{
		{
			name:         "nothing set keeps plaintext",
			wantInsecure: true,
			wantReason:   "no OTEL_EXPORTER_OTLP_* variable is set",
		},
		{
			name:         "http scheme is plaintext",
			env:          map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4317"},
			wantInsecure: true,
			wantReason:   "OTEL_EXPORTER_OTLP_ENDPOINT has an http scheme",
			wantEndpoint: "http://collector:4317",
		},
		{
			name:         "https scheme is TLS",
			env:          map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "https://collector:4317"},
			wantInsecure: false,
			wantReason:   "https scheme",
			wantEndpoint: "https://collector:4317",
		},
		{
			name:         "unix scheme is plaintext",
			env:          map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "unix:///run/otlp.sock"},
			wantInsecure: true,
			wantReason:   "unix scheme",
		},
		{
			name:         "no scheme is TLS, as in the SDK",
			env:          map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "collector:4317"},
			wantInsecure: false,
			wantReason:   "no http or https scheme",
		},
		{
			name: "INSECURE=true overrides no scheme",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT": "collector:4317",
				"OTEL_EXPORTER_OTLP_INSECURE": "true",
			},
			wantInsecure: true,
			wantReason:   "OTEL_EXPORTER_OTLP_INSECURE=true",
		},
		{
			name: "INSECURE=false overrides http scheme",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4317",
				"OTEL_EXPORTER_OTLP_INSECURE": "false",
			},
			wantInsecure: false,
			wantReason:   "OTEL_EXPORTER_OTLP_INSECURE=false",
		},
		{
			name: "unparseable INSECURE leaves the scheme's decision",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4317",
				"OTEL_EXPORTER_OTLP_INSECURE": "yes please",
			},
			wantInsecure: true,
			wantReason:   "http scheme",
		},
		{
			name: "signal endpoint wins over generic",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT":        "http://generic:4317",
				"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "https://traces:4317",
			},
			signal:       Traces,
			wantInsecure: false,
			wantReason:   "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT has an https scheme",
			wantEndpoint: "https://traces:4317",
		},
		{
			name: "other signal's endpoint is ignored",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT":        "http://generic:4317",
				"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "https://traces:4317",
			},
			signal:       Metrics,
			wantInsecure: true,
			wantReason:   "OTEL_EXPORTER_OTLP_ENDPOINT has an http scheme",
			wantEndpoint: "http://generic:4317",
		},
		{
			name: "signal INSECURE wins over generic",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT":         "https://collector:4317",
				"OTEL_EXPORTER_OTLP_INSECURE":         "false",
				"OTEL_EXPORTER_OTLP_METRICS_INSECURE": "true",
			},
			signal:       Metrics,
			wantInsecure: true,
			wantReason:   "OTEL_EXPORTER_OTLP_METRICS_INSECURE=true",
		},
		{
			name:         "only a certificate set still dials the SDK default with TLS",
			env:          map[string]string{"OTEL_EXPORTER_OTLP_CERTIFICATE": "/etc/ca.pem"},
			wantInsecure: false,
			wantReason:   "OTEL_EXPORTER_OTLP_CERTIFICATE is set",
		},
		{
			name: "certificate forces TLS over INSECURE=true",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT":    "https://collector:4317",
				"OTEL_EXPORTER_OTLP_INSECURE":    "true",
				"OTEL_EXPORTER_OTLP_CERTIFICATE": "/etc/ca.pem",
			},
			wantInsecure: false,
			wantReason:   "OTEL_EXPORTER_OTLP_CERTIFICATE is set, which makes the SDK use TLS",
		},
		{
			name: "certificate forces TLS over an http scheme",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT":    "http://collector:4317",
				"OTEL_EXPORTER_OTLP_CERTIFICATE": "/etc/ca.pem",
			},
			wantInsecure: false,
			wantReason:   "OTEL_EXPORTER_OTLP_CERTIFICATE is set",
		},
		{
			name: "client certificate pair forces TLS over an http scheme",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT":                   "http://collector:4317",
				"OTEL_EXPORTER_OTLP_METRICS_CLIENT_CERTIFICATE": "/etc/client.pem",
				"OTEL_EXPORTER_OTLP_METRICS_CLIENT_KEY":         "/etc/client.key",
			},
			signal:       Metrics,
			wantInsecure: false,
			wantReason:   "OTEL_EXPORTER_OTLP_METRICS_CLIENT_CERTIFICATE is set",
		},
		{
			name:         "TLS toward the GKE managed collector warns",
			env:          map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "https://opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317"},
			wantInsecure: false,
			wantReason:   "https scheme",
			wantWarning:  "plaintext only",
		},
		{
			name:         "plaintext toward the GKE managed collector does not warn",
			env:          map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317"},
			wantInsecure: true,
			wantReason:   "http scheme",
		},
		{
			name: "half a client pair does not force TLS",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT":           "http://collector:4317",
				"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE": "/etc/client.pem",
			},
			wantInsecure: true,
			wantReason:   "http scheme",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			signal := tc.signal
			if signal == "" {
				signal = Traces
			}
			got := Resolve(signal)
			wantWarning := tc.wantWarning
			if strings.Contains(tc.name, "no scheme") {
				wantWarning = "has no scheme"
			}
			if (got.Warning != "") != (wantWarning != "") || !strings.Contains(got.Warning, wantWarning) {
				t.Errorf("Resolve(%s).Warning = %q, want %q", signal, got.Warning, wantWarning)
			}
			if got.Insecure != tc.wantInsecure {
				t.Errorf("Resolve(%s).Insecure = %v, want %v (reason %q)", signal, got.Insecure, tc.wantInsecure, got.Reason)
			}
			if !strings.Contains(got.Reason, tc.wantReason) {
				t.Errorf("Resolve(%s).Reason = %q, want it to mention %q", signal, got.Reason, tc.wantReason)
			}
			if tc.wantEndpoint != "" && got.Endpoint != tc.wantEndpoint {
				t.Errorf("Resolve(%s).Endpoint = %q, want %q", signal, got.Endpoint, tc.wantEndpoint)
			}
		})
	}
}

func TestResolveCertificateFiles(t *testing.T) {
	clearEnv(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://collector:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", "/generic/ca.pem")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE", "/traces/ca.pem")
	t.Setenv("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", "/generic/client.pem")
	t.Setenv("OTEL_EXPORTER_OTLP_CLIENT_KEY", "/generic/client.key")
	// A signal-specific certificate without its key is not a pair, so the
	// generic pair applies to traces.
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE", "/traces/client.pem")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_CLIENT_CERTIFICATE", "/metrics/client.pem")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_CLIENT_KEY", "/metrics/client.key")

	traces := Resolve(Traces)
	if traces.CAFile != "/traces/ca.pem" {
		t.Errorf("traces CAFile = %q, want the signal-specific bundle", traces.CAFile)
	}
	if traces.ClientCertFile != "/generic/client.pem" || traces.ClientKeyFile != "/generic/client.key" {
		t.Errorf("traces client pair = (%q, %q), want the generic pair", traces.ClientCertFile, traces.ClientKeyFile)
	}

	metrics := Resolve(Metrics)
	if metrics.CAFile != "/generic/ca.pem" {
		t.Errorf("metrics CAFile = %q, want the generic bundle", metrics.CAFile)
	}
	if metrics.ClientCertFile != "/metrics/client.pem" || metrics.ClientKeyFile != "/metrics/client.key" {
		t.Errorf("metrics client pair = (%q, %q), want the signal-specific pair", metrics.ClientCertFile, metrics.ClientKeyFile)
	}
}

func TestCredentials(t *testing.T) {
	ca, clientCert, clientKey := writeTestPKI(t)

	t.Run("plaintext", func(t *testing.T) {
		creds, err := (Transport{Insecure: true}).Credentials()
		if err != nil {
			t.Fatalf("Credentials(): %v", err)
		}
		if got := creds.Info().SecurityProtocol; got != "insecure" {
			t.Errorf("SecurityProtocol = %q, want insecure", got)
		}
		if cfg, err := (Transport{Insecure: true}).TLSConfig(); err != nil || cfg != nil {
			t.Errorf("TLSConfig() = (%v, %v), want (nil, nil) for plaintext", cfg, err)
		}
	})

	t.Run("system roots", func(t *testing.T) {
		tr := Transport{}
		creds, err := tr.Credentials()
		if err != nil {
			t.Fatalf("Credentials(): %v", err)
		}
		if got := creds.Info().SecurityProtocol; got != "tls" {
			t.Errorf("SecurityProtocol = %q, want tls", got)
		}
		cfg, err := tr.TLSConfig()
		if err != nil {
			t.Fatalf("TLSConfig(): %v", err)
		}
		if cfg.RootCAs != nil || len(cfg.Certificates) != 0 {
			t.Error("TLSConfig() must leave RootCAs nil (system roots) and present no client certificate by default")
		}
	})

	t.Run("custom CA and client certificate", func(t *testing.T) {
		cfg, err := (Transport{CAFile: ca, ClientCertFile: clientCert, ClientKeyFile: clientKey}).TLSConfig()
		if err != nil {
			t.Fatalf("TLSConfig(): %v", err)
		}
		if cfg.RootCAs == nil {
			t.Error("TLSConfig().RootCAs = nil, want the bundle from CAFile")
		}
		if len(cfg.Certificates) != 1 {
			t.Errorf("len(TLSConfig().Certificates) = %d, want 1", len(cfg.Certificates))
		}
	})

	t.Run("missing CA file", func(t *testing.T) {
		if _, err := (Transport{CAFile: filepath.Join(t.TempDir(), "absent.pem")}).Credentials(); err == nil {
			t.Error("Credentials() = nil error, want one for an absent CA file")
		}
	})

	t.Run("CA file without a certificate", func(t *testing.T) {
		empty := filepath.Join(t.TempDir(), "empty.pem")
		if err := os.WriteFile(empty, []byte("not pem"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := (Transport{CAFile: empty}).TLSConfig()
		if err == nil || !strings.Contains(err.Error(), "no PEM certificate") {
			t.Errorf("TLSConfig() error = %v, want it to say the bundle holds no certificate", err)
		}
	})

	t.Run("client certificate without key", func(t *testing.T) {
		if _, err := (Transport{ClientCertFile: clientCert, ClientKeyFile: filepath.Join(t.TempDir(), "absent.key")}).TLSConfig(); err == nil {
			t.Error("TLSConfig() = nil error, want one for an absent client key")
		}
	})
}

func TestLogValueNamesNoSecrets(t *testing.T) {
	tr := Transport{
		Endpoint:       "https://collector:4317",
		Reason:         "OTEL_EXPORTER_OTLP_ENDPOINT has an https scheme",
		CAFile:         "/etc/otlp/ca.pem",
		ClientCertFile: "/etc/otlp/client.pem",
		ClientKeyFile:  "/etc/otlp/client.key",
	}
	var keys []string
	for _, a := range tr.LogValue().Group() {
		keys = append(keys, a.Key)
	}
	got := strings.Join(keys, ",")
	for _, want := range []string{"insecure", "reason", "endpoint", "ca_file", "client_cert_file"} {
		if !strings.Contains(got, want) {
			t.Errorf("LogValue() keys = %s, want %s among them", got, want)
		}
	}
	if strings.Contains(got, "key") {
		t.Errorf("LogValue() keys = %s, must not name the client key", got)
	}
}

// writeTestPKI writes a self-signed CA and a client certificate it issued into
// a temp dir and returns their paths.
func writeTestPKI(t *testing.T) (caFile, certFile, keyFile string) {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caFile = filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		t.Fatal(err)
	}

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTmpl, caTmpl, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	certFile = filepath.Join(dir, "client.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}
	keyFile = filepath.Join(dir, "client.key")
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return caFile, certFile, keyFile
}
