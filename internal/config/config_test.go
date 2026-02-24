package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Test reading profiles from a config file
func TestReadProfile(t *testing.T) {
	// Create a temp dir for this test and set it as our config dir
	tempDir := t.TempDir()
	os.Setenv("FROGO_CONFIG_DIR", tempDir)

	configPath, err := Path()
	if err != nil {
		t.Fatalf("failed to get config directory: %v", err)
	}

	configContent :=
		`[profiles.default]
brokers = ["localhost:9092"]

[profiles.production]
brokers = ["prod1.kafka.com:9092", "prod2.kafka.com:9092"]
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	opts, err := ReadProfile("default") // Read default profile
	if err != nil {
		t.Fatalf("failed to get default profile: %v", err)
	}

	fmt.Printf("Value of readprofile: %v", opts)
	// TODO: Test ops for default profile set correctly
	// TODO: Test ops for production profile set correctly
}

func TestReadConfigFile_Missing(t *testing.T) {
	tempDir := t.TempDir()
	os.Setenv("FROGO_CONFIG_DIR", tempDir)

	// Should return empty config, not error
	config, err := readConfigFile()
	if err != nil {
		t.Fatalf("expected no error for missing config, got: %v", err)
	}

	if len(config.Profiles) != 0 {
		t.Errorf("expected empty config, got %d profiles", len(config.Profiles))
	}
}

func TestBrokerOpts(t *testing.T) {
	p := Profile{Brokers: []string{"broker1:9092", "broker2:9092"}}
	opts, err := p.brokerOpts()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(opts) != 1 {
		t.Fatalf("expected 1 opt, got %d", len(opts))
	}
}

func TestSecurityOpts(t *testing.T) {
	tests := []struct {
		name     string
		profile  Profile
		wantOpts int
		wantErr  bool
	}{
		{
			name:     "empty protocol",
			profile:  Profile{},
			wantOpts: 0,
		},
		{
			name:     "plaintext",
			profile:  Profile{SecurityProtocol: "plaintext"},
			wantOpts: 0,
		},
		{
			name:     "ssl",
			profile:  Profile{SecurityProtocol: "ssl"},
			wantOpts: 1,
		},
		{
			name: "sasl_plaintext with PLAIN",
			profile: Profile{
				SecurityProtocol: "sasl_plaintext",
				SASLMechanism:    "PLAIN",
				SASLUsername:     "user",
				SASLPassword:     "pass",
			},
			wantOpts: 1,
		},
		{
			name: "sasl_ssl with SCRAM-SHA-256",
			profile: Profile{
				SecurityProtocol: "sasl_ssl",
				SASLMechanism:    "SCRAM-SHA-256",
				SASLUsername:     "user",
				SASLPassword:     "pass",
			},
			wantOpts: 2,
		},
		{
			name: "sasl_ssl with SCRAM-SHA-512",
			profile: Profile{
				SecurityProtocol: "sasl_ssl",
				SASLMechanism:    "SCRAM-SHA-512",
				SASLUsername:     "user",
				SASLPassword:     "pass",
			},
			wantOpts: 2,
		},
		{
			name: "sasl_ssl missing username",
			profile: Profile{
				SecurityProtocol: "sasl_ssl",
				SASLMechanism:    "PLAIN",
				SASLPassword:     "pass",
			},
			wantErr: true,
		},
		{
			name: "sasl_ssl missing password",
			profile: Profile{
				SecurityProtocol: "sasl_ssl",
				SASLMechanism:    "PLAIN",
				SASLUsername:     "user",
			},
			wantErr: true,
		},
		{
			name: "sasl_ssl unknown mechanism",
			profile: Profile{
				SecurityProtocol: "sasl_ssl",
				SASLMechanism:    "OAUTHBEARER",
				SASLUsername:     "user",
				SASLPassword:     "pass",
			},
			wantErr: true,
		},
		{
			name: "sasl_ssl with AWS_MSK_IAM",
			profile: Profile{
				SecurityProtocol: "sasl_ssl",
				SASLMechanism:    "AWS_MSK_IAM",
			},
			wantOpts: 2,
		},
		{
			name:    "bogus protocol",
			profile: Profile{SecurityProtocol: "bogus"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := tt.profile.securityOpts()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(opts) != tt.wantOpts {
				t.Fatalf("expected %d opts, got %d", tt.wantOpts, len(opts))
			}
		})
	}
}

func TestMessageSizeOpts(t *testing.T) {
	tests := []struct {
		name     string
		profile  Profile
		wantOpts int
	}{
		{
			name:     "both zero",
			profile:  Profile{},
			wantOpts: 0,
		},
		{
			name:     "message_max_bytes only",
			profile:  Profile{MessageMaxBytes: 1048576},
			wantOpts: 1,
		},
		{
			name:     "receive_message_max_bytes only",
			profile:  Profile{ReceiveMessageMaxBytes: 104857600},
			wantOpts: 1,
		},
		{
			name: "both set",
			profile: Profile{
				MessageMaxBytes:        1048576,
				ReceiveMessageMaxBytes: 104857600,
			},
			wantOpts: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := tt.profile.messageSizeOpts()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(opts) != tt.wantOpts {
				t.Fatalf("expected %d opts, got %d", tt.wantOpts, len(opts))
			}
		})
	}
}

// generateSelfSignedCert creates a self-signed cert and key PEM files in dir,
// returning their paths.
func generateSelfSignedCert(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	certPath = filepath.Join(dir, "cert.pem")
	certFile, _ := os.Create(certPath)
	pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	certFile.Close()

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("failed to marshal key: %v", err)
	}

	keyPath = filepath.Join(dir, "key.pem")
	keyFile, _ := os.Create(keyPath)
	pem.Encode(keyFile, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	keyFile.Close()

	return certPath, keyPath
}

func TestBuildTLSConfig(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateSelfSignedCert(t, dir)

	// Write an invalid PEM file
	invalidPEMPath := filepath.Join(dir, "invalid.pem")
	os.WriteFile(invalidPEMPath, []byte("not a PEM file"), 0644)

	tests := []struct {
		name    string
		profile Profile
		wantErr bool
		check   func(t *testing.T, p Profile)
	}{
		{
			name:    "all empty",
			profile: Profile{},
			check: func(t *testing.T, p Profile) {
				cfg, _ := p.buildTLSConfig()
				if cfg.InsecureSkipVerify {
					t.Error("expected InsecureSkipVerify false")
				}
				if cfg.RootCAs != nil {
					t.Error("expected nil RootCAs")
				}
				if len(cfg.Certificates) != 0 {
					t.Error("expected no Certificates")
				}
			},
		},
		{
			name:    "skip verify",
			profile: Profile{TLSSkipVerify: true},
			check: func(t *testing.T, p Profile) {
				cfg, _ := p.buildTLSConfig()
				if !cfg.InsecureSkipVerify {
					t.Error("expected InsecureSkipVerify true")
				}
			},
		},
		{
			name:    "valid CA cert",
			profile: Profile{TLSCACertFile: certPath},
			check: func(t *testing.T, p Profile) {
				cfg, _ := p.buildTLSConfig()
				if cfg.RootCAs == nil {
					t.Error("expected non-nil RootCAs")
				}
			},
		},
		{
			name:    "nonexistent CA cert file",
			profile: Profile{TLSCACertFile: "/nonexistent/ca.pem"},
			wantErr: true,
		},
		{
			name:    "invalid PEM CA cert",
			profile: Profile{TLSCACertFile: invalidPEMPath},
			wantErr: true,
		},
		{
			name: "valid client cert and key",
			profile: Profile{
				TLSClientCertFile: certPath,
				TLSClientKeyFile:  keyPath,
			},
			check: func(t *testing.T, p Profile) {
				cfg, _ := p.buildTLSConfig()
				if len(cfg.Certificates) != 1 {
					t.Errorf("expected 1 certificate, got %d", len(cfg.Certificates))
				}
			},
		},
		{
			name:    "client cert without key",
			profile: Profile{TLSClientCertFile: certPath},
			wantErr: true,
		},
		{
			name:    "client key without cert",
			profile: Profile{TLSClientKeyFile: keyPath},
			wantErr: true,
		},
		{
			name: "bad client cert files",
			profile: Profile{
				TLSClientCertFile: invalidPEMPath,
				TLSClientKeyFile:  invalidPEMPath,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := tt.profile.buildTLSConfig()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg == nil {
				t.Fatal("expected non-nil config")
			}
			if tt.check != nil {
				tt.check(t, tt.profile)
			}
		})
	}
}
