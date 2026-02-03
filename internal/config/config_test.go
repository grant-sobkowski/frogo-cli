package config

import (
	"fmt"
	"os"
	"testing"
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

	if len(config.profiles) != 0 {
		t.Errorf("expected empty config, got %d profiles", len(config.profiles))
	}
}
