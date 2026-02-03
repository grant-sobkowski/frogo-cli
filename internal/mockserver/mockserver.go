package mockserver

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/twmb/franz-go/pkg/kfake"
)

type Cluster struct {
	cluster *kfake.Cluster
}

type Config struct {
	NumBrokers int // Number of brokers (defaults to 1 if not set)
}

type mockServerState struct {
	PID   int      `json:"pid"`
	Addrs []string `json:"addrs"`
}

type ConfigError struct {
	Field   string
	Message string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("config error on field %s: %s", e.Field, e.Message)
}

func Start(cfg *Config) (*Cluster, error) {
	state, err := loadState()
	if err != nil {
		return nil, fmt.Errorf("failed to check existing server state: %w", err)
	}
	if state != nil && isProcessRunning(state.PID) {
		return nil, fmt.Errorf("mock server is already running (PID: %d, addresses: %v)", state.PID, state.Addrs)
	}

	configErr := cfg.validate()
	if configErr != nil {
		return nil, configErr
	}

	cluster, err := kfake.NewCluster(kfake.NumBrokers(cfg.NumBrokers))
	if err != nil {
		return nil, fmt.Errorf("failed to create mock server: %w", err)
	}

	m := &Cluster{cluster: cluster}

	serverState := &mockServerState{
		PID:   os.Getpid(),
		Addrs: m.cluster.ListenAddrs(),
	}

	err = saveState(serverState)
	if err != nil {
		return nil, fmt.Errorf("failed to save server state: %w", err)
	}

	return m, nil
}

func (cfg *Config) validate() *ConfigError {
	if cfg.NumBrokers == 0 {
		return &ConfigError{
			Field:   "NumBrokers",
			Message: "at least 1 broker is required.",
		}
	}

	return nil
}

// Shutdown mock server
func (m *Cluster) Stop() {
	if m.cluster != nil {
		m.cluster.Close()
	}
	err := removeState()
	if err != nil {
		fmt.Printf("WARNING: error removing statefile %s", err)
	}
}

// Addrs returns the broker addresses of the mock server
func (m *Cluster) Addrs() []string {
	return m.cluster.ListenAddrs()
}

//  ─────────────────────────── STATEFILE LOGIC ───────────────────────────

func loadState() (*mockServerState, error) {
	stateFile, err := stateFilePath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No state file means no running server
		}
		return nil, fmt.Errorf("failed to read state file: %w", err)
	}

	var state mockServerState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to parse state file: %w", err)
	}

	return &state, nil
}

func saveState(state *mockServerState) error {
	stateFile, err := stateFilePath()
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	if err := os.WriteFile(stateFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	return nil
}

func removeState() error {
	stateFile, err := stateFilePath()
	if err != nil {
		return err
	}

	if err := os.Remove(stateFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove state file: %w", err)
	}

	return nil
}

func stateFilePath() (string, error) {
	configDir, err := stateFileDir()
	if err != nil {
		return "", err
	}
	stateDir := filepath.Join(configDir, ".frogo")
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create state directory: %w", err)
	}

	return filepath.Join(stateDir, "mockserver.json"), nil
}

func stateFileDir() (string, error) {
	optDir := os.Getenv("FROGO_CONFIG_DIR")
	if optDir != "" {
		return optDir, nil
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get home directory: %w", err)
	}
	configDir := filepath.Join(homeDir, ".frogo")
	return configDir, nil
}

// Check if PID exists
func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	// Send signal 0 to check if process exists without actually sending a signal
	err = process.Signal(syscall.Signal(0))
	return err == nil
}
