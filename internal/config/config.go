package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Profile struct {
	Name    string   `toml:"-" mapstructure:"-"` // Profile name (from map key)
	Brokers []string `toml:"brokers" mapstructure:"brokers"`
}

type configFile struct {
	profiles map[string]Profile `toml:"profiles" mapstructure:"profiles"`
}

func ReadProfile(profile string) ([]kgo.Opt, error) {
	configFile, err := readConfigFile() // Load ~/.frogo/config.toml
	if err != nil {
		panic(err)
	}

	configProfile, exists := configFile.profiles[profile] // Access profile
	if !exists {
		return nil, fmt.Errorf("profile %v not found. Config file: %v", profile, configFile)
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(configProfile.Brokers...),
	}

	return opts, nil
}

// WriteProfileConfig loads the current config, updates a single profile, and writes it back
func WriteProfile(p Profile) error {
	// Load existing config or create new one
	cfg, err := readConfigFile()
	if err != nil {
		return err
	}

	// Ensure Profiles map is initialized
	if cfg.profiles == nil {
		cfg.profiles = make(map[string]Profile)
	}

	// Set the profile name and add/update it
	cfg.profiles[p.Name] = p

	// Write config using Viper
	return writeConfigFile(*cfg)
}

// Get home dir for config files (~/.frogo/ by default)
func Dir() (string, error) {
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

func Path() (string, error) {
	configDir, err := Dir()
	if err != nil {
		return "", err
	}

	return filepath.Join(configDir, "config.toml"), nil
}

// Loads configuration profiles from the config file using Viper
// If the config file doesn't exist, returns an empty config (not an error)
func readConfigFile() (*configFile, error) {
	configDir, err := Dir()
	if err != nil {
		return nil, err
	}

	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("toml")
	v.AddConfigPath(configDir)

	// TODO: Make passing in arbitrary kgo options easier
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			return &configFile{profiles: make(map[string]Profile)}, nil
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config configFile
	v.Unmarshal(&config)
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	for name, profile := range config.profiles {
		profile.Name = name
		config.profiles[name] = profile
	}

	return &config, nil
}

// Write all profiles to config file
func writeConfigFile(cfg configFile) error {
	if err := ensureConfigDir(); err != nil {
		return err
	}

	configDir, err := Dir()
	if err != nil {
		return fmt.Errorf("failed to get configuration directory: %w", err)
	}

	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("toml")
	v.AddConfigPath(configDir)
	v.Set("profiles", cfg.profiles)

	configPath, err := Path()
	if err != nil {
		return err
	}

	return v.WriteConfigAs(configPath)
}

// Create the ~/.frogo directory if it doesn't exist
func ensureConfigDir() error {
	configDir, err := Dir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	return nil
}
