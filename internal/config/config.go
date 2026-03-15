package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"

	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/viper"
	"github.com/twmb/franz-go/pkg/kgo"
	awsSasl "github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Profile struct {
	Name                   string   `toml:"-" mapstructure:"-"` // Profile name (from map key)
	Brokers                []string `toml:"brokers" mapstructure:"brokers"`
	SecurityProtocol       string   `toml:"security_protocol" mapstructure:"security_protocol"`
	SASLMechanism          string   `toml:"sasl_mechanism" mapstructure:"sasl_mechanism"`
	SASLUsername           string   `toml:"sasl_username" mapstructure:"sasl_username"`
	SASLPassword           string   `toml:"sasl_password" mapstructure:"sasl_password"`
	MessageMaxBytes        int32    `toml:"message_max_bytes" mapstructure:"message_max_bytes"`
	ReceiveMessageMaxBytes int32    `toml:"receive_message_max_bytes" mapstructure:"receive_message_max_bytes"`
	TLSCACertFile          string   `toml:"tls_ca_cert_file" mapstructure:"tls_ca_cert_file"`
	TLSClientCertFile      string   `toml:"tls_client_cert_file" mapstructure:"tls_client_cert_file"`
	TLSClientKeyFile       string   `toml:"tls_client_key_file" mapstructure:"tls_client_key_file"`
	TLSSkipVerify          bool     `toml:"tls_skip_verify" mapstructure:"tls_skip_verify"`
}

type configFile struct {
	Profiles map[string]Profile `toml:"profiles" mapstructure:"profiles"`
}

// Each method on Profile maps a group of config fields to kgo options.
var optBuilders = []func(Profile) ([]kgo.Opt, error){
	Profile.brokerOpts,
	Profile.securityOpts,
	Profile.messageSizeOpts,
}

// ReadProfile gets profile from config file, then parses
// it into config options supported by kgo clients.
func ReadProfile(profile string) ([]kgo.Opt, error) {
	configFile, err := readConfigFile() // Load ~/.frogo/config.toml
	if err != nil {
		panic(err)
	}

	configProfile, exists := configFile.Profiles[profile] // Access profile
	if !exists {
		return nil, fmt.Errorf("profile %v not found. Config file: %v", profile, configFile)
	}

	var opts []kgo.Opt
	for _, build := range optBuilders {
		o, err := build(configProfile)
		if err != nil {
			return nil, err
		}
		opts = append(opts, o...)
	}

	return opts, nil
}

func (p Profile) brokerOpts() ([]kgo.Opt, error) {
	return []kgo.Opt{kgo.SeedBrokers(p.Brokers...)}, nil
}

func (p Profile) securityOpts() ([]kgo.Opt, error) {
	switch p.SecurityProtocol {
	case "", "plaintext":
		return nil, nil
	case "ssl":
		tlsCfg, err := p.buildTLSConfig()
		if err != nil {
			return nil, err
		}
		return []kgo.Opt{kgo.DialTLSConfig(tlsCfg)}, nil
	case "sasl_plaintext":
		saslOpt, err := p.saslOpt()
		if err != nil {
			return nil, err
		}
		return []kgo.Opt{saslOpt}, nil
	case "sasl_ssl":
		tlsCfg, err := p.buildTLSConfig()
		if err != nil {
			return nil, err
		}
		saslOpt, err := p.saslOpt()
		if err != nil {
			return nil, err
		}
		return []kgo.Opt{kgo.DialTLSConfig(tlsCfg), saslOpt}, nil
	default:
		return nil, fmt.Errorf("unsupported security_protocol: %q", p.SecurityProtocol)
	}
}

func (p Profile) buildTLSConfig() (*tls.Config, error) {
	cfg := &tls.Config{}

	if p.TLSSkipVerify {
		cfg.InsecureSkipVerify = true
	}

	if p.TLSCACertFile != "" {
		pemData, err := os.ReadFile(p.TLSCACertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read tls_ca_cert_file %q: %w", p.TLSCACertFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemData) {
			return nil, fmt.Errorf("tls_ca_cert_file %q contains no valid PEM certificates", p.TLSCACertFile)
		}
		cfg.RootCAs = pool
	}

	if p.TLSClientCertFile != "" || p.TLSClientKeyFile != "" {
		if p.TLSClientCertFile == "" {
			return nil, fmt.Errorf("tls_client_cert_file is required when tls_client_key_file is set")
		}
		if p.TLSClientKeyFile == "" {
			return nil, fmt.Errorf("tls_client_key_file is required when tls_client_cert_file is set")
		}
		cert, err := tls.LoadX509KeyPair(p.TLSClientCertFile, p.TLSClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client cert/key pair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}

func (p Profile) saslOpt() (kgo.Opt, error) {
	if p.SASLMechanism == "AWS_MSK_IAM" {
		return kgo.SASL(
			awsSasl.ManagedStreamingIAM(func(ctx context.Context) (awsSasl.Auth, error) {
				cfg, err := awsCfg.LoadDefaultConfig(ctx)
				if err != nil {
					return awsSasl.Auth{}, fmt.Errorf("failed to load AWS config: %w", err)
				}
				creds, err := cfg.Credentials.Retrieve(ctx)
				if err != nil {
					return awsSasl.Auth{}, fmt.Errorf("failed to retrieve AWS credentials: %w", err)
				}
				return awsSasl.Auth{
					AccessKey:    creds.AccessKeyID,
					SecretKey:    creds.SecretAccessKey,
					SessionToken: creds.SessionToken,
				}, nil
			}),
		), nil
	}

	if p.SASLUsername == "" {
		return nil, fmt.Errorf("sasl_username is required when security_protocol is %q", p.SecurityProtocol)
	}
	if p.SASLPassword == "" {
		return nil, fmt.Errorf("sasl_password is required when security_protocol is %q", p.SecurityProtocol)
	}

	switch p.SASLMechanism {
	case "PLAIN":
		return kgo.SASL(plain.Auth{User: p.SASLUsername, Pass: p.SASLPassword}.AsMechanism()), nil
	case "SCRAM-SHA-256":
		return kgo.SASL(scram.Auth{User: p.SASLUsername, Pass: p.SASLPassword}.AsSha256Mechanism()), nil
	case "SCRAM-SHA-512":
		return kgo.SASL(scram.Auth{User: p.SASLUsername, Pass: p.SASLPassword}.AsSha512Mechanism()), nil
	default:
		return nil, fmt.Errorf("unsupported sasl_mechanism: %q", p.SASLMechanism)
	}
}

func (p Profile) messageSizeOpts() ([]kgo.Opt, error) {
	var opts []kgo.Opt
	if p.MessageMaxBytes > 0 {
		opts = append(opts, kgo.ProducerBatchMaxBytes(p.MessageMaxBytes))
	}
	if p.ReceiveMessageMaxBytes > 0 {
		opts = append(opts, kgo.BrokerMaxReadBytes(p.ReceiveMessageMaxBytes))
	}
	return opts, nil
}

// ListProfiles returns the names of all profiles in the config file.
func ListProfiles() ([]string, error) {
	cfg, err := readConfigFile()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(cfg.Profiles))
	for name := range cfg.Profiles {
		names = append(names, name)
	}
	return names, nil
}

// GetProfile retrieves the raw Profile struct for a named profile.
// If the profile doesn't exist, an empty Profile is returned (not an error).
func GetProfile(name string) (Profile, error) {
	cfg, err := readConfigFile()
	if err != nil {
		return Profile{}, err
	}
	p := cfg.Profiles[name]
	p.Name = name
	return p, nil
}

// WriteProfileConfig loads the current config, updates a single profile, and writes it back
func WriteProfile(p Profile) error {
	// Load existing config or create new one
	cfg, err := readConfigFile()
	if err != nil {
		return err
	}

	// Ensure Profiles map is initialized
	if cfg.Profiles == nil {
		cfg.Profiles = make(map[string]Profile)
	}

	// Set the profile name and add/update it
	cfg.Profiles[p.Name] = p

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
			return &configFile{Profiles: make(map[string]Profile)}, nil
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config configFile
	v.Unmarshal(&config)
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	for name, profile := range config.Profiles {
		profile.Name = name
		config.Profiles[name] = profile
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
	v.Set("profiles", cfg.Profiles)

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
