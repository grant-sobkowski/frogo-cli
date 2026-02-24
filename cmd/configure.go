package cmd

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/grant-sobkowski/frogo-cli/internal/config"
	"github.com/spf13/cobra"
)

var configureCmd = &cobra.Command{
	Use:   "configure",
	Short: "Set configuration values for a profile",
	Long: `Configure profile settings stored in ~/.frogo/config.toml.

Use the --profile flag to target a specific profile (defaults to "default").
Each subcommand sets one field and leaves others unchanged.`,
}

func init() {
	rootCmd.AddCommand(configureCmd)
	configureCmd.AddCommand(configureBrokersCmd)
	configureCmd.AddCommand(configureSecurityProtocolCmd)
	configureCmd.AddCommand(configureSASLMechanismCmd)
	configureCmd.AddCommand(configureSASLUsernameCmd)
	configureCmd.AddCommand(configureSASLPasswordCmd)
	configureCmd.AddCommand(configureMessageMaxBytesCmd)
	configureCmd.AddCommand(configureReceiveMessageMaxBytesCmd)
	configureCmd.AddCommand(configureTLSCACertFileCmd)
	configureCmd.AddCommand(configureTLSClientCertFileCmd)
	configureCmd.AddCommand(configureTLSClientKeyFileCmd)
	configureCmd.AddCommand(configureTLSSkipVerifyCmd)
}

// updateProfile loads the named profile (creating it if absent), applies fn, then writes it back.
func updateProfile(name string, fn func(*config.Profile)) error {
	p, err := config.GetProfile(name)
	if err != nil {
		return err
	}
	fn(&p)
	return config.WriteProfile(p)
}

var configureBrokersCmd = &cobra.Command{
	Use:   "brokers <broker1,broker2,...>",
	Short: "Set the broker addresses for a profile (comma-delimited)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		brokers := strings.Split(args[0], ",")
		return updateProfile(profile, func(p *config.Profile) {
			p.Brokers = brokers
		})
	},
}

var configureSecurityProtocolCmd = &cobra.Command{
	Use:   "security-protocol <protocol>",
	Short: "Set the security protocol (plaintext, ssl, sasl_plaintext, sasl_ssl)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return updateProfile(profile, func(p *config.Profile) {
			p.SecurityProtocol = args[0]
		})
	},
}

var configureSASLMechanismCmd = &cobra.Command{
	Use:   "sasl-mechanism <mechanism>",
	Short: "Set the SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, AWS_MSK_IAM)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return updateProfile(profile, func(p *config.Profile) {
			p.SASLMechanism = args[0]
		})
	},
}

var configureSASLUsernameCmd = &cobra.Command{
	Use:   "sasl-username <username>",
	Short: "Set the SASL username for a profile",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return updateProfile(profile, func(p *config.Profile) {
			p.SASLUsername = args[0]
		})
	},
}

var configureSASLPasswordCmd = &cobra.Command{
	Use:   "sasl-password <password>",
	Short: "Set the SASL password for a profile",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return updateProfile(profile, func(p *config.Profile) {
			p.SASLPassword = args[0]
		})
	},
}

var configureMessageMaxBytesCmd = &cobra.Command{
	Use:   "message-max-bytes <bytes>",
	Short: "Set the maximum producer message size in bytes",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		n, err := strconv.ParseInt(args[0], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid byte count %q: %w", args[0], err)
		}
		return updateProfile(profile, func(p *config.Profile) {
			p.MessageMaxBytes = int32(n)
		})
	},
}

var configureReceiveMessageMaxBytesCmd = &cobra.Command{
	Use:   "receive-message-max-bytes <bytes>",
	Short: "Set the maximum receive message size in bytes",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		n, err := strconv.ParseInt(args[0], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid byte count %q: %w", args[0], err)
		}
		return updateProfile(profile, func(p *config.Profile) {
			p.ReceiveMessageMaxBytes = int32(n)
		})
	},
}

var configureTLSCACertFileCmd = &cobra.Command{
	Use:   "tls-ca-cert-file <path>",
	Short: "Set the path to the TLS CA certificate file",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return updateProfile(profile, func(p *config.Profile) {
			p.TLSCACertFile = args[0]
		})
	},
}

var configureTLSClientCertFileCmd = &cobra.Command{
	Use:   "tls-client-cert-file <path>",
	Short: "Set the path to the TLS client certificate file",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return updateProfile(profile, func(p *config.Profile) {
			p.TLSClientCertFile = args[0]
		})
	},
}

var configureTLSClientKeyFileCmd = &cobra.Command{
	Use:   "tls-client-key-file <path>",
	Short: "Set the path to the TLS client key file",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return updateProfile(profile, func(p *config.Profile) {
			p.TLSClientKeyFile = args[0]
		})
	},
}

var configureTLSSkipVerifyCmd = &cobra.Command{
	Use:   "tls-skip-verify <true|false>",
	Short: "Set whether to skip TLS certificate verification",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		val, err := strconv.ParseBool(args[0])
		if err != nil {
			return fmt.Errorf("invalid boolean %q (use true or false)", args[0])
		}
		return updateProfile(profile, func(p *config.Profile) {
			p.TLSSkipVerify = val
		})
	},
}
