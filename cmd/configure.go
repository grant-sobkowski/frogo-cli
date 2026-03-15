package cmd

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/grant-sobkowski/frogo-cli/internal/config"
	"github.com/spf13/cobra"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage frogo configuration",
	Long: `Manage frogo configuration profiles stored in ~/.frogo/config.toml.

Use the --profile flag to target a specific profile (defaults to "default").`,
}

var configSetCmd = &cobra.Command{
	Use:   "set",
	Short: "Set a configuration value for a profile",
	Long:  `Set a single configuration field for a profile. Each subcommand sets one field and leaves others unchanged.`,
}

var configListProfilesCmd = &cobra.Command{
	Use:   "list-profiles",
	Short: "List all configured profiles",
	RunE: func(cmd *cobra.Command, args []string) error {
		names, err := config.ListProfiles()
		if err != nil {
			return err
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Println(name)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(configCmd)
	configCmd.AddCommand(configSetCmd)
	configCmd.AddCommand(configListProfilesCmd)
	configSetCmd.AddCommand(configSetBrokersCmd)
	configSetCmd.AddCommand(configSetSecurityProtocolCmd)
	configSetCmd.AddCommand(configSetSASLMechanismCmd)
	configSetCmd.AddCommand(configSetSASLUsernameCmd)
	configSetCmd.AddCommand(configSetSASLPasswordCmd)
	configSetCmd.AddCommand(configSetMessageMaxBytesCmd)
	configSetCmd.AddCommand(configSetReceiveMessageMaxBytesCmd)
	configSetCmd.AddCommand(configSetTLSCACertFileCmd)
	configSetCmd.AddCommand(configSetTLSClientCertFileCmd)
	configSetCmd.AddCommand(configSetTLSClientKeyFileCmd)
	configSetCmd.AddCommand(configSetTLSSkipVerifyCmd)
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

var configSetBrokersCmd = &cobra.Command{
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

var configSetSecurityProtocolCmd = &cobra.Command{
	Use:   "security-protocol <protocol>",
	Short: "Set the security protocol (plaintext, ssl, sasl_plaintext, sasl_ssl)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return updateProfile(profile, func(p *config.Profile) {
			p.SecurityProtocol = args[0]
		})
	},
}

var configSetSASLMechanismCmd = &cobra.Command{
	Use:   "sasl-mechanism <mechanism>",
	Short: "Set the SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, AWS_MSK_IAM)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return updateProfile(profile, func(p *config.Profile) {
			p.SASLMechanism = args[0]
		})
	},
}

var configSetSASLUsernameCmd = &cobra.Command{
	Use:   "sasl-username <username>",
	Short: "Set the SASL username for a profile",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return updateProfile(profile, func(p *config.Profile) {
			p.SASLUsername = args[0]
		})
	},
}

var configSetSASLPasswordCmd = &cobra.Command{
	Use:   "sasl-password <password>",
	Short: "Set the SASL password for a profile",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return updateProfile(profile, func(p *config.Profile) {
			p.SASLPassword = args[0]
		})
	},
}

var configSetMessageMaxBytesCmd = &cobra.Command{
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

var configSetReceiveMessageMaxBytesCmd = &cobra.Command{
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

var configSetTLSCACertFileCmd = &cobra.Command{
	Use:   "tls-ca-cert-file <path>",
	Short: "Set the path to the TLS CA certificate file",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return updateProfile(profile, func(p *config.Profile) {
			p.TLSCACertFile = args[0]
		})
	},
}

var configSetTLSClientCertFileCmd = &cobra.Command{
	Use:   "tls-client-cert-file <path>",
	Short: "Set the path to the TLS client certificate file",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return updateProfile(profile, func(p *config.Profile) {
			p.TLSClientCertFile = args[0]
		})
	},
}

var configSetTLSClientKeyFileCmd = &cobra.Command{
	Use:   "tls-client-key-file <path>",
	Short: "Set the path to the TLS client key file",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return updateProfile(profile, func(p *config.Profile) {
			p.TLSClientKeyFile = args[0]
		})
	},
}

var configSetTLSSkipVerifyCmd = &cobra.Command{
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
