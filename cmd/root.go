/*
Copyright © 2025 GRANT SOBKOWSKI <grant.sobkowski@gmail.com>
*/
package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/grant-sobkowski/frogo-cli/internal/config"
	"github.com/grant-sobkowski/frogo-cli/internal/logger"
	"github.com/spf13/cobra"
)

var profile string
var verbose bool

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "frogo",
	Short: "A human-friendly Kafka CLI client",
	Long: `Frogo is a command-line tool for interacting with Kafka clusters.
It provides a simple, human-friendly interface for consuming messages,
managing topics, and exploring your Kafka data.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

func RootCmd() *cobra.Command { return rootCmd }

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	defer logger.L.Sync() //nolint:errcheck
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Global flags available to all commands
	rootCmd.PersistentFlags().StringVarP(&profile, "profile", "p", "default", "Configuration profile to use for Kafka connection")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose logging to stderr (DEBUG level)")
	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		cmd.Root().SilenceUsage = true
		logger.Init(verbose)
		if !cmd.Root().PersistentFlags().Changed("profile") {
			if env := os.Getenv("FROGO_PROFILE"); env != "" {
				profile = env
			}
		}
		logger.L.Debugf("[config] profile: %s", profile)
		return checkProfileConfigured(cmd)
	}
}

// isConfigExemptCommand returns true for commands that don't require a configured Kafka profile.
func isConfigExemptCommand(cmd *cobra.Command) bool {
	parts := strings.Fields(cmd.CommandPath())
	for _, part := range parts[1:] { // skip root "frogo"
		if part == "config" || part == "mockserver" {
			return true
		}
	}
	return false
}

// checkProfileConfigured validates that the active profile has brokers configured,
// printing helpful suggestions if not.
func checkProfileConfigured(cmd *cobra.Command) error {
	if isConfigExemptCommand(cmd) {
		return nil
	}

	allProfiles, err := config.ListProfiles()
	if err != nil {
		return err
	}

	if len(allProfiles) == 0 {
		configPath, _ := config.Path()
		return fmt.Errorf("No configuration profiles configured.\nSee `frogo config help` for some examples.\n\nCurrent configuration file path: %s", configPath)
	}

	p, err := config.GetProfile(profile)
	if err != nil {
		return err
	}

	if len(p.Brokers) == 0 {
		var lines []string
		for _, name := range allProfiles {
			if name == profile {
				continue
			}
			other, _ := config.GetProfile(name)
			if len(other.Brokers) > 0 {
				brokerDisplay := other.Brokers[0]
				if len(other.Brokers) > 1 {
					brokerDisplay += ", ..."
				}
				lines = append(lines, fmt.Sprintf("%s (%s)", name, brokerDisplay))
			} else {
				lines = append(lines, name)
			}
		}

		msg := fmt.Sprintf("No configurations set for profile: %s.\nUse FROGO_PROFILE or --profile to set the current profile.", profile)
		if len(lines) > 0 {
			msg += "\n\nDid you mean one of the following?\n\n" + strings.Join(lines, "\n")
		}
		return fmt.Errorf("%s", msg)
	}

	return nil
}
