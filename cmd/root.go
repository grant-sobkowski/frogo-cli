/*
Copyright © 2025 GRANT SOBKOWSKI <grant.sobkowski@gmail.com>
*/
package cmd

import (
	"os"

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
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose logging to stderr (INFO level)")
	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		cmd.Root().SilenceUsage = true
		logger.Init(verbose)
		if !cmd.Root().PersistentFlags().Changed("profile") {
			if env := os.Getenv("FROGO_PROFILE"); env != "" {
				profile = env
			}
		}
		logger.L.Infof("[config] profile: %s", profile)
		return nil
	}
}
