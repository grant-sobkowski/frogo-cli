/*
Copyright © 2025 GRANT SOBKOWSKI <grant.sobkowski@gmail.com>
*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var profile string

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

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Global flags available to all commands
	rootCmd.PersistentFlags().StringVarP(&profile, "profile", "p", "default", "Configuration profile to use for Kafka connection")
}
