package cmd

import "github.com/spf13/cobra"

var topicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Manage Kafka topics",
}

func init() {
	rootCmd.AddCommand(topicCmd)
}
