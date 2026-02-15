package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/grant-sobkowski/frogo-cli/internal/config"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

var deleteTopicCmd = &cobra.Command{
	Use:   "delete-topic <topic>",
	Short: "Delete a Kafka topic",
	Args:  cobra.ExactArgs(1),
	RunE:  runDeleteTopic,
}

func init() {
	rootCmd.AddCommand(deleteTopicCmd)
}

func runDeleteTopic(cmd *cobra.Command, args []string) error {
	topic := args[0]

	cl, err := config.Client(profile)
	if err != nil {
		return err
	}
	defer cl.Close()

	adminClient := kadm.NewClient(cl)
	defer adminClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resps, err := adminClient.DeleteTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}
	if err := resps.Error(); err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}

	fmt.Printf("deleted topic %q\n", topic)
	return nil
}
