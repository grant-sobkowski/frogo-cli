package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/grant-sobkowski/frogo-cli/internal/config"
	"github.com/grant-sobkowski/frogo-cli/internal/logger"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

var partitions int32

var topicCreateCmd = &cobra.Command{
	Use:   "create <topic>",
	Short: "Create a Kafka topic",
	Example: `  # Create a topic with 1 partition (default)
  frogo topic create my-topic

  # Create a topic with 3 partitions
  frogo topic create my-topic --partitions 3`,
	Args: cobra.ExactArgs(1),
	RunE: runCreateTopic,
}

func init() {
	topicCreateCmd.Flags().Int32Var(&partitions, "partitions", 1, "number of partitions")
	topicCmd.AddCommand(topicCreateCmd)
}

func runCreateTopic(cmd *cobra.Command, args []string) error {
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

	resp, err := adminClient.CreateTopic(ctx, partitions, -1, nil, topic)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}
	if resp.Err != nil {
		return fmt.Errorf("failed to create topic: %w", resp.Err)
	}

	logger.L.Infof("created topic %q with %d partitions", topic, partitions)
	return nil
}
