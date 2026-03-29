package cmd

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/grant-sobkowski/frogo-cli/internal/config"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

var topicListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all topics in the cluster",
	RunE:  runListTopics,
}

func init() {
	topicCmd.AddCommand(topicListCmd)
}

func runListTopics(cmd *cobra.Command, args []string) error {
	cl, err := config.Client(profile)
	if err != nil {
		return err
	}
	defer cl.Close()

	adminClient := kadm.NewClient(cl)
	defer adminClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	topics, err := adminClient.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("failed to list topics: %w", err)
	}

	names := topics.Names()
	sort.Strings(names)
	for _, name := range names {
		fmt.Println(name)
	}
	return nil
}
