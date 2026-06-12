package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/grant-sobkowski/frogo-cli/internal/config"
	"github.com/grant-sobkowski/frogo-cli/internal/logger"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

var topicDemoCleanupCmd = &cobra.Command{
	Use:   "demo-cleanup",
	Short: "Delete all demo topics (prefixed with frdemo-)",
	Args:  cobra.NoArgs,
	RunE:  runDemoCleanup,
}

func init() {
	topicCmd.AddCommand(topicDemoCleanupCmd)
}

func runDemoCleanup(cmd *cobra.Command, args []string) error {
	cl, err := config.Client(profile)
	if err != nil {
		return err
	}
	defer cl.Close()

	adminClient := kadm.NewClient(cl)
	defer adminClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topics, err := adminClient.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("failed to list topics: %w", err)
	}

	var demoTopics []string
	for _, name := range topics.Names() {
		if strings.HasPrefix(name, "frdemo-") {
			demoTopics = append(demoTopics, name)
		}
	}

	if len(demoTopics) == 0 {
		logger.L.Info("no frdemo- topics found")
		return nil
	}

	resps, err := adminClient.DeleteTopics(ctx, demoTopics...)
	if err != nil {
		return fmt.Errorf("failed to delete topics: %w", err)
	}
	if err := resps.Error(); err != nil {
		return fmt.Errorf("failed to delete topics: %w", err)
	}

	for _, name := range demoTopics {
		logger.L.Infof("deleted topic %q", name)
	}
	return nil
}
