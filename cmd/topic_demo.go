package cmd

import (
	"github.com/grant-sobkowski/frogo-cli/internal/logger"
	"github.com/spf13/cobra"
	"strings"
	"time"
)

var topicDemoCmd = &cobra.Command{
	Use:   "demo <scenario>",
	Short: "Create a ready-to-go topic from a predefined template",
	// TODO: fix this Example section
	Example: `  # Create a topic with 1 partition (default)
  frogo topic create my-topic

  # Create a topic with 3 partitions
  frogo topic create my-topic --partitions 3`,
	ValidArgs: []string{"hello-world"},
	Args:      cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	RunE:      runDemoTopic,
}

func init() {
	topicCmd.AddCommand(topicDemoCmd)
}

func runDemoTopic(cmd *cobra.Command, args []string) error {
	scenario := args[0]
	start := time.Now()

	// Topic naming convention: frdemo-<scenario>
	switch scenario {
	case "hello-world":
		createTopic("frdemo-hello-world", 1)

		reader := strings.NewReader(helloWorldText())
		recordsProduced, err := putRecordsWithReader(reader, "frdemo-hello-world", "utf8")
		if err != nil {
			return err
		}
		logger.L.Infof("%v messages produced in %.2fs", recordsProduced, time.Since(start).Seconds())
		return nil
	}

	return nil
}

func helloWorldText() string {
	text := `hello
world
from
frogo!`
	return text
}
