package cmd

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/grant-sobkowski/frogo-cli/internal/config"
	"github.com/grant-sobkowski/frogo-cli/internal/kafka"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
)

var filePath string
var format string

var putCmd = &cobra.Command{
	Use:   "put <topic>",
	Short: "Produce messages to a Kafka topic from a file",
	Args:  cobra.ExactArgs(1),
	PreRunE: func(cmd *cobra.Command, args []string) error {
		switch format {
		case "utf8":
			return nil
		default:
			return fmt.Errorf("unsupported format %q (supported: utf8)", format)
		}
	},
	RunE: runPut,
}

func init() {
	putCmd.Flags().StringVar(&filePath, "file", "", "path to input file")
	putCmd.Flags().StringVar(&format, "format", "utf8", "input format (utf8)")
	putCmd.MarkFlagRequired("file")
	rootCmd.AddCommand(putCmd)
}

func runPut(cmd *cobra.Command, args []string) error {
	topic := args[0]

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	var records []*kgo.Record
	switch format {
	case "utf8":
		records, err = parseUTF8Records(file, topic)
	}
	if err != nil {
		return err
	}

	cl, err := config.Client(profile)
	if err != nil {
		return err
	}
	defer cl.Close()

	return kafka.Put(cl, topic, records)
}

func parseUTF8Records(reader io.Reader, topic string) ([]*kgo.Record, error) {
	var records []*kgo.Record
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		records = append(records, &kgo.Record{
			Topic: topic,
			Value: []byte(scanner.Text()),
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read input: %w", err)
	}
	return records, nil
}
