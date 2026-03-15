package cmd

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"time"

	"github.com/grant-sobkowski/frogo-cli/internal/config"
	"github.com/grant-sobkowski/frogo-cli/internal/kafka"
	"github.com/grant-sobkowski/frogo-cli/internal/logger"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
)

var filePath string
var text string
var format string

var putCmd = &cobra.Command{
	Use:   "put <topic>",
	Short: "Produce messages to a Kafka topic from a file",
	Long: `Read messages from a file and produce them to a Kafka topic.
Each line in the file is treated as one message.

Supported formats:
  utf8         plain text, one message per line (default)
  base64       base64-encoded message values, one per line
  record-json  JSON objects with "key" and "value" fields, one per line`,
	Example: `  # Produce plain-text messages from a file
  frogo put my-topic --file messages.txt

  # Produce base64-encoded binary messages
  frogo put my-topic --file messages.b64 --format base64

  # Produce messages with explicit keys using record-json format
  frogo put my-topic --file records.jsonl --format record-json`,
	Args: cobra.ExactArgs(1),
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if filePath == "" && text == "" {
			return fmt.Errorf("one of --file or --text is required")
		}
		if filePath != "" && text != "" {
			return fmt.Errorf("--file and --text are mutually exclusive")
		}
		switch format {
		case "utf8", "base64", "record-json":
			return nil
		default:
			return fmt.Errorf("unsupported format %q (supported: utf8, base64, record-json)", format)
		}
	},
	RunE: runPut,
}

func init() {
	putCmd.Flags().StringVar(&filePath, "file", "", "path to input file")
	putCmd.Flags().StringVar(&text, "text", "", "input text, alternative to --file")
	putCmd.Flags().StringVar(&format, "format", "utf8", "input format (utf8, base64, record-json)")
	rootCmd.AddCommand(putCmd)
}

func runPut(cmd *cobra.Command, args []string) error {
	topic := args[0]

	var reader io.Reader
	if text != "" {
		reader = strings.NewReader(text)
	} else {
		file, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer file.Close()
		reader = file
	}

	var records []*kgo.Record
	var err error
	switch format {
	case "utf8":
		records, err = parseUTF8Records(reader, topic)
	case "base64":
		records, err = parseBase64Records(reader, topic)
	case "record-json":
		records, err = parseRecordJSONRecords(reader, topic)
	}
	if err != nil {
		return err
	}

	cl, err := config.Client(profile)
	if err != nil {
		return err
	}
	defer cl.Close()

	start := time.Now()
	err = kafka.Put(cl, topic, records)
	if err == nil {
		logger.L.Infof("%d messages produced in %.2fs", len(records), time.Since(start).Seconds())
	}
	return err
}

type recordJSON struct {
	Key   json.RawMessage `json:"key"`
	Value json.RawMessage `json:"value"`
}

// rawToBytes resolves a json.RawMessage to bytes.
// JSON strings are unquoted, objects/arrays are kept as raw JSON bytes.
func rawToBytes(raw json.RawMessage) ([]byte, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		return []byte(s), nil
	}
	return []byte(raw), nil
}

func parseRecordJSONRecords(reader io.Reader, topic string) ([]*kgo.Record, error) {
	var records []*kgo.Record
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		var rj recordJSON
		if err := json.Unmarshal(scanner.Bytes(), &rj); err != nil {
			return nil, fmt.Errorf("failed to parse record-json: %w", err)
		}
		if rj.Value == nil {
			return nil, fmt.Errorf("record-json: \"value\" is required")
		}
		value, err := rawToBytes(rj.Value)
		if err != nil {
			return nil, fmt.Errorf("record-json: invalid value: %w", err)
		}
		key, err := rawToBytes(rj.Key)
		if err != nil {
			return nil, fmt.Errorf("record-json: invalid key: %w", err)
		}
		records = append(records, &kgo.Record{
			Topic: topic,
			Key:   key,
			Value: value,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read input: %w", err)
	}
	return records, nil
}

func parseBase64Records(reader io.Reader, topic string) ([]*kgo.Record, error) {
	var records []*kgo.Record
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		decoded, err := base64.StdEncoding.DecodeString(scanner.Text())
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64: %w", err)
		}
		records = append(records, &kgo.Record{
			Topic: topic,
			Value: decoded,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read input: %w", err)
	}
	return records, nil
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
