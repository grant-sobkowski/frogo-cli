package cmd

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/grant-sobkowski/frogo-cli/internal/config"
	"github.com/grant-sobkowski/frogo-cli/internal/kafka"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ──────────────────────────── COMMAND ────────────────────────────

var from string
var to string
var wait bool

var getCmd = &cobra.Command{
	Use:   "get <topic>",
	Short: "Consume messages from a Kafka topic",
	Args:  cobra.ExactArgs(1),
	RunE:  runGet,
}

func init() {
	getCmd.Flags().StringVar(&from, "from", "", "start point in type/value format (e.g. offset/0)")
	getCmd.Flags().StringVar(&to, "to", "", "stop point in type/value format (e.g. offset/100)")
	getCmd.Flags().BoolVar(&wait, "wait", false, "wait past high watermark for new messages instead of stopping at current end")
	getCmd.MarkFlagRequired("from")
	getCmd.MarkFlagRequired("to")
	rootCmd.AddCommand(getCmd)
}

func runGet(cmd *cobra.Command, args []string) error {
	topic := args[0]

	onStart, err := parseFromArg(from)
	if err != nil {
		return err
	}

	onRecord, err := parseToArg(to)
	if err != nil {
		return err
	}

	cl, err := config.Client(profile)
	if err != nil {
		return err
	}
	defer cl.Close()

	records, err := kafka.Get(cl, topic, onStart, onRecord, !wait)
	if err != nil {
		return err
	}

	printRecords(records)
	return nil
}

// ──────────────────────────── PARSING ────────────────────────────

func parseTypeValueFormat(s string) (string, string, error) {
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("expected format \"type/value\", got %q", s)
	}
	return parts[0], parts[1], nil
}

func parseFromArg(from string) (kafka.OnStartHook, error) {
	typ, value, err := parseTypeValueFormat(from)
	if err != nil {
		return nil, fmt.Errorf("invalid --from: %w", err)
	}

	switch typ {
	case "offset":
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid --from: invalid offset value %q: %w", value, err)
		}
		return kafka.OnStartStrict(&kafka.StrictOffset{Offset: v}), nil
	default:
		return nil, fmt.Errorf("unsupported from type %q (supported: offset)", typ)
	}
}

func parseToArg(to string) (kafka.OnRecordHook, error) {
	typ, value, err := parseTypeValueFormat(to)
	if err != nil {
		return nil, fmt.Errorf("invalid --to: %w", err)
	}

	switch typ {
	case "offset":
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid --to: invalid offset value %q: %w", value, err)
		}
		return kafka.OnRecordStrict(&kafka.StrictOffset{Offset: v}), nil
	default:
		return nil, fmt.Errorf("unsupported to type %q (supported: offset)", typ)
	}
}

// ──────────────────────────── OUTPUT ────────────────────────────

func printRecords(records []*kgo.Record) {
	for _, r := range records {
		fmt.Printf("offset=%d value=%s\n", r.Offset, string(r.Value))
	}
}
