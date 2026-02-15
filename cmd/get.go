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

var getCmd = &cobra.Command{
	Use:   "get <topic>",
	Short: "Consume messages from a Kafka topic",
	Args:  cobra.ExactArgs(1),
	RunE:  runGet,
}

func init() {
	getCmd.Flags().StringVar(&from, "from", "", "start point in type/value format (e.g. offset/0)")
	getCmd.Flags().StringVar(&to, "to", "", "stop point in type/value format (e.g. offset/100)")
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

	records, err := kafka.Get(cl, topic, onStart, onRecord)
	if err != nil {
		return err
	}

	printRecords(records)
	return nil
}

// ──────────────────────────── IOTA TYPES ────────────────────────────

type fromType int

const (
	strictFrom fromType = iota
)

type toType int

const (
	strictTo toType = iota
)

// ──────────────────────────── FROM / TO ARG INTERFACES ──────────────

type fromArg interface {
	validate(offsetLike string) error
	parse(offsetLike string) kafka.OnStartHook
}

type toArg interface {
	validate(offsetLike string) error
	parse(offsetLike string) kafka.OnRecordHook
}

// ──────────────────────────── PARSING ────────────────────────────

func parseTypeValueFormat(s string) (string, string, error) {
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("expected format \"type/value\", got %q", s)
	}
	return parts[0], parts[1], nil
}

func newFromArg(typ string) (fromArg, error) {
	switch typ {
	case "offset":
		return &fromStrictOffset{}, nil
	default:
		return nil, fmt.Errorf("unsupported from type %q (supported: offset)", typ)
	}
}

func newToArg(typ string) (toArg, error) {
	switch typ {
	case "offset":
		return &toStrictOffset{}, nil
	default:
		return nil, fmt.Errorf("unsupported to type %q (supported: offset)", typ)
	}
}

func parseFromArg(from string) (kafka.OnStartHook, error) {
	typ, value, err := parseTypeValueFormat(from)
	if err != nil {
		return nil, fmt.Errorf("invalid --from: %w", err)
	}

	arg, err := newFromArg(typ)
	if err != nil {
		return nil, err
	}

	if err := arg.validate(value); err != nil {
		return nil, fmt.Errorf("invalid --from: %w", err)
	}

	return arg.parse(value), nil
}

func parseToArg(to string) (kafka.OnRecordHook, error) {
	typ, value, err := parseTypeValueFormat(to)
	if err != nil {
		return nil, fmt.Errorf("invalid --to: %w", err)
	}

	arg, err := newToArg(typ)
	if err != nil {
		return nil, err
	}

	if err := arg.validate(value); err != nil {
		return nil, fmt.Errorf("invalid --to: %w", err)
	}

	return arg.parse(value), nil
}

// ──────────────────────────── STRICT OFFSET ─────────────────────────

type fromStrictOffset struct{}

func (s *fromStrictOffset) validate(offsetLike string) error {
	_, err := strconv.ParseInt(offsetLike, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid offset value %q: %w", offsetLike, err)
	}
	return nil
}

func (s *fromStrictOffset) parse(offsetLike string) kafka.OnStartHook {
	v, _ := strconv.ParseInt(offsetLike, 10, 64)
	return kafka.OnStartStrict(kafka.NewStrictOffset(v))
}

type toStrictOffset struct{}

func (s *toStrictOffset) validate(offsetLike string) error {
	_, err := strconv.ParseInt(offsetLike, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid offset value %q: %w", offsetLike, err)
	}
	return nil
}

func (s *toStrictOffset) parse(offsetLike string) kafka.OnRecordHook {
	v, _ := strconv.ParseInt(offsetLike, 10, 64)
	return kafka.OnRecordStrict(kafka.NewStrictOffset(v))
}

// ──────────────────────────── OUTPUT ────────────────────────────

func printRecords(records []*kgo.Record) {
	for _, r := range records {
		fmt.Printf("offset=%d value=%s\n", r.Offset, string(r.Value))
	}
}
