package cmd

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/grant-sobkowski/frogo-cli/internal/config"
	"github.com/grant-sobkowski/frogo-cli/internal/kafka"
	"github.com/grant-sobkowski/frogo-cli/internal/logger"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ──────────────────────────── COMMAND ────────────────────────────

var from string
var to string
var wait bool
var tz string
var outputFormat string

var getCmd = &cobra.Command{
	Use:   "get <topic>",
	Short: "Consume messages from a Kafka topic",
	Long: `Read from a Kafka topic by specifying a start point (--from) and a stop point (--to).
Both flags are required and use type/value format.

Supported types for --from:
  START             first available offset
  END         		current high watermark
  offset/<n>        absolute offset (0-based)
  index/<n>         relative index from end (negative: -1 = last message)
  unix/<ts>         unix timestamp (seconds ≤10 digits, milliseconds otherwise)
  iso/<rfc3339>     ISO 8601 timestamp (e.g. 2024-01-15T09:00:00Z)
  date/<yy:mm:dd>   calendar date; resolves to start of day in --tz

Supported types for --to:
  END		        current high watermark
  FUTURE            stream indefinitely
  offset/<n>        stop at this absolute offset (exclusive)
  index/<n>         relative index from end
  unix/<ts>         stop at this unix timestamp
  iso/<rfc3339>     stop at this ISO timestamp
  date/<yy:mm:dd>   calendar date; resolves to end of day in --tz
  count/<n>         stop after consuming n messages`,
	Example: `  # Fetch the last 10 messages
  frogo get my-topic --from index/-10 --to alias/END

  # Fetch all messages from the beginning
  frogo get my-topic --from alias/START --to alias/END

  # Stream new messages as they arrive (live tail)
  frogo get my-topic --from alias/END --to alias/FUTURE --wait`,
	Args: cobra.ExactArgs(1),
	RunE: runGet,
}

func init() {
	getCmd.Flags().StringVar(&from, "from", "", "start point in type/value format (e.g. offset/0)")
	getCmd.Flags().StringVar(&to, "to", "", "stop point in type/value format (e.g. offset/100)")
	getCmd.Flags().BoolVar(&wait, "wait", false, "wait past high watermark for new messages instead of stopping at current end")
	getCmd.Flags().StringVar(&tz, "tz", "UTC", "timezone for date type offsets (e.g. UTC, America/New_York)")
	getCmd.Flags().StringVar(&outputFormat, "output", "offset-json", "output format")
	getCmd.MarkFlagRequired("from")
	getCmd.MarkFlagRequired("to")
	rootCmd.AddCommand(getCmd)
}

func runGet(cmd *cobra.Command, args []string) error {
	topic := args[0]

	// --wait disables high watermark stopping, but negative indices need high watermarks to compute targets
	if wait && strings.HasPrefix(to, "index/") {
		return fmt.Errorf("--to index/ is not compatible with --wait (negative indices require high watermarks)")
	}

	// FUTURE streams indefinitely, so implicitly enable --wait
	if to == "FUTURE" || to == "future" || to == "alias/FUTURE" {
		wait = true
	}

	// END stops at the current high watermark, so --wait (which streams past it) is contradictory
	if wait && (to == "END" || to == "end" || to == "alias/END") {
		return fmt.Errorf("--wait is not compatible with --to END (END stops at the current high watermark)")
	}

	onStart, err := parseFromArg(from)
	if err != nil {
		return err
	}

	onRecord, err := parseToArg(to)
	if err != nil {
		return err
	}

	outputFormat, err := parseFormatArg(outputFormat)
	if err != nil {
		return err
	}

	cl, err := config.Client(profile)
	if err != nil {
		return err
	}
	defer cl.Close()

	start := time.Now()
	msgCount := 0

	// Create a wrapper over onRecord to track processed records
	countingOnRecord := func(r kgo.Record, state kafka.GetState) (kafka.RecordAction, error) {
		action, err := onRecord(r, state)
		if err == nil && (action == kafka.OutputAndStop || action == kafka.OutputAndContinue) {
			msgCount++
		}
		return action, err
	}

	_, err = kafka.Get(cl, topic, onStart, countingOnRecord, !wait, outputFormat)
	if err == nil {
		logger.L.Infof("consumed %d messages in %.2fs", msgCount, time.Since(start).Seconds())
	}
	return err
}

// ──────────────────────────── PARSING ────────────────────────────

func parseTypeValueFormat(s string) (string, string, error) {
	// Accept omitted types for our 'alias' values
	// this is done for convenience as aliases are used most.
	if s == "START" || s == "start" {
		return "alias", "START", nil
	}
	if s == "END" || s == "end" {
		return "alias", "END", nil
	}
	if s == "FUTURE" || s == "future" {
		return "alias", "FUTURE", nil
	}
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
	case "unix":
		millis, err := parseUnixToMillis(value)
		if err != nil {
			return nil, fmt.Errorf("invalid --from: %w", err)
		}
		return kafka.OnStartUnixMillis(&kafka.UnixMillisOffset{Millis: millis}), nil
	case "iso":
		t, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return nil, fmt.Errorf("invalid --from: invalid ISO timestamp %q: %w", value, err)
		}
		return kafka.OnStartUnixMillis(&kafka.UnixMillisOffset{Millis: t.UnixMilli()}), nil
	case "date":
		millis, err := parseDateToMillis(value, false)
		if err != nil {
			return nil, fmt.Errorf("invalid --from: %w", err)
		}
		return kafka.OnStartUnixMillis(&kafka.UnixMillisOffset{Millis: millis}), nil
	case "index":
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid --from: invalid index value %q: %w", value, err)
		}
		return kafka.OnStartIndex(&kafka.IndexOffset{Index: v}), nil
	case "alias":
		switch value {
		case "START":
			return kafka.OnStartAliasStart(), nil
		case "END":
			return kafka.OnStartAliasEnd(), nil
		default:
			return nil, fmt.Errorf("unsupported --from alias %q (supported: START, END)", value)
		}
	default:
		return nil, fmt.Errorf("unsupported from type %q (supported: offset, unix, iso, date, index, alias)", typ)
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
	case "unix":
		millis, err := parseUnixToMillis(value)
		if err != nil {
			return nil, fmt.Errorf("invalid --to: %w", err)
		}
		return kafka.OnRecordUnixMillis(&kafka.UnixMillisOffset{Millis: millis}), nil
	case "iso":
		t, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return nil, fmt.Errorf("invalid --to: invalid ISO timestamp %q: %w", value, err)
		}
		return kafka.OnRecordUnixMillis(&kafka.UnixMillisOffset{Millis: t.UnixMilli()}), nil
	case "date":
		millis, err := parseDateToMillis(value, true)
		if err != nil {
			return nil, fmt.Errorf("invalid --to: %w", err)
		}
		return kafka.OnRecordUnixMillis(&kafka.UnixMillisOffset{Millis: millis}), nil
	case "index":
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid --to: invalid index value %q: %w", value, err)
		}
		return kafka.OnRecordIndex(&kafka.IndexOffset{Index: v}), nil
	case "count":
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid --to: count value must be a positive integer, got %q", value)
		}
		return kafka.OnRecordCount(&kafka.CountOffset{N: n}), nil
	case "alias":
		switch value {
		case "END":
			return kafka.OnRecordAliasEnd(), nil
		case "FUTURE":
			return kafka.OnRecordAliasFuture(), nil
		default:
			return nil, fmt.Errorf("unsupported --to alias %q (supported: END, FUTURE)", value)
		}
	default:
		return nil, fmt.Errorf("unsupported to type %q (supported: offset, unix, iso, date, index, alias, count)", typ)
	}
}

func parseFormatArg(outputFormat string) (string, error) {
	switch outputFormat {
	case "offset-json":
		return "offset-json", nil
	case "plain":
		return "plain", nil
	default:
		return "", fmt.Errorf("unsupported format type %q (supported: basic-json, plain)", outputFormat)
	}
}

// parseUnixToMillis parses a unix timestamp string as seconds or milliseconds.
// Values ≤ 9999999999 (10 digits) are treated as seconds, otherwise milliseconds.
func parseUnixToMillis(value string) (int64, error) {
	v, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid unix timestamp %q: %w", value, err)
	}
	if v <= 9999999999 {
		return v * 1000, nil
	}
	return v, nil
}

// parseDateToMillis parses a yy:mm:dd date string into unix millis.
// If endOfDay is true, resolves to 23:59:59.999; otherwise 00:00:00.000.
func parseDateToMillis(value string, endOfDay bool) (int64, error) {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return 0, fmt.Errorf("invalid timezone %q: %w", tz, err)
	}
	t, err := time.ParseInLocation("06:01:02", value, loc)
	if err != nil {
		return 0, fmt.Errorf("invalid date %q (expected yy:mm:dd): %w", value, err)
	}
	if endOfDay {
		t = t.Add(24*time.Hour - time.Millisecond)
	}
	return t.UnixMilli(), nil
}
