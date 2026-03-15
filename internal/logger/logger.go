package logger

import (
	"fmt"
	"os"
	"time"

	"github.com/mattn/go-isatty"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ANSI color codes
const (
	colorReset  = "\033[0m"
	colorGray   = "\033[90m"
	colorCyan   = "\033[36m"
	colorYellow = "\033[33m"
	colorRed    = "\033[31m"
)

// L is the global sugared logger. Default is a no-op; call Init to enable logging.
var L *zap.SugaredLogger = zap.NewNop().Sugar()

// Init configures the global logger. If verbose is true, INFO level is used; otherwise WARN.
func Init(verbose bool) {
	level := zapcore.InfoLevel
	if verbose {
		level = zapcore.DebugLevel
	}
	levelEncoder := zapcore.LevelEncoder(paddedLevelEncoder)
	if isatty.IsTerminal(os.Stderr.Fd()) {
		levelEncoder = zapcore.LevelEncoder(colorLevelEncoder)
	}

	cfg := zap.Config{
		Level:    zap.NewAtomicLevelAt(level),
		Encoding: "console",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:          "T",
			LevelKey:         "L",
			MessageKey:       "M",
			EncodeTime:       zapcore.TimeEncoderOfLayout("15:04:05"),
			EncodeLevel:      levelEncoder,
			ConsoleSeparator: " ",
		},
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	base, err := cfg.Build()
	if err != nil {
		L = zap.NewNop().Sugar()
		return
	}
	L = base.Sugar()
}

// paddedLevelEncoder formats the level left-aligned in 5 chars, e.g. "WARN ".
func paddedLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(fmt.Sprintf("%-5s", l.CapitalString()))
}

// colorLevelEncoder is like paddedLevelEncoder but wraps the level in ANSI color codes.
func colorLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	var color string
	switch l {
	case zapcore.DebugLevel:
		color = colorGray
	case zapcore.InfoLevel:
		color = colorCyan
	case zapcore.WarnLevel:
		color = colorYellow
	default: // Error and above
		color = colorRed
	}
	enc.AppendString(fmt.Sprintf("%s%-5s%s", color, l.CapitalString(), colorReset))
}

// KafkaHook logs Kafka API requests and responses via the global logger.
// It implements kgo.HookBrokerWrite (request sent) and kgo.HookBrokerRead (response received).
type KafkaHook struct{}

func (h *KafkaHook) OnBrokerWrite(meta kgo.BrokerMetadata, key int16, bytesWritten int, writeWait, timeToWrite time.Duration, err error) {
	broker := fmt.Sprintf("%s:%d", meta.Host, meta.Port)
	if err != nil {
		L.Debugf("[kafka-api] (OUT), %s (%s): %v", kafkaAPIName(key), broker, err)
	} else {
		L.Debugf("[kafka-api] (OUT), %s (%s)", kafkaAPIName(key), broker)
	}
}

func (h *KafkaHook) OnBrokerRead(meta kgo.BrokerMetadata, key int16, bytesRead int, readWait, timeToRead time.Duration, err error) {
	broker := fmt.Sprintf("%s:%d", meta.Host, meta.Port)
	if err != nil {
		L.Debugf("[kafka-api] (IN), %s (%s): %v", kafkaAPIName(key), broker, err)
	} else {
		L.Debugf("[kafka-api] (IN), %s (%s)", kafkaAPIName(key), broker)
	}
}

var kafkaAPINames = map[int16]string{
	0:  "Produce",
	1:  "Fetch",
	2:  "ListOffsets",
	3:  "Metadata",
	4:  "LeaderAndIsr",
	5:  "StopReplica",
	6:  "UpdateMetadata",
	7:  "ControlledShutdown",
	8:  "OffsetCommit",
	9:  "OffsetFetch",
	10: "FindCoordinator",
	11: "JoinGroup",
	12: "Heartbeat",
	13: "LeaveGroup",
	14: "SyncGroup",
	15: "DescribeGroups",
	16: "ListGroups",
	17: "SaslHandshake",
	18: "ApiVersions",
	19: "CreateTopics",
	20: "DeleteTopics",
	21: "DeleteRecords",
	22: "InitProducerId",
	23: "OffsetForLeaderEpoch",
	24: "AddPartitionsToTxn",
	25: "AddOffsetsToTxn",
	26: "EndTxn",
	27: "WriteTxnMarkers",
	28: "TxnOffsetCommit",
	29: "DescribeAcls",
	30: "CreateAcls",
	31: "DeleteAcls",
	32: "DescribeConfigs",
	33: "AlterConfigs",
	34: "AlterReplicaLogDirs",
	35: "DescribeLogDirs",
	36: "SaslAuthenticate",
	37: "CreatePartitions",
	38: "AlterDelegationToken",
	39: "DescribeDelegationToken",
	40: "DeleteGroups",
	41: "ElectLeaders",
	42: "IncrementalAlterConfigs",
	43: "AlterPartitionReassignments",
	44: "ListPartitionReassignments",
	45: "OffsetDelete",
	46: "DescribeClientQuotas",
	47: "AlterClientQuotas",
	48: "DescribeUserScramCredentials",
	49: "AlterUserScramCredentials",
	50: "AlterPartition",
	51: "UpdateFeatures",
	52: "DescribeCluster",
	53: "DescribeProducers",
	54: "BrokerRegistration",
	55: "BrokerHeartbeat",
	56: "UnregisterBroker",
	57: "DescribeTransactions",
	58: "ListTransactions",
	59: "AllocateProducerIds",
	60: "ConsumerGroupHeartbeat",
	61: "ConsumerGroupDescribe",
	62: "ControllerRegistration",
	63: "GetTelemetrySubscriptions",
	64: "PushTelemetry",
	65: "AssignReplicasToDirs",
	66: "ListClientMetricsResources",
	67: "DescribeTopicPartitions",
}

func kafkaAPIName(key int16) string {
	if name, ok := kafkaAPINames[key]; ok {
		return name
	}
	return fmt.Sprintf("Unknown(%d)", key)
}
