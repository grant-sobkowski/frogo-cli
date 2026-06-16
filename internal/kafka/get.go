package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/grant-sobkowski/frogo-cli/internal/logger"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type StdoutRecord struct {
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Value     string `json:"value"`
}

// OutputRecord prints a record to stdout as JSON.
func OutputRecord(r *kgo.Record, outputFormat string) {
	if outputFormat == "offset-json" {
		b, _ := json.Marshal(StdoutRecord{r.Partition, r.Offset, string(r.Value)})
		fmt.Println(string(b))
		return
	}
	if outputFormat == "plain" {
		fmt.Println(string(r.Value))
		return
	}

	// Error should be caught in the cli parsing steps, panic on unexpected format
	err := fmt.Errorf("OutputRecord - recieved invalid outputFormat: %v", outputFormat)
	panic(err)
}

// GetState holds information about the status of the topic consumer.
type GetState struct {
	completedPartitions []int32              // holds IDs of partitions deemed completed by onRecord hook
	lastProcessed       map[int32]kgo.Record // holds offsets of processed records, by partition ID
	topicMeta           *kadm.TopicDetail
	HighWatermarks      map[int32]kadm.ListedOffset // populated when stopOnHighWatermark is true
}

// Consume a given topic using hooks.
// stopOnHighWatermark, when set, will stop processing records when the offsets
// of the last known high watermark have been reached. For infrequently updated topics
// this prevents waiting for conditions that have already been met.
func Get(cl *kgo.Client, topic string, onStart OnStartHook, onRecord OnRecordHook, stopOnHighWatermark bool, outputFormat string) ([]*kgo.Record, error) {

	var ctx context.Context
	var cancel context.CancelFunc
	if stopOnHighWatermark {
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()

	// Make topic/cluster metadata request
	topicMeta, err := topicMetadata(cl, topic)
	if err != nil {
		return nil, err
	}

	state := GetState{completedPartitions: []int32{}, lastProcessed: make(map[int32]kgo.Record), topicMeta: topicMeta}

	// Make listOffsets request
	// In cases where --wait is not specified, we need the high watermark offset
	// in order to determine when all existing messages have been completed.
	var highWatermarks *map[int32]kadm.ListedOffset

	if stopOnHighWatermark {
		logger.L.Debugf("[offsets] stopOnHighWatermark true, fetching topic watermarks")
		highWatermarks, err = topicHighWatermarks(cl, topicMeta)
		if err != nil {
			return nil, fmt.Errorf("failed to get high watermarks: %w", err)
		}
		state.HighWatermarks = *highWatermarks
		logger.LogWatermarks(*highWatermarks)
	} else {
		logger.L.Debugf("[offsets] stopOnHighWatermark false, skipping fetching of topic watermarks")
	}

	startOffsets, err := onStart(state)
	if err != nil {
		return nil, fmt.Errorf("onStart hook failed: %w", err)
	}
	logger.LogStartOffsets(startOffsets)

	// OnStart hook returned nil, nothing to do; exit
	if startOffsets == nil {
		return nil, nil
	}

	// Already at or past highWaterMark (e.g. empty topic); exit
	if stopOnHighWatermark && isPastHighWatermark(*highWatermarks, state.lastProcessed) {
		if isTopicEmpty(*highWatermarks) {
			logger.L.Warnf("%s empty, nothing to do", topic)
		}
		return nil, nil
	}

	// AddConsumePartitions configures franz-go to start retrieving records.
	// startOffsets defines the exact point to start consuming messages;
	// record retrieval is stateless.
	cl.AddConsumePartitions(startOffsets)

	for {
		// Wait for at least one Kafka broker to respond to Fetch API request
		fetches := cl.PollFetches(ctx)
		err := fetches.Err()
		if err != nil {
			return nil, fmt.Errorf("failed to poll fetches: %w", err)
		}

		var hookErr error

		// Iterate through records returned by franz-go client using EachRecord
		fetches.EachRecord(func(r *kgo.Record) {
			state.lastProcessed[r.Partition] = *r
			if hookErr != nil {
				return
			}
			// skip records from completed partitions
			if slices.Contains(state.completedPartitions, r.Partition) {
				return
			}
			// call onRecord hook
			action, err := onRecord(*r, state)
			if err != nil {
				hookErr = fmt.Errorf("onRecord hook failed: %w", err)
				return
			}
			if action == OutputAndStop || action == OutputAndContinue {
				OutputRecord(r, outputFormat)
			}
			if action == Stop || action == OutputAndStop {
				state.completedPartitions = append(state.completedPartitions, r.Partition)
				return
			}
		})
		if hookErr != nil {
			return nil, hookErr
		}

		// All partitions are marked 'complete' by our onRecord hook; exit.
		// Each partition must have had at least one record processed for this to occur.
		if len(state.completedPartitions) == len(topicMeta.Partitions.Numbers()) {
			return nil, nil
		}

		// Reached high watermark of topic; exit.
		// Prevents cases where onRecord hook will never return true because topic isn't
		// recieving any new messages.
		if stopOnHighWatermark && isPastHighWatermark(*highWatermarks, state.lastProcessed) {
			return nil, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}
}

// topicMetadata retrieves cluster metadata and checks topic is defined
func topicMetadata(cl *kgo.Client, topic string) (*kadm.TopicDetail, error) {
	adminClient := kadm.NewClient(cl)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	metadata, err := adminClient.Metadata(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %w", err)
	}

	// Topic load errors are seperate from metadata request errors
	err = metadata.Topics.Error()
	if err != nil {
		return nil, fmt.Errorf("metadata response contains error: %w", err)
	}

	ok := metadata.Topics.Has(topic)
	if !ok {
		return nil, fmt.Errorf("Topic %v not found in cluster metadata.", topic)
	}

	topicMetadata := metadata.Topics[topic]

	return &topicMetadata, nil
}

// topicHighWatermarks retrieves the highest existing offset for each topic partition
func topicHighWatermarks(cl *kgo.Client, td *kadm.TopicDetail) (*map[int32]kadm.ListedOffset, error) {
	adminClient := kadm.NewClient(cl)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	listedOffsets, err := adminClient.ListEndOffsets(ctx, td.Topic)
	if err != nil {
		return nil, err
	}

	err = listedOffsets.Error()
	if err != nil {
		return nil, fmt.Errorf("found error in list offsets response: %w", err)
	}

	watermarks, ok := listedOffsets[td.Topic]
	if !ok {
		return nil, fmt.Errorf("Topic %v not found in list offsets response", td.Topic)
	}

	return &watermarks, nil
}

// isTopicEmpty returns true if all partitions have a high watermark of 0.
func isTopicEmpty(highWatermarks map[int32]kadm.ListedOffset) bool {
	for _, listed := range highWatermarks {
		if listed.Offset != 0 {
			return false
		}
	}
	return true
}

// isPastHighWatermark returns true if every partition in the high watermark map
// has a lastProcessed record at or past its high watermark offset.
func isPastHighWatermark(highWatermarks map[int32]kadm.ListedOffset, lastProcessed map[int32]kgo.Record) bool {
	for partition, listed := range highWatermarks {
		// Empty partition (hwm 0) — nothing to consume, skip
		if listed.Offset == 0 {
			continue
		}
		record, ok := lastProcessed[partition]
		if !ok {
			return false
		}
		// High watermark is the next offset to be written,
		// so the last existing record is at hwm-1
		if record.Offset < listed.Offset-1 {
			return false
		}
	}
	return true
}
