package kafka

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// GetState holds information about the status of the topic consumer.
type GetState struct {
	completedPartitions []int32              // holds IDs of partitions deemed completed by onRecord hook
	lastProcessed       map[int32]kgo.Record // holds offsets of processed records, by partition ID
	topicMeta           *kadm.TopicDetail
}

// Consume a given topic using hooks.
// stopOnHighWatermark, when set, will stop processing records when the offsets
// of the last known high watermark have been reached. For infrequently updated topics
// this prevents waiting for conditions that have already been met.
func Get(cl *kgo.Client, topic string, onStart OnStartHook, onRecord OnRecordHook, stopOnHighWatermark bool) ([]*kgo.Record, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Make topic/cluster metadata request
	topicMeta, err := topicMetadata(cl, topic)
	if err != nil {
		return nil, err
	}

	var highWatermarks *map[int32]kadm.ListedOffset
	if stopOnHighWatermark {
		highWatermarks, err = topicHighWatermarks(cl, topicMeta)
		if err != nil {
			return nil, fmt.Errorf("failed to get high watermarks: %w", err)
		}
	}

	state := GetState{completedPartitions: []int32{}, lastProcessed: make(map[int32]kgo.Record), topicMeta: topicMeta}
	records := []*kgo.Record{}

	startOffsets, err := onStart(state)
	if err != nil {
		return nil, fmt.Errorf("onStart hook failed: %w", err)
	}
	// onStart can specify a nil value to indicate that no consumption is needed
	if startOffsets == nil {
		return records, nil
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
			stop, err := onRecord(*r, state)
			if err != nil {
				hookErr = fmt.Errorf("onRecord hook failed: %w", err)
				return
			}
			// onRecord hook has deemed this partition complete
			if stop {
				state.completedPartitions = append(state.completedPartitions, r.Partition)
				return
			}
			records = append(records, r)
		})
		if hookErr != nil {
			return nil, hookErr
		}

		// Consumption stops once all partitions are marked 'complete' by our onRecord hook.
		// This requires at least one record to be processed from each partition in the topic.
		// For cases where some partitions sit stale, the onStart hook should set starting
		// offsets accordingly to prevent unnecessary waiting / timeouts
		if len(state.completedPartitions) == len(topicMeta.Partitions.Numbers()) {
			return records, nil
		}

		if stopOnHighWatermark && isPastHighWatermark(*highWatermarks, state.lastProcessed) {
			return records, nil
		}

		select {
		case <-ctx.Done():
			return records, ctx.Err()
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

// isPastHighWatermark returns true if every partition in the high watermark map
// has a lastProcessed record at or past its high watermark offset.
// High watermark is the offset of the next message to be written, so offset >= hwm-1
// means all existing messages have been consumed.
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
