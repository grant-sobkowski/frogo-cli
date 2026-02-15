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
	completedPartitions []int32
	topicMeta           *kadm.TopicDetail
}

// Consume a given topic using hooks
func Get(cl *kgo.Client, topic string, onStart OnStartHook, onRecord OnRecordHook) ([]*kgo.Record, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Make topic/cluster metadata request
	topicMeta, err := topicMetadata(cl, topic)
	if err != nil {
		return nil, err
	}

	state := GetState{completedPartitions: []int32{}, topicMeta: topicMeta}
	records := []*kgo.Record{}

	startOffsets, err := onStart(state)
	if err != nil {
		//TODO
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
			return nil, fmt.Errorf("error while polling fetches: %w", err)
		}

		fetches.EachRecord(func(r *kgo.Record) {
			// skip records from completed partitions
			if slices.Contains(state.completedPartitions, r.Partition) {
				return
			}
			// call onRecord hook
			stop, err := onRecord(*r, state)
			if err != nil {
				//TODO
			}
			// onRecord hook has deemed this partition complete
			if stop == true {
				state.completedPartitions = append(state.completedPartitions, r.Partition)
				return
			}
			records = append(records, r)
		})

		// Consumption stops once all partitions are marked 'complete' by our onRecord hook.
		// This requires at least one record to be processed from each partition in the topic.
		// For cases where some partitions sit stale, the onStart hook should set starting
		// offsets accordingly to prevent unnecessary waiting / timeouts
		if len(state.completedPartitions) == len(topicMeta.Partitions.Numbers()) {
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
	defer adminClient.Close()

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
