package kafka

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// resolvable represents an implementation of an offset type,
// which can be resolved at runtime.
// NOTE: kgo.Offset supports absolute, relative, and timestamp based input.
// See: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Offset
type offsetResolver interface {
	offsetResolve() kgo.Offset
}

// stopHook is called on each processed record in Get. If true,
// Get stops processing the owning partition of the record
type stopChecker interface {
	stopCheck(*kgo.Record) bool
}

// absolute represents any positive topic offset.
type absolute struct {
	offset int64
}

func (abs *absolute) offsetResolve() kgo.Offset {
	offset := kgo.NewOffset().At(abs.offset)
	return offset
}

// Get resolves starting and stopping points and returns consumed messages.
func Get(cl *kgo.Client, topic string, start offsetResolver, stopper stopChecker) ([]*kgo.Record, error) {
	meta, err := clusterMetadata(cl, topic)
	if err != nil {
		return nil, err
	}

	offset := start.offsetResolve()
	pStarts := partitionOffsets(meta.Topics[topic], offset)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cl.AddConsumePartitions(pStarts)
	records := []*kgo.Record{}
	completedPartitions := []int32{}

	for {
		fetches := cl.PollFetches(ctx)
		err := fetches.Err()
		if err != nil {
			return nil, fmt.Errorf("error while polling fetches: %w", err)
		}

		fetches.EachRecord(func(r *kgo.Record) {
			if slices.Contains(completedPartitions, r.Partition) {
				return
			}
			if stopper.stopCheck(r) {
				completedPartitions = append(completedPartitions, r.Partition)
				return
			}
			records = append(records, r)

		})

		if len(completedPartitions) == len(meta.Topics[topic].Partitions.Numbers()) {
			return records, nil
		}

		// Check if context is done
		select {
		case <-ctx.Done():
			return records, ctx.Err()
		default:
		}
	}
}

// partitionOffsets formats `at` offset as a map of partition offsets
func partitionOffsets(td kadm.TopicDetail, at kgo.Offset) map[string]map[int32]kgo.Offset {

	offsets := make(map[string]map[int32]kgo.Offset)
	offsets[td.Topic] = make(map[int32]kgo.Offset)

	for id := range td.Partitions {
		offsets[td.Topic][id] = at
	}

	return offsets
}

func clusterMetadata(cl *kgo.Client, topic string) (*kadm.Metadata, error) {
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

	return &metadata, nil
}
