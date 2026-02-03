package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// startHook represents an offsetLike struct. When get is run,
// offset() is called, resolving the offsetLike to a kgo.Offset
// NOTE: kgo.Offset supports absolute, relative, and timestamp based input.
// See: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Offset
type startHook interface {
	offset() kgo.Offset
}

// stopHook is called on each processed record in Get. If true,
// Get stops processing the owning partition of the record
type stopHook interface {
	check(kgo.Record) bool
}

// absoluteOffset represents any positive topic offset.
type absoluteOffset struct {
	absOffset int64
}

// returns kgo offset directly mapping to input offset
func (o *absoluteOffset) offset() kgo.Offset {
	usable := kgo.NewOffset().At(o.absOffset)
	return usable
}

func Get(cl *kgo.Client, topic string, start startHook, stop stopHook) ([]kgo.Record, error) {
	records := []kgo.Record{}

	metadata, err := clusterMetadata(cl, topic)
	if err != nil {
		return nil, err
	}

	startOffset := start.offset() // resolve start hook
	startOffsets := partitionOffsets(metadata.Topics[topic], startOffset)

	// TODO: Consume using record iterator, update partition status using stop hook

	return records, nil
}

// partitionOffsets formats an offset for kgo.Client.AddConsumePartitions
func partitionOffsets(topicDetail kadm.TopicDetail, at kgo.Offset) map[string]map[int32]kgo.Offset {

	offsets := make(map[string]map[int32]kgo.Offset)
	topic := topicDetail.Topic

	for id := range topicDetail.Partitions {
		offsets[topic][id] = at
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
