package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func mockTopicDetail(topic string, partitions []int32) kadm.TopicDetail {
	pd := make(kadm.PartitionDetails)
	for _, id := range partitions {
		pd[id] = kadm.PartitionDetail{Partition: id}
	}
	return kadm.TopicDetail{
		Topic:      topic,
		Partitions: pd,
	}
}

func TestOnStartStrict(t *testing.T) {
	tests := []struct {
		name       string
		offset     int64
		topic      string
		partitions []int32
	}{
		{
			name:       "single partition",
			offset:     42,
			topic:      "test-topic",
			partitions: []int32{0},
		},
		{
			name:       "multiple partitions",
			offset:     100,
			topic:      "multi-part",
			partitions: []int32{0, 1, 2},
		},
		{
			name:       "zero offset",
			offset:     0,
			topic:      "from-start",
			partitions: []int32{0, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			abs := NewStrictOffset(tt.offset)
			td := mockTopicDetail(tt.topic, tt.partitions)
			state := GetState{completedPartitions: []int32{}, topicMeta: &td}

			hook := OnStartStrict(abs)
			result, err := hook(state)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			topicOffsets, ok := result[tt.topic]
			if !ok {
				t.Fatalf("expected topic %q in result", tt.topic)
			}

			if len(topicOffsets) != len(tt.partitions) {
				t.Fatalf("expected %d partitions, got %d", len(tt.partitions), len(topicOffsets))
			}

			for _, id := range tt.partitions {
				if _, ok := topicOffsets[id]; !ok {
					t.Errorf("expected partition %d in result", id)
				}
			}
		})
	}
}

func TestOnRecordStrict(t *testing.T) {
	tests := []struct {
		name         string
		stopOffset   int64
		recordOffset int64
		want         bool
	}{
		{
			name:         "record before stop offset",
			stopOffset:   10,
			recordOffset: 5,
			want:         false,
		},
		{
			name:         "record at stop offset",
			stopOffset:   10,
			recordOffset: 10,
			want:         true,
		},
		{
			name:         "record past stop offset",
			stopOffset:   10,
			recordOffset: 15,
			want:         true,
		},
		{
			name:         "zero stop offset with zero record",
			stopOffset:   0,
			recordOffset: 0,
			want:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			abs := NewStrictOffset(tt.stopOffset)
			td := mockTopicDetail("test-topic", []int32{0})
			state := GetState{completedPartitions: []int32{}, topicMeta: &td}
			record := kgo.Record{Offset: tt.recordOffset}

			hook := OnRecordStrict(abs)
			got, err := hook(record, state)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("OnRecordStrict() = %v, want %v", got, tt.want)
			}
		})
	}
}
