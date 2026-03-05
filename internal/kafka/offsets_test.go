package kafka

import (
	"testing"
	"time"

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
			abs := &StrictOffset{Offset: tt.offset}
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
		wantStop     bool
	}{
		{
			name:         "record before stop offset",
			stopOffset:   10,
			recordOffset: 5,
			wantStop:     false,
		},
		{
			name:         "record at stop offset",
			stopOffset:   10,
			recordOffset: 10,
			wantStop:     true,
		},
		{
			name:         "record past stop offset",
			stopOffset:   10,
			recordOffset: 15,
			wantStop:     true,
		},
		{
			name:         "zero stop offset with zero record",
			stopOffset:   0,
			recordOffset: 0,
			wantStop:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			abs := &StrictOffset{Offset: tt.stopOffset}
			td := mockTopicDetail("test-topic", []int32{0})
			state := GetState{completedPartitions: []int32{}, topicMeta: &td}
			record := kgo.Record{Offset: tt.recordOffset}

			hook := OnRecordStrict(abs)
			gotStop, err := hook(record, state)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotStop != tt.wantStop {
				t.Errorf("stop = %v, want %v", gotStop, tt.wantStop)
			}
		})
	}
}

func TestOnStartUnixMillis(t *testing.T) {
	tests := []struct {
		name       string
		millis     int64
		topic      string
		partitions []int32
	}{
		{
			name:       "single partition",
			millis:     1700000000000,
			topic:      "test-topic",
			partitions: []int32{0},
		},
		{
			name:       "multiple partitions",
			millis:     1700000000000,
			topic:      "multi-part",
			partitions: []int32{0, 1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			um := &UnixMillisOffset{Millis: tt.millis}
			td := mockTopicDetail(tt.topic, tt.partitions)
			state := GetState{completedPartitions: []int32{}, topicMeta: &td}

			hook := OnStartUnixMillis(um)
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

func TestOnStartIndex(t *testing.T) {
	tests := []struct {
		name       string
		index      int64
		topic      string
		partitions []int32
	}{
		{
			name:       "zero index starts at beginning",
			index:      0,
			topic:      "test-topic",
			partitions: []int32{0},
		},
		{
			name:       "positive index",
			index:      5,
			topic:      "test-topic",
			partitions: []int32{0, 1},
		},
		{
			name:       "negative index",
			index:      -3,
			topic:      "test-topic",
			partitions: []int32{0, 1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := &IndexOffset{Index: tt.index}
			td := mockTopicDetail(tt.topic, tt.partitions)
			state := GetState{completedPartitions: []int32{}, topicMeta: &td}

			hook := OnStartIndex(idx)
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

func TestOnRecordIndex(t *testing.T) {
	tests := []struct {
		name         string
		index        int64
		recordOffset int64
		hwm          int64 // high watermark offset (only used for negative indices)
		wantStop     bool
	}{
		{
			name:         "positive index: record before",
			index:        5,
			recordOffset: 3,
			wantStop:     false,
		},
		{
			name:         "positive index: record at index",
			index:        5,
			recordOffset: 5,
			wantStop:     true,
		},
		{
			name:         "positive index: record past index",
			index:        5,
			recordOffset: 7,
			wantStop:     true,
		},
		{
			name:         "zero index: record at zero",
			index:        0,
			recordOffset: 0,
			wantStop:     true,
		},
		{
			name:         "negative index -2: hwm=10, record at 8 (before target 9)",
			index:        -2,
			recordOffset: 8,
			hwm:          10,
			wantStop:     false,
		},
		{
			name:         "negative index -2: hwm=10, record at 9 (at target)",
			index:        -2,
			recordOffset: 9,
			hwm:          10,
			wantStop:     true,
		},
		{
			name:         "negative index -1: hwm=10, record at 9 (target=10, never stops)",
			index:        -1,
			recordOffset: 9,
			hwm:          10,
			wantStop:     false,
		},
		{
			name:         "negative index -3: hwm=5, record at 2 (before target 3)",
			index:        -3,
			recordOffset: 2,
			hwm:          5,
			wantStop:     false,
		},
		{
			name:         "negative index -3: hwm=5, record at 3 (at target)",
			index:        -3,
			recordOffset: 3,
			hwm:          5,
			wantStop:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := &IndexOffset{Index: tt.index}
			td := mockTopicDetail("test-topic", []int32{0})
			state := GetState{completedPartitions: []int32{}, topicMeta: &td}

			if tt.index < 0 {
				state.HighWatermarks = map[int32]kadm.ListedOffset{
					0: {Offset: tt.hwm},
				}
			}

			record := kgo.Record{Offset: tt.recordOffset, Partition: 0}

			hook := OnRecordIndex(idx)
			gotStop, err := hook(record, state)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotStop != tt.wantStop {
				t.Errorf("stop = %v, want %v", gotStop, tt.wantStop)
			}
		})
	}
}

func TestOnStartAliasStart(t *testing.T) {
	td := mockTopicDetail("test-topic", []int32{0, 1, 2})
	state := GetState{completedPartitions: []int32{}, topicMeta: &td}

	hook := OnStartAliasStart()
	result, err := hook(state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	topicOffsets, ok := result["test-topic"]
	if !ok {
		t.Fatalf("expected topic in result")
	}
	if len(topicOffsets) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(topicOffsets))
	}
}

func TestOnStartAliasEnd(t *testing.T) {
	td := mockTopicDetail("test-topic", []int32{0, 1, 2})
	state := GetState{completedPartitions: []int32{}, topicMeta: &td}

	hook := OnStartAliasEnd()
	result, err := hook(state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	topicOffsets, ok := result["test-topic"]
	if !ok {
		t.Fatalf("expected topic in result")
	}
	if len(topicOffsets) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(topicOffsets))
	}
}

func TestOnRecordAliasEnd(t *testing.T) {
	td := mockTopicDetail("test-topic", []int32{0})
	state := GetState{completedPartitions: []int32{}, topicMeta: &td}
	record := kgo.Record{Offset: 42}

	hook := OnRecordAliasEnd()
	stop, err := hook(record, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stop {
		t.Errorf("OnRecordAliasEnd() stop = true, want false")
	}
}

func TestOnRecordAliasFuture(t *testing.T) {
	td := mockTopicDetail("test-topic", []int32{0})
	state := GetState{completedPartitions: []int32{}, topicMeta: &td}
	record := kgo.Record{Offset: 42}

	hook := OnRecordAliasFuture()
	stop, err := hook(record, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stop {
		t.Errorf("OnRecordAliasFuture() stop = true, want false")
	}
}

func TestOnRecordUnixMillis(t *testing.T) {
	tests := []struct {
		name       string
		stopMillis int64
		recordTime time.Time
		wantStop   bool
	}{
		{
			name:       "record before stop time",
			stopMillis: 1700000000000,
			recordTime: time.UnixMilli(1699999999000),
			wantStop:   false,
		},
		{
			name:       "record at stop time",
			stopMillis: 1700000000000,
			recordTime: time.UnixMilli(1700000000000),
			wantStop:   true,
		},
		{
			name:       "record past stop time",
			stopMillis: 1700000000000,
			recordTime: time.UnixMilli(1700000001000),
			wantStop:   true,
		},
		{
			name:       "zero millis with zero record",
			stopMillis: 0,
			recordTime: time.UnixMilli(0),
			wantStop:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			um := &UnixMillisOffset{Millis: tt.stopMillis}
			td := mockTopicDetail("test-topic", []int32{0})
			state := GetState{completedPartitions: []int32{}, topicMeta: &td}
			record := kgo.Record{Timestamp: tt.recordTime}

			hook := OnRecordUnixMillis(um)
			gotStop, err := hook(record, state)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotStop != tt.wantStop {
				t.Errorf("stop = %v, want %v", gotStop, tt.wantStop)
			}
		})
	}
}
