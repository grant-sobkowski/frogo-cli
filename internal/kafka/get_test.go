package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestIsPastHighWatermark(t *testing.T) {
	tests := []struct {
		name           string
		highWatermarks map[int32]kadm.ListedOffset
		lastProcessed  map[int32]kgo.Record
		want           bool
	}{
		{
			name: "single partition at watermark",
			highWatermarks: map[int32]kadm.ListedOffset{
				0: {Offset: 5},
			},
			lastProcessed: map[int32]kgo.Record{
				0: {Offset: 4},
			},
			want: true,
		},
		{
			name: "single partition past watermark",
			highWatermarks: map[int32]kadm.ListedOffset{
				0: {Offset: 5},
			},
			lastProcessed: map[int32]kgo.Record{
				0: {Offset: 5},
			},
			want: true,
		},
		{
			name: "single partition below watermark",
			highWatermarks: map[int32]kadm.ListedOffset{
				0: {Offset: 5},
			},
			lastProcessed: map[int32]kgo.Record{
				0: {Offset: 3},
			},
			want: false,
		},
		{
			name: "partition not yet processed",
			highWatermarks: map[int32]kadm.ListedOffset{
				0: {Offset: 5},
			},
			lastProcessed: map[int32]kgo.Record{},
			want:          false,
		},
		{
			name: "empty partition skipped",
			highWatermarks: map[int32]kadm.ListedOffset{
				0: {Offset: 0},
			},
			lastProcessed: map[int32]kgo.Record{},
			want:          true,
		},
		{
			name: "multiple partitions all at watermark",
			highWatermarks: map[int32]kadm.ListedOffset{
				0: {Offset: 10},
				1: {Offset: 5},
				2: {Offset: 8},
			},
			lastProcessed: map[int32]kgo.Record{
				0: {Offset: 9},
				1: {Offset: 4},
				2: {Offset: 7},
			},
			want: true,
		},
		{
			name: "multiple partitions one behind",
			highWatermarks: map[int32]kadm.ListedOffset{
				0: {Offset: 10},
				1: {Offset: 5},
			},
			lastProcessed: map[int32]kgo.Record{
				0: {Offset: 9},
				1: {Offset: 2},
			},
			want: false,
		},
		{
			name: "mix of empty and non-empty partitions",
			highWatermarks: map[int32]kadm.ListedOffset{
				0: {Offset: 5},
				1: {Offset: 0},
			},
			lastProcessed: map[int32]kgo.Record{
				0: {Offset: 4},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isPastHighWatermark(tt.highWatermarks, tt.lastProcessed)
			if got != tt.want {
				t.Errorf("isPastHighWatermark() = %v, want %v", got, tt.want)
			}
		})
	}
}
