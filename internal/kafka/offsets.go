package kafka

import (
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// onStartHook is called once before consuming begins.
// It receives the current GetState and returns partition offsets to consume from.
// Returning nil offsets signals that no consumption is needed.
type OnStartHook func(state GetState) (map[string]map[int32]kgo.Offset, error)

// onRecordHook is called on each consumed record.
// It receives the current GetState and returns true if the current partition
// should be considered completed, false otherwise.
type OnRecordHook func(record kgo.Record, state GetState) (bool, error)

//  ──────────────────────────── STRICT OFFSET ────────────────────────────

type StrictOffset struct {
	Offset int64
}

func OnStartStrict(abs *StrictOffset) OnStartHook {
	return func(state GetState) (map[string]map[int32]kgo.Offset, error) {
		offset := kgo.NewOffset().At(abs.Offset)
		return partitionOffsets(*state.topicMeta, offset), nil
	}
}

func OnRecordStrict(abs *StrictOffset) OnRecordHook {
	return func(record kgo.Record, state GetState) (bool, error) {
		return record.Offset >= abs.Offset, nil
	}
}

//  ──────────────────────────── UNIX MILLIS OFFSET ────────────────────────────

type UnixMillisOffset struct {
	Millis int64
}

func OnStartUnixMillis(um *UnixMillisOffset) OnStartHook {
	return func(state GetState) (map[string]map[int32]kgo.Offset, error) {
		offset := kgo.NewOffset().AfterMilli(um.Millis)
		return partitionOffsets(*state.topicMeta, offset), nil
	}
}

func OnRecordUnixMillis(um *UnixMillisOffset) OnRecordHook {
	return func(record kgo.Record, state GetState) (bool, error) {
		return record.Timestamp.UnixMilli() >= um.Millis, nil
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
