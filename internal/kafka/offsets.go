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

//  ──────────────────────────── INDEX OFFSET ────────────────────────────

type IndexOffset struct {
	Index int64
}

func OnStartIndex(idx *IndexOffset) OnStartHook {
	return func(state GetState) (map[string]map[int32]kgo.Offset, error) {
		var offset kgo.Offset
		switch {
		case idx.Index == 0:
			offset = kgo.NewOffset().AtStart()
		case idx.Index > 0:
			offset = kgo.NewOffset().AtStart().Relative(idx.Index)
		default: // negative
			offset = kgo.NewOffset().AtEnd().Relative(idx.Index)
		}
		return partitionOffsets(*state.topicMeta, offset), nil
	}
}

func OnRecordIndex(idx *IndexOffset) OnRecordHook {
	return func(record kgo.Record, state GetState) (bool, error) {
		if idx.Index >= 0 {
			return record.Offset >= idx.Index, nil
		}
		// Negative index: compute target offset from high watermark
		hwm, ok := state.HighWatermarks[record.Partition]
		if !ok {
			return false, nil
		}
		target := hwm.Offset + idx.Index + 1
		return record.Offset >= target, nil
	}
}

//  ──────────────────────────── ALIAS OFFSETS ────────────────────────────

func OnStartAliasStart() OnStartHook {
	return func(state GetState) (map[string]map[int32]kgo.Offset, error) {
		return partitionOffsets(*state.topicMeta, kgo.NewOffset().AtStart()), nil
	}
}

func OnStartAliasEnd() OnStartHook {
	return func(state GetState) (map[string]map[int32]kgo.Offset, error) {
		return partitionOffsets(*state.topicMeta, kgo.NewOffset().AtEnd()), nil
	}
}

func OnRecordAliasEnd() OnRecordHook {
	return func(record kgo.Record, state GetState) (bool, error) {
		return false, nil
	}
}

func OnRecordAliasFuture() OnRecordHook {
	return func(record kgo.Record, state GetState) (bool, error) {
		return false, nil
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
