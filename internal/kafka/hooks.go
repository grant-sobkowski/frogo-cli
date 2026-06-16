package kafka

import (
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// OnStartHook is called once before consuming begins.
// It receives the current GetState and returns partition offsets to consume from.
// Returning nil offsets signals that no consumption is needed.
type OnStartHook func(state GetState) (map[string]map[int32]kgo.Offset, error)

// RecordAction is returned by OnRecordHook to control consumption flow.
type RecordAction int

const (
	Stop              RecordAction = iota // discard record, mark partition complete
	OutputAndStop                         // output record, mark partition complete
	OutputAndContinue                     // output record, keep consuming
)

// OnRecordHook is called on each consumed record.
// It returns a RecordAction directing whether to output the record and whether to stop.
type OnRecordHook func(record kgo.Record, state GetState) (RecordAction, error)

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
	return func(record kgo.Record, state GetState) (RecordAction, error) {
		if record.Offset >= abs.Offset {
			return Stop, nil
		}
		if record.Offset+1 == abs.Offset {
			return OutputAndStop, nil
		}
		return OutputAndContinue, nil
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
	return func(record kgo.Record, state GetState) (RecordAction, error) {
		if record.Timestamp.UnixMilli() >= um.Millis {
			return Stop, nil
		}
		return OutputAndContinue, nil
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
	return func(record kgo.Record, state GetState) (RecordAction, error) {
		var stop bool
		if idx.Index >= 0 {
			stop = record.Offset >= idx.Index
		} else {
			hwm, ok := state.HighWatermarks[record.Partition]
			if !ok {
				return OutputAndContinue, nil
			}
			target := hwm.Offset + idx.Index + 1
			stop = record.Offset >= target
		}
		if stop {
			return Stop, nil
		}
		return OutputAndContinue, nil
	}
}

//  ──────────────────────────── COUNT OFFSET ────────────────────────────

type CountOffset struct {
	N int64
}

func OnRecordCount(c *CountOffset) OnRecordHook {
	var seen int64
	return func(record kgo.Record, state GetState) (RecordAction, error) {
		if seen >= c.N {
			return Stop, nil
		}
		seen++
		if seen >= c.N {
			return OutputAndStop, nil
		}
		return OutputAndContinue, nil
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
	return func(record kgo.Record, state GetState) (RecordAction, error) {
		return OutputAndContinue, nil
	}
}

func OnRecordAliasFuture() OnRecordHook {
	return func(record kgo.Record, state GetState) (RecordAction, error) {
		return OutputAndContinue, nil
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
