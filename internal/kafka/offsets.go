package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// OutputRecord prints a record to stdout.
func OutputRecord(r *kgo.Record) {
	fmt.Printf("%d %s\n", r.Offset, string(r.Value))
}

// onStartHook is called once before consuming begins.
// It receives the current GetState and returns partition offsets to consume from.
// Returning nil offsets signals that no consumption is needed.
type OnStartHook func(state GetState) (map[string]map[int32]kgo.Offset, error)

// onRecordHook is called on each consumed record.
// It returns (stop, error): stop marks the partition complete.
// Hooks call state.OutputRecord to include a record in results.
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
		// This record is outside our range, stop and don't output anything
		if record.Offset >= abs.Offset {
			return true, nil
		}
		// Record is last message in our range, output then stop
		if record.Offset+1 == abs.Offset {
			OutputRecord(&record)
			return true, nil
		}
		OutputRecord(&record)
		return false, nil
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
		past := record.Timestamp.UnixMilli() >= um.Millis
		if !past {
			OutputRecord(&record)
		}
		return past, nil
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
		var stop bool
		if idx.Index >= 0 {
			stop = record.Offset >= idx.Index
		} else {
			hwm, ok := state.HighWatermarks[record.Partition]
			if !ok {
				OutputRecord(&record)
				return false, nil
			}
			target := hwm.Offset + idx.Index + 1
			stop = record.Offset >= target
		}
		if !stop {
			OutputRecord(&record)
		}
		return stop, nil
	}
}

//  ──────────────────────────── COUNT OFFSET ────────────────────────────

type CountOffset struct {
	N int64
}

func OnRecordCount(c *CountOffset) OnRecordHook {
	var seen int64
	return func(record kgo.Record, state GetState) (bool, error) {
		if seen >= c.N {
			return true, nil
		}
		OutputRecord(&record)
		seen++
		return seen >= c.N, nil
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
		OutputRecord(&record)
		return false, nil
	}
}

func OnRecordAliasFuture() OnRecordHook {
	return func(record kgo.Record, state GetState) (bool, error) {
		OutputRecord(&record)
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
