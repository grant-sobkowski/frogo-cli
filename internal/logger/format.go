package logger

import (
	"fmt"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func LogWatermarks(wm map[int32]kadm.ListedOffset) {
	L.Infof("[offsets] high watermarks: %s", formatWatermarks(wm))
}

func LogStartOffsets(offsets map[string]map[int32]kgo.Offset) {
	L.Infof("[offsets] start offsets: %s", formatStartOffsets(offsets))
}

func formatWatermarks(wm map[int32]kadm.ListedOffset) string {
	parts := make([]string, 0, len(wm))
	for partition, lo := range wm {
		parts = append(parts, fmt.Sprintf("%d: %d", partition, lo.Offset))
	}
	slices.Sort(parts)
	return "{" + strings.Join(parts, ", ") + "}"
}

func formatStartOffsets(offsets map[string]map[int32]kgo.Offset) string {
	parts := make([]string, 0)
	for _, partitions := range offsets {
		for partition, offset := range partitions {
			parts = append(parts, fmt.Sprintf("%d: %s", partition, formatOffset(offset)))
		}
	}
	slices.Sort(parts)
	return "{" + strings.Join(parts, ", ") + "}"
}

func formatOffset(o kgo.Offset) string {
	switch o.EpochOffset().Offset {
	case -2:
		return "START"
	case -1:
		return "END"
	default:
		return fmt.Sprintf("%d", o.EpochOffset().Offset)
	}
}
