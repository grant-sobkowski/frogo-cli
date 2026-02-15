package cmd

import (
	"strings"
	"testing"
)

func TestParseUTF8Records(t *testing.T) {
	input := "this is message 1\nthis is message 2\nthis is message 3"
	topic := "test-topic"

	records, err := parseUTF8Records(strings.NewReader(input), topic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []string{"this is message 1", "this is message 2", "this is message 3"}

	if len(records) != len(expected) {
		t.Fatalf("expected %d records, got %d", len(expected), len(records))
	}

	for i, r := range records {
		if r.Topic != topic {
			t.Errorf("record[%d]: expected topic %q, got %q", i, topic, r.Topic)
		}
		if string(r.Value) != expected[i] {
			t.Errorf("record[%d]: expected value %q, got %q", i, expected[i], string(r.Value))
		}
	}
}
