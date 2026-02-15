package cmd

import (
	"encoding/base64"
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

func TestParseBase64Records(t *testing.T) {
	messages := []string{"hello world", "foo bar", "baz"}
	var lines []string
	for _, m := range messages {
		lines = append(lines, base64.StdEncoding.EncodeToString([]byte(m)))
	}
	input := strings.Join(lines, "\n")
	topic := "test-topic"

	records, err := parseBase64Records(strings.NewReader(input), topic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(records) != len(messages) {
		t.Fatalf("expected %d records, got %d", len(messages), len(records))
	}

	for i, r := range records {
		if r.Topic != topic {
			t.Errorf("record[%d]: expected topic %q, got %q", i, topic, r.Topic)
		}
		if string(r.Value) != messages[i] {
			t.Errorf("record[%d]: expected value %q, got %q", i, messages[i], string(r.Value))
		}
	}
}

func TestParseRecordJSONRecords_ValueOnly(t *testing.T) {
	input := `{"value": "message1"}
{"value": "message2"}`
	topic := "test-topic"

	records, err := parseRecordJSONRecords(strings.NewReader(input), topic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	for i, r := range records {
		if r.Topic != topic {
			t.Errorf("record[%d]: expected topic %q, got %q", i, topic, r.Topic)
		}
		if r.Key != nil {
			t.Errorf("record[%d]: expected nil key, got %q", i, string(r.Key))
		}
	}
	if string(records[0].Value) != "message1" {
		t.Errorf("record[0]: expected value %q, got %q", "message1", string(records[0].Value))
	}
	if string(records[1].Value) != "message2" {
		t.Errorf("record[1]: expected value %q, got %q", "message2", string(records[1].Value))
	}
}

func TestParseRecordJSONRecords_ValueAndKey(t *testing.T) {
	input := `{"key": "my-key", "value": "message1"}`
	topic := "test-topic"

	records, err := parseRecordJSONRecords(strings.NewReader(input), topic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	r := records[0]
	if r.Topic != topic {
		t.Errorf("expected topic %q, got %q", topic, r.Topic)
	}
	if string(r.Key) != "my-key" {
		t.Errorf("expected key %q, got %q", "my-key", string(r.Key))
	}
	if string(r.Value) != "message1" {
		t.Errorf("expected value %q, got %q", "message1", string(r.Value))
	}
}

func TestParseRecordJSONRecords_NestedValue(t *testing.T) {
	input := `{"value": {"nested": "this is some nested json"}}`
	topic := "test-topic"

	records, err := parseRecordJSONRecords(strings.NewReader(input), topic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	r := records[0]
	if r.Topic != topic {
		t.Errorf("expected topic %q, got %q", topic, r.Topic)
	}
	expected := `{"nested": "this is some nested json"}`
	if string(r.Value) != expected {
		t.Errorf("expected value %q, got %q", expected, string(r.Value))
	}
	if r.Key != nil {
		t.Errorf("expected nil key, got %q", string(r.Key))
	}
}
