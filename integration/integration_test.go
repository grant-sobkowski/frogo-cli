package integration

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/grant-sobkowski/frogo-cli/cmd"
	"github.com/twmb/franz-go/pkg/kfake"
)

var cluster *kfake.Cluster

func TestMain(m *testing.M) {
	var err error
	cluster, err = kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create kfake cluster: %v\n", err)
		os.Exit(1)
	}

	tmpDir, err := os.MkdirTemp("", "frogo-integration-*")
	if err != nil {
		cluster.Close()
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}

	addrs := cluster.ListenAddrs()
	configContent := fmt.Sprintf("[profiles.test]\nbrokers = [\"%s\"]\n", strings.Join(addrs, "\", \""))
	if err := os.WriteFile(filepath.Join(tmpDir, "config.toml"), []byte(configContent), 0644); err != nil {
		cluster.Close()
		os.RemoveAll(tmpDir)
		fmt.Fprintf(os.Stderr, "failed to write config: %v\n", err)
		os.Exit(1)
	}

	os.Setenv("FROGO_CONFIG_DIR", tmpDir)

	code := m.Run()

	cluster.Close()
	os.RemoveAll(tmpDir)
	os.Exit(code)
}

// runCmd executes a frogo CLI command and returns captured stdout.
func runCmd(t *testing.T, args ...string) string {
	t.Helper()

	// Capture os.Stdout since commands use fmt.Printf directly
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}
	os.Stdout = w

	root := cmd.RootCmd()
	root.SetArgs(args)
	root.SetOut(w)
	root.SetErr(io.Discard)

	execErr := root.Execute()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)
	r.Close()

	if execErr != nil {
		t.Fatalf("command %v failed: %v\noutput: %s", args, execErr, buf.String())
	}

	return buf.String()
}

func fixturesDir() string {
	return filepath.Join("fixtures")
}

// setupFixtureTopic creates a topic, populates it from a fixture file, and
// registers cleanup to delete the topic.
func setupFixtureTopic(t *testing.T, topic string, fixtureFile string, format string) {
	t.Helper()

	runCmd(t, "create-topic", topic, "--profile", "test")

	path := filepath.Join(fixturesDir(), fixtureFile)
	runCmd(t, "put", topic, "--file", path, "--format", format, "--profile", "test")

	t.Cleanup(func() {
		runCmd(t, "delete-topic", topic, "--profile", "test")
	})
}

// ─────────────────────────── TOPIC MANAGEMENT ───────────────────────────

func TestIntegration_CreateAndDeleteTopic(t *testing.T) {
	topic := "test-create-delete"

	out := runCmd(t, "create-topic", topic, "--profile", "test")
	if !strings.Contains(out, topic) {
		t.Errorf("create-topic output should mention topic name, got: %s", out)
	}

	out = runCmd(t, "delete-topic", topic, "--profile", "test")
	if !strings.Contains(out, topic) {
		t.Errorf("delete-topic output should mention topic name, got: %s", out)
	}
}

// ─────────────────────────── PUT FORMATS ───────────────────────────

func TestIntegration_PutUTF8(t *testing.T) {
	topic := "test-put-utf8"
	runCmd(t, "create-topic", topic, "--profile", "test")
	t.Cleanup(func() {
		runCmd(t, "delete-topic", topic, "--profile", "test")
	})

	path := filepath.Join(fixturesDir(), "put-utf8.txt")
	runCmd(t, "put", topic, "--file", path, "--format", "utf8", "--profile", "test")

	// --to well past end; high watermark check stops consumption after all 5 messages
	out := runCmd(t, "get", topic, "--from", "offset/0", "--to", "offset/99", "--profile", "test")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 5 {
		t.Fatalf("expected 5 lines, got %d: %s", len(lines), out)
	}
	if !strings.Contains(out, "hello world") {
		t.Errorf("expected 'hello world' in get output, got: %s", out)
	}
	if !strings.Contains(out, "line five") {
		t.Errorf("expected 'line five' in get output, got: %s", out)
	}
}

func TestIntegration_PutBase64(t *testing.T) {
	topic := "test-put-base64"
	runCmd(t, "create-topic", topic, "--profile", "test")
	t.Cleanup(func() {
		runCmd(t, "delete-topic", topic, "--profile", "test")
	})

	path := filepath.Join(fixturesDir(), "put-base64.txt")
	runCmd(t, "put", topic, "--file", path, "--format", "base64", "--profile", "test")

	out := runCmd(t, "get", topic, "--from", "offset/0", "--to", "offset/99", "--profile", "test")
	// base64 decoded values
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 5 {
		t.Fatalf("expected 5 lines, got %d: %s", len(lines), out)
	}
	if !strings.Contains(out, "hello world") {
		t.Errorf("expected decoded 'hello world' in get output, got: %s", out)
	}
	if !strings.Contains(out, "line five") {
		t.Errorf("expected decoded 'line five' in get output, got: %s", out)
	}
}

func TestIntegration_PutRecordJSON(t *testing.T) {
	topic := "test-put-record-json"
	runCmd(t, "create-topic", topic, "--profile", "test")
	t.Cleanup(func() {
		runCmd(t, "delete-topic", topic, "--profile", "test")
	})

	path := filepath.Join(fixturesDir(), "put-record-json.txt")
	runCmd(t, "put", topic, "--file", path, "--format", "record-json", "--profile", "test")

	out := runCmd(t, "get", topic, "--from", "offset/0", "--to", "offset/99", "--profile", "test")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 5 {
		t.Fatalf("expected 5 lines, got %d: %s", len(lines), out)
	}
	if !strings.Contains(out, "hello world") {
		t.Errorf("expected 'hello world' in get output, got: %s", out)
	}
	if !strings.Contains(out, "no key message") {
		t.Errorf("expected 'no key message' in get output, got: %s", out)
	}
}

// ─────────────────────────── GET WITH OFFSETS ───────────────────────────

func TestIntegration_GetFromOffsetToOffset(t *testing.T) {
	topic := "test-get-offsets"
	setupFixtureTopic(t, topic, "get-from-offset-to-offset.txt", "utf8")

	// --to well past end; high watermark stops consumption after offsets 1-4
	out := runCmd(t, "get", topic, "--from", "offset/1", "--to", "offset/99", "--profile", "test")

	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected 4 lines, got %d: %s", len(lines), out)
	}

	expected := []string{"msg-one", "msg-two", "msg-three", "msg-four"}
	for i, exp := range expected {
		if !strings.Contains(lines[i], exp) {
			t.Errorf("line %d: expected to contain %q, got %q", i, exp, lines[i])
		}
	}

	// Verify offset 0 message is NOT included
	if strings.Contains(out, "msg-zero") {
		t.Errorf("should not contain msg-zero when starting from offset/1, got: %s", out)
	}
}
