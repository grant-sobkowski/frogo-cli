//go:build integration

package mockserver

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

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
func runCmd(t *testing.T, args ...string) (string, string) {
	t.Helper()

	// Capture os.Stdout since commands use fmt.Printf directly
	oldStdout := os.Stdout
	oldStderr := os.Stderr

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}
	os.Stdout = w

	r2, w2, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}
	os.Stderr = w2

	root := cmd.RootCmd()
	root.SetArgs(args)
	root.SetOut(w)
	root.SetErr(w2)

	execErr := root.Execute()

	w.Close()
	w2.Close()

	os.Stdout = oldStdout
	os.Stderr = oldStderr

	var buf bytes.Buffer
	var buf2 bytes.Buffer

	// Capture stdout into a variable
	io.Copy(&buf, r)
	r.Close()

	// Capture stderr into a variable
	io.Copy(&buf2, r2)
	r2.Close()

	if execErr != nil {
		t.Fatalf("command %v failed: %v\noutput: %s", args, execErr, buf.String())
	}

	return buf.String(), buf2.String()
}

func fixturesDir() string {
	return filepath.Join("fixtures")
}

// setupFixtureTopic runs `frogo topic create` and `frogo put` using a fixture file
func setupFixtureTopic(t *testing.T, topic string, fixtureFile string, format string) {
	t.Helper()

	runCmd(t, "topic", "create", topic, "--profile", "test")

	path := filepath.Join(fixturesDir(), fixtureFile)
	runCmd(t, "put", topic, "--file", path, "--format", format, "--profile", "test")

	t.Cleanup(func() {
		runCmd(t, "topic", "delete", topic, "--profile", "test")
	})
}

// ─────────────────────────── TOPIC MANAGEMENT ───────────────────────────

func TestMockCLI_CreateAndDeleteTopic(t *testing.T) {
	topic := "test-create-delete"

	_, stdErr := runCmd(t, "topic", "create", topic, "--profile", "test")

	if !strings.Contains(stdErr, topic) {
		t.Errorf("create-topic output should mention topic name, got: %s", stdErr)
	}

	_, stdErr = runCmd(t, "topic", "delete", topic, "--profile", "test")
	if !strings.Contains(stdErr, topic) {
		t.Errorf("delete-topic output should mention topic name, got: %s", stdErr)
	}
}

// ─────────────────────────── PUT FORMATS ───────────────────────────

func TestMockCLI_PutUTF8(t *testing.T) {
	topic := "test-put-utf8"
	runCmd(t, "topic", "create", topic, "--profile", "test")
	t.Cleanup(func() {
		runCmd(t, "topic", "delete", topic, "--profile", "test")
	})

	path := filepath.Join(fixturesDir(), "put-utf8.txt")
	runCmd(t, "put", topic, "--file", path, "--format", "utf8", "--profile", "test")

	// --to well past end; high watermark check stops consumption after all 5 messages
	out, _ := runCmd(t, "get", topic, "--from", "offset/0", "--to", "offset/99", "--profile", "test")
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

func TestMockCLI_PutBase64(t *testing.T) {
	topic := "test-put-base64"
	runCmd(t, "topic", "create", topic, "--profile", "test")
	t.Cleanup(func() {
		runCmd(t, "topic", "delete", topic, "--profile", "test")
	})

	path := filepath.Join(fixturesDir(), "put-base64.txt")
	runCmd(t, "put", topic, "--file", path, "--format", "base64", "--profile", "test")

	out, _ := runCmd(t, "get", topic, "--from", "offset/0", "--to", "offset/99", "--profile", "test")
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

func TestMockCLI_PutRecordJSON(t *testing.T) {
	topic := "test-put-record-json"
	runCmd(t, "topic", "create", topic, "--profile", "test")
	t.Cleanup(func() {
		runCmd(t, "topic", "delete", topic, "--profile", "test")
	})

	path := filepath.Join(fixturesDir(), "put-record-json.txt")
	runCmd(t, "put", topic, "--file", path, "--format", "record-json", "--profile", "test")

	out, _ := runCmd(t, "get", topic, "--from", "offset/0", "--to", "offset/99", "--profile", "test")
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

// ─────────────────────────── GET WITH INDEX ───────────────────────────

func TestMockCLI_GetIndex(t *testing.T) {
	topic := "test-get-index"
	setupFixtureTopic(t, topic, "get-from-offset-to-offset.txt", "utf8")

	// index/0 to index/-2: should get first 4 messages (offsets 0-3), stopping before the last
	out, _ := runCmd(t, "get", topic, "--from", "index/0", "--to", "index/-2", "--profile", "test")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected 4 lines for index/0..index/-2, got %d: %s", len(lines), out)
	}
	if !strings.Contains(out, "msg-zero") {
		t.Errorf("expected 'msg-zero' in output, got: %s", out)
	}
	if strings.Contains(out, "msg-four") {
		t.Errorf("should not contain 'msg-four' for index/-2, got: %s", out)
	}

	// index/-3 to index/-1: should get last 3 messages (offsets 2-4)
	out, _ = runCmd(t, "get", topic, "--from", "index/-3", "--to", "index/-1", "--profile", "test")
	lines = strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines for index/-3..index/-1, got %d: %s", len(lines), out)
	}
	if !strings.Contains(out, "msg-two") {
		t.Errorf("expected 'msg-two' in output, got: %s", out)
	}
	if !strings.Contains(out, "msg-four") {
		t.Errorf("expected 'msg-four' in output, got: %s", out)
	}
	if strings.Contains(out, "msg-one") {
		t.Errorf("should not contain 'msg-one' for index/-3, got: %s", out)
	}
}

// ─────────────────────────── GET WITH OFFSETS ───────────────────────────

func TestMockCLI_GetFromOffsetToOffset(t *testing.T) {
	topic := "test-get-offsets"
	setupFixtureTopic(t, topic, "get-from-offset-to-offset.txt", "utf8")

	// --to well past end; high watermark stops consumption after offsets 1-4
	out, _ := runCmd(t, "get", topic, "--from", "offset/1", "--to", "offset/99", "--profile", "test")

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

func TestMockCLI_GetUnixTimestamp(t *testing.T) {
	topic := "test-get-unix"
	setupFixtureTopic(t, topic, "get-from-offset-to-offset.txt", "utf8")

	// Use offset for --from (kfake may not support AfterMilli seeking),
	// and a future unix timestamp for --to so all records are before the cutoff
	future := strconv.FormatInt(time.Now().Add(1*time.Minute).Unix(), 10)
	out, _ := runCmd(t, "get", topic, "--from", "offset/0", "--to", "unix/"+future, "--profile", "test")

	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 5 {
		t.Fatalf("expected 5 lines, got %d: %s", len(lines), out)
	}

	if !strings.Contains(out, "msg-zero") {
		t.Errorf("expected 'msg-zero' in get output, got: %s", out)
	}
	if !strings.Contains(out, "msg-four") {
		t.Errorf("expected 'msg-four' in get output, got: %s", out)
	}
}

// ─────────────────────────── GET WITH ALIAS ───────────────────────────

func TestMockCLI_GetAlias(t *testing.T) {
	topic := "test-get-alias"
	setupFixtureTopic(t, topic, "get-from-offset-to-offset.txt", "utf8")

	out, _ := runCmd(t, "get", topic, "--from", "START", "--to", "END", "--profile", "test")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 5 {
		t.Fatalf("expected 5 lines for alias/START..alias/END, got %d: %s", len(lines), out)
	}
	if !strings.Contains(out, "msg-zero") {
		t.Errorf("expected 'msg-zero' in output, got: %s", out)
	}
	if !strings.Contains(out, "msg-four") {
		t.Errorf("expected 'msg-four' in output, got: %s", out)
	}
}
