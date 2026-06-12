package cmd

import (
	"fmt"
	"strings"

	"github.com/dustinkirkland/golang-petname"
	"github.com/spf13/cobra"
)

var topicDemoCmd = &cobra.Command{
	Use:   "demo <scenario>",
	Short: "Create a ready-to-go topic from a predefined template",
	Long: `Available scenarios:
  hello-world                    Simple text messages
  basic-incremental              Numbered sequential messages
  basic-json                     JSON purchase/refund events
  10k-pets-3-partitions-json     10,000 JSON pet records across 3 partitions`,
	ValidArgs: []string{"hello-world", "basic-incremental", "basic-json", "10k-pets-3-partitions-json"},
	Args:      cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	RunE:      runDemoTopic,
}

func init() {
	topicCmd.AddCommand(topicDemoCmd)
}

func runDemoTopic(cmd *cobra.Command, args []string) error {
	scenario := args[0]

	// Topic naming convention: frdemo-<scenario>
	switch scenario {
	case "hello-world":
		createTopic("frdemo-hello-world", 1)

		reader := strings.NewReader(helloWorldText())
		err := putRecordsWithReader(reader, "frdemo-hello-world", "utf8")
		if err != nil {
			return err
		}

		return nil

	case "basic-incremental":
		createTopic("frdemo-basic-incremental", 1)

		reader := strings.NewReader(basicIncrementalText())
		err := putRecordsWithReader(reader, "frdemo-basic-incremental", "utf8")
		if err != nil {
			return err
		}

		return nil

	case "basic-json":
		createTopic("frdemo-basic-json", 1)

		reader := strings.NewReader(basicJsonText())
		err := putRecordsWithReader(reader, "frdemo-basic-json", "utf8")
		if err != nil {
			return err
		}

		return nil

	case "10k-pets-3-partitions-json":
		createTopic("frdemo-10k-pets-3-partitions-json", 3)

		text := ""

		// Generate 10k json pet messages
		for i := 0; i < 10000; i++ {

			// Generate random name, Ex: quickly-scornful-johnathan
			petName := petname.Generate(3, "-")
			petNameLower := strings.ToLower(petName)

			jsonMessage := fmt.Sprintf(`{"id": "%v", "name": "%v"}`, i, petNameLower)

			if i > 0 {
				text += "\n"
			}

			text += jsonMessage
		}

		reader := strings.NewReader(text)
		err := putRecordsWithReader(reader, "frdemo-10k-pets-3-partitions-json", "utf8")
		if err != nil {
			return err
		}

	}

	return nil
}

func basicJsonText() string {
	text := `{"id":1,"user":"alice","action":"purchase","amount":29.99,"ts":"2026-06-07T08:00:01Z"}
  {"id":2,"user":"bob","action":"refund","amount":15.00,"ts":"2026-06-07T08:00:14Z"}
  {"id":3,"user":"carol","action":"purchase","amount":5.49,"ts":"2026-06-07T08:00:28Z"}
  {"id":4,"user":"dave","action":"purchase","amount":99.00,"ts":"2026-06-07T08:00:45Z"}
  {"id":5,"user":"alice","action":"purchase","amount":12.75,"ts":"2026-06-07T08:01:03Z"}
  {"id":6,"user":"eve","action":"refund","amount":99.00,"ts":"2026-06-07T08:01:20Z"}
  {"id":7,"user":"bob","action":"purchase","amount":3.25,"ts":"2026-06-07T08:01:38Z"}
  {"id":8,"user":"frank","action":"purchase","amount":55.00,"ts":"2026-06-07T08:01:55Z"}
  {"id":9,"user":"carol","action":"purchase","amount":8.99,"ts":"2026-06-07T08:02:10Z"}
  {"id":10,"user":"dave","action":"refund","amount":5.49,"ts":"2026-06-07T08:02:27Z"}
  {"id":11,"user":"grace","action":"purchase","amount":22.50,"ts":"2026-06-07T08:02:44Z"}
  {"id":12,"user":"alice","action":"refund","amount":12.75,"ts":"2026-06-07T08:03:01Z"}
  {"id":13,"user":"henry","action":"purchase","amount":7.00,"ts":"2026-06-07T08:03:18Z"}
  {"id":14,"user":"eve","action":"purchase","amount":41.00,"ts":"2026-06-07T08:03:35Z"}
  {"id":15,"user":"bob","action":"purchase","amount":18.49,"ts":"2026-06-07T08:03:52Z"}
  {"id":16,"user":"frank","action":"refund","amount":55.00,"ts":"2026-06-07T08:04:09Z"}
  {"id":17,"user":"grace","action":"purchase","amount":9.99,"ts":"2026-06-07T08:04:26Z"}
  {"id":18,"user":"henry","action":"purchase","amount":63.00,"ts":"2026-06-07T08:04:43Z"}
  {"id":19,"user":"carol","action":"refund","amount":8.99,"ts":"2026-06-07T08:05:00Z"}
  {"id":20,"user":"dave","action":"purchase","amount":34.00,"ts":"2026-06-07T08:05:17Z"}
  {"id":21,"user":"alice","action":"purchase","amount":77.25,"ts":"2026-06-07T08:05:34Z"}
  {"id":22,"user":"eve","action":"purchase","amount":2.99,"ts":"2026-06-07T08:05:51Z"}
  {"id":23,"user":"grace","action":"refund","amount":22.50,"ts":"2026-06-07T08:06:08Z"}
  {"id":24,"user":"bob","action":"purchase","amount":14.00,"ts":"2026-06-07T08:06:25Z"}
  {"id":25,"user":"henry","action":"refund","amount":7.00,"ts":"2026-06-07T08:06:42Z"}
  {"id":26,"user":"frank","action":"purchase","amount":88.00,"ts":"2026-06-07T08:06:59Z"}
  {"id":27,"user":"dave","action":"refund","action":"refund","amount":34.00,"ts":"2026-06-07T08:07:16Z"}
  {"id":28,"user":"carol","action":"purchase","amount":19.99,"ts":"2026-06-07T08:07:33Z"}
  {"id":29,"user":"alice","action":"purchase","amount":6.50,"ts":"2026-06-07T08:07:50Z"}
  {"id":30,"user":"eve","action":"refund","amount":41.00,"ts":"2026-06-07T08:08:07Z"}`

	return text

}

func helloWorldText() string {
	text := `hello
world
from
frogo!`
	return text
}

func basicIncrementalText() string {
	text := `msg-zero
msg-one
msg-two
msg-three
msg-four`
	return text
}
