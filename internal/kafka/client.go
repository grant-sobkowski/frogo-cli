package kafka

import (
	"fmt"

	"github.com/grant-sobkowski/frogo-cli/internal/config"
	"github.com/twmb/franz-go/pkg/kgo"
)

func client(profile string) (*kgo.Client, error) {

	opts, err := config.ReadProfile(profile)
	if err != nil {
		return nil, fmt.Errorf("failed to read profile for client config: %w", err)
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("error while instantiating client: %w", err)
	}

	return cl, nil
}
