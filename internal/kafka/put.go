package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Put produces records to a Kafka topic synchronously.
func Put(cl *kgo.Client, topic string, records []*kgo.Record) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Produce messages, getting first erroring result (if any)
	err := cl.ProduceSync(ctx, records...).FirstErr()

	if err != nil {
		return fmt.Errorf("Put failure while calling ProduceSync: %w", err)
	}

	return nil
}
