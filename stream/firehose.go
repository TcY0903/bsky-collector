package stream

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"log/slog"

	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/gorilla/websocket"

	"bsky-collector/config"
)

// ==================== Types ====================

// FirehoseClient manages the WebSocket connection to the Firehose.
type FirehoseClient struct {
	cfg    *config.Config
	logger *log.Logger

	conn     *websocket.Conn
	mu       sync.Mutex
	running  bool
	eventCh  chan *events.XRPCStreamEvent
	errorCh  chan error
	stopCh   chan struct{}
	stopOnce sync.Once
}

// ==================== Lifecycle ====================

// NewFirehoseClient creates a new Firehose client using config.
func NewFirehoseClient(cfg *config.Config, logger *log.Logger) *FirehoseClient {
	eventChannelSize := cfg.FirehoseEventChannelSize
	if eventChannelSize <= 0 {
		eventChannelSize = 10000
	}
	return &FirehoseClient{
		cfg:     cfg,
		logger:  logger,
		eventCh: make(chan *events.XRPCStreamEvent, eventChannelSize),
		errorCh: make(chan error, 10),
		stopCh:  make(chan struct{}),
	}
}

// StartWithRetry connects to Firehose with automatic reconnection.
func (c *FirehoseClient) StartWithRetry(ctx context.Context) {
	backoff := 1 * time.Second
	maxBackoff := 60 * time.Second

	for {
		select {
		case <-c.stopCh:
			c.logger.Println("Firehose client stopped")
			return
		case <-ctx.Done():
			c.logger.Println("Context cancelled, stopping Firehose client")
			return
		default:
		}

		c.logger.Println("Connecting to Firehose...")
		err := c.startOnce(ctx)

		select {
		case <-c.stopCh:
			c.logger.Println("Firehose client stopped during reconnect")
			return
		case <-ctx.Done():
			c.logger.Println("Context cancelled during reconnect")
			return
		default:
		}

		if err != nil {
			c.logger.Printf("WARN: Firehose connection lost: %v", err)
			c.logger.Printf("Reconnecting in %v...", backoff)

			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
			case <-c.stopCh:
				c.logger.Println("Firehose client stopped during backoff")
				return
			case <-ctx.Done():
				c.logger.Println("Context cancelled during backoff")
				return
			}
		} else {
			backoff = 1 * time.Second
			c.logger.Println("Firehose connection ended, reconnecting...")

			select {
			case <-time.After(1 * time.Second):
			case <-c.stopCh:
				c.logger.Println("Firehose client stopped during reconnect delay")
				return
			case <-ctx.Done():
				c.logger.Println("Context cancelled during reconnect delay")
				return
			}
		}
	}
}

// startOnce establishes a single Firehose connection.
func (c *FirehoseClient) startOnce(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return fmt.Errorf("client already running")
	}
	c.running = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
	}()

	dialer := websocket.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, c.cfg.FirehoseURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to Firehose: %w", err)
	}
	c.conn = conn
	c.logger.Printf("Connected to Firehose")

	slogLogger := slog.New(slog.NewTextHandler(log.Writer(), &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	handler := &firehoseEventHandler{
		client: c,
		logger: c.logger,
	}

	workers := c.cfg.FirehoseSchedulerWorkers
	if workers <= 0 {
		workers = 10
	}
	queue := c.cfg.FirehoseSchedulerQueue
	if queue <= 0 {
		queue = 100
	}

	sched := parallel.NewScheduler(workers, queue, "bsky-collector", handler.HandleEvent)

	err = events.HandleRepoStream(ctx, conn, sched, slogLogger)
	if err != nil {
		c.logger.Printf("Firehose stream ended: %v", err)
		return err
	}

	return nil
}

// Close shuts down the client gracefully.
func (c *FirehoseClient) Close() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.conn != nil {
			c.logger.Println("Closing Firehose connection...")
			c.conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		}
	})
}

// ==================== Event Handling ====================

// firehoseEventHandler implements the event handling logic.
type firehoseEventHandler struct {
	client *FirehoseClient
	logger *log.Logger
}

// HandleEvent forwards each event from the Firehose to the event channel.
func (h *firehoseEventHandler) HandleEvent(ctx context.Context, evt *events.XRPCStreamEvent) error {
	select {
	case h.client.eventCh <- evt:
		return nil
	default:
		h.logger.Printf("WARN: event channel full, dropping event")
		return nil
	}
}

// Events returns the channel for receiving raw events.
func (c *FirehoseClient) Events() <-chan *events.XRPCStreamEvent {
	return c.eventCh
}

// Errors returns the channel for receiving errors.
func (c *FirehoseClient) Errors() <-chan error {
	return c.errorCh
}

// QueueDepth returns the number of events currently buffered but not yet consumed.
func (c *FirehoseClient) QueueDepth() int {
	return len(c.eventCh)
}

// QueueCapacity returns the capacity of the event channel.
func (c *FirehoseClient) QueueCapacity() int {
	return cap(c.eventCh)
}
