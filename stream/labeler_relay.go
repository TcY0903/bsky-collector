package stream

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const LabelerRelayURL = "wss://relay.labeler.community/xrpc/community.labeler.sync.subscribeLabelers"

// LabelerRelayClient manages the WebSocket connection to Labeler Relay.
type LabelerRelayClient struct {
	logger   *log.Logger
	conn     *websocket.Conn
	mu       sync.Mutex
	running  bool
	eventCh  chan []byte
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewLabelerRelayClient creates a new LabelerRelayClient.
func NewLabelerRelayClient(logger *log.Logger) *LabelerRelayClient {
	return &LabelerRelayClient{
		logger:  logger,
		eventCh: make(chan []byte, 10000),
		stopCh:  make(chan struct{}),
	}
}

// StartWithRetry connects to Labeler Relay with automatic reconnection.
func (c *LabelerRelayClient) StartWithRetry(ctx context.Context) {
	backoff := 1 * time.Second
	maxBackoff := 60 * time.Second

	for {
		select {
		case <-c.stopCh:
			c.logger.Println("Labeler Relay client stopped")
			return
		case <-ctx.Done():
			c.logger.Println("Context cancelled, stopping Labeler Relay client")
			return
		default:
		}

		c.logger.Println("Connecting to Labeler Relay...")
		err := c.startOnce(ctx)

		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		default:
		}

		if err != nil {
			c.logger.Printf("Warning: Labeler Relay connection lost: %v", err)
			c.logger.Printf("Reconnecting in %v...", backoff)
			select {
			case <-time.After(backoff):
				backoff = minDuration(backoff*2, maxBackoff)
			case <-c.stopCh:
				return
			case <-ctx.Done():
				return
			}
		} else {
			backoff = 1 * time.Second
			select {
			case <-time.After(1 * time.Second):
			case <-c.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}
}

func (c *LabelerRelayClient) startOnce(ctx context.Context) error {
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
	conn, _, err := dialer.DialContext(ctx, LabelerRelayURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to Labeler Relay: %w", err)
	}
	c.conn = conn
	c.logger.Printf("Connected to Labeler Relay")

	for {
		select {
		case <-c.stopCh:
			return nil
		case <-ctx.Done():
			return nil
		default:
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			c.logger.Printf("Labeler Relay stream ended: %v", err)
			return err
		}

		select {
		case c.eventCh <- message:
		default:
			c.logger.Printf("Warning: Labeler Relay event channel full, dropping message")
		}
	}
}

// Events returns the channel for receiving raw label stream messages.
func (c *LabelerRelayClient) Events() <-chan []byte {
	return c.eventCh
}

// Close shuts down the client gracefully.
func (c *LabelerRelayClient) Close() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.conn != nil {
			c.logger.Println("Closing Labeler Relay connection...")
			c.conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		}
	})
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
