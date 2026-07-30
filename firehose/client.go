package firehose

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/gorilla/websocket"

	"bsky-collector/models"
)

// Client manages the WebSocket connection to the Firehose.
type Client struct {
	url     string
	logger  *log.Logger
	conn    *websocket.Conn
	mu      sync.Mutex
	running bool
	postCh  chan *models.Post
	errorCh chan error
}

// NewClient creates a new Firehose client.
func NewClient(url string, logger *log.Logger) *Client {
	return &Client{
		url:     url,
		logger:  logger,
		postCh:  make(chan *models.Post, 10000),
		errorCh: make(chan error, 10),
	}
}

// Start establishes the connection and begins processing the Firehose stream.
func (c *Client) Start(ctx context.Context) error {
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
	conn, _, err := dialer.DialContext(ctx, c.url, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to Firehose: %w", err)
	}
	c.conn = conn
	c.logger.Printf("✅ Connected to Firehose")

	slogLogger := slog.New(slog.NewTextHandler(log.Writer(), &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	handler := &eventHandler{
		client: c,
		logger: c.logger,
	}

	sched := parallel.NewScheduler(10, 100, "bsky-collector", handler.HandleEvent)

	err = events.HandleRepoStream(ctx, conn, sched, slogLogger)
	if err != nil {
		c.logger.Printf("Firehose stream ended: %v", err)
		return err
	}

	return nil
}

// eventHandler implements the event handling logic.
type eventHandler struct {
	client *Client
	logger *log.Logger
}

// HandleEvent processes each event from the Firehose.
func (h *eventHandler) HandleEvent(ctx context.Context, evt *events.XRPCStreamEvent) error {
	if evt.RepoCommit == nil {
		return nil
	}
	h.client.processCommitEvent(evt.RepoCommit)
	return nil
}

// processCommitEvent only processes post creations.
func (c *Client) processCommitEvent(evt *atproto.SyncSubscribeRepos_Commit) {
	for _, op := range evt.Ops {
		// Only process post creation events
		if op.Action != "create" {
			continue
		}
		if !strings.HasPrefix(op.Path, "app.bsky.feed.post/") {
			continue
		}
		if op.Path == "" {
			continue
		}

		// Extract rkey
		parts := strings.Split(op.Path, "/")
		if len(parts) != 2 {
			continue
		}
		rkey := parts[1]

		// Extract CID
		cidStr := ""
		if op.Cid != nil {
			cidStr = op.Cid.String()
		}

		post := &models.Post{
			URI:        fmt.Sprintf("at://%s/%s", evt.Repo, op.Path),
			DID:        evt.Repo,
			RKey:       rkey,
			Cid:        cidStr,
			ReceivedAt: time.Now(),
		}

		// Store blocks as base64 for later parsing
		blocksBase64 := base64.StdEncoding.EncodeToString(evt.Blocks)

		ops := []map[string]string{}
		for _, o := range evt.Ops {
			o_cid := ""
			if o.Cid != nil {
				o_cid = o.Cid.String()
			}
			ops = append(ops, map[string]string{
				"action": o.Action,
				"path":   o.Path,
				"cid":    o_cid,
			})
		}

		payloadData := map[string]interface{}{
			"repo":          evt.Repo,
			"rev":           evt.Rev,
			"seq":           evt.Seq,
			"time":          evt.Time,
			"ops":           ops,
			"blocks_base64": blocksBase64,
		}

		if payload, err := json.Marshal(payloadData); err == nil {
			post.Payload = payload
		}

		select {
		case c.postCh <- post:
		default:
			c.logger.Printf("⚠️ Post channel full, dropping post: %s", op.Path)
		}
	}
}

// Posts returns the channel for receiving processed posts.
func (c *Client) Posts() <-chan *models.Post {
	return c.postCh
}

// Errors returns the channel for receiving errors.
func (c *Client) Errors() <-chan error {
	return c.errorCh
}

// Close shuts down the client gracefully.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.logger.Println("Closing Firehose connection...")
		return c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	}
	return nil
}
