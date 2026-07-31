package processor

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"time"

	"bsky-collector/models"

	"github.com/fxamacker/cbor/v2"
	"github.com/ipld/go-car/v2"
)

// CommitEvent represents the structure stored in Post.Payload
type CommitEvent struct {
	Repo string `json:"repo"`
	Rev  string `json:"rev"`
	Seq  int64  `json:"seq"`
	Time string `json:"time"`
	Ops  []struct {
		Action string `json:"action"`
		Path   string `json:"path"`
		Cid    string `json:"cid"`
	} `json:"ops"`
	BlocksBase64 string `json:"blocks_base64"`
}

// enrichPosts parses CAR files and extracts post text, createdAt, and reply info.
func (p *Processor) enrichPosts(posts []*models.Post) {
	for _, post := range posts {
		var commit CommitEvent
		if err := json.Unmarshal(post.Payload, &commit); err != nil {
			continue
		}

		if commit.BlocksBase64 == "" {
			continue
		}

		blocksData, err := base64.StdEncoding.DecodeString(commit.BlocksBase64)
		if err != nil {
			continue
		}

		targetCID := post.Cid
		if targetCID == "" {
			continue
		}

		blockReader, err := car.NewBlockReader(bytes.NewReader(blocksData))
		if err != nil {
			continue
		}

		var text string
		var createdAt time.Time
		var replyParent string
		var replyRoot string

		for {
			block, err := blockReader.Next()
			if err != nil {
				break
			}

			if block.Cid().String() != targetCID {
				continue
			}

			var record map[string]interface{}
			if err := cbor.Unmarshal(block.RawData(), &record); err != nil {
				break
			}

			if t, ok := record["text"].(string); ok {
				text = t
			}
			if c, ok := record["createdAt"].(string); ok {
				if t, err := time.Parse(time.RFC3339, c); err == nil {
					createdAt = t
				}
			}

			if replyVal, ok := record["reply"]; ok {
				replyMap := make(map[string]interface{})
				switch v := replyVal.(type) {
				case map[string]interface{}:
					replyMap = v
				case map[interface{}]interface{}:
					for key, val := range v {
						if strKey, ok := key.(string); ok {
							replyMap[strKey] = val
						}
					}
				}

				if parent, ok := replyMap["parent"]; ok {
					parentMap := make(map[string]interface{})
					switch v := parent.(type) {
					case map[string]interface{}:
						parentMap = v
					case map[interface{}]interface{}:
						for key, val := range v {
							if strKey, ok := key.(string); ok {
								parentMap[strKey] = val
							}
						}
					}
					if uri, ok := parentMap["uri"].(string); ok {
						replyParent = uri
					}
				}

				if root, ok := replyMap["root"]; ok {
					rootMap := make(map[string]interface{})
					switch v := root.(type) {
					case map[string]interface{}:
						rootMap = v
					case map[interface{}]interface{}:
						for key, val := range v {
							if strKey, ok := key.(string); ok {
								rootMap[strKey] = val
							}
						}
					}
					if uri, ok := rootMap["uri"].(string); ok {
						replyRoot = uri
					}
				}
			}

			break
		}

		post.Text = text
		post.CreatedAt = createdAt
		post.ReplyParent = replyParent
		post.ReplyRoot = replyRoot
		post.Payload = nil
	}
}
