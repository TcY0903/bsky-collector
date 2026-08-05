package processor

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/fxamacker/cbor/v2"
	"github.com/ipfs/go-cid"

	"bsky-collector/models"
)

// MediaParser handles parsing of Bluesky embed media from CBOR/JSON data
type MediaParser struct {
	logger *log.Logger
}

// NewMediaParser creates a new MediaParser
func NewMediaParser(logger *log.Logger) *MediaParser {
	return &MediaParser{logger: logger}
}

// sanitizeText removes NULL bytes and control characters from text
func sanitizeText(text string) string {
	if text == "" {
		return ""
	}
	text = strings.ReplaceAll(text, "\x00", "")
	var builder strings.Builder
	for _, r := range text {
		if r == '\n' || r == '\t' || r == '\r' {
			builder.WriteRune(r)
		} else if r >= 32 {
			builder.WriteRune(r)
		}
	}
	return builder.String()
}

// ParseEmbed extracts media metadata from the embed field
func (mp *MediaParser) ParseEmbed(embedVal interface{}, did string) []byte {
	if embedVal == nil {
		return nil
	}

	embedMap := toMapStringInterface(embedVal)
	if embedMap == nil {
		return nil
	}

	media := &models.PostMedia{}
	embedType, _ := embedMap["$type"].(string)

	switch {
	case embedType == "app.bsky.embed.images" || embedType == "app.bsky.embed.images#view":
		media.Type = "images"
		media.Images = mp.parseImages(embedMap, did)

	case embedType == "app.bsky.embed.video" || embedType == "app.bsky.embed.video#view":
		media.Type = "video"
		media.Video = mp.parseVideo(embedMap, did)

	case embedType == "app.bsky.embed.external" || embedType == "app.bsky.embed.external#view":
		media.Type = "external"
		media.External = mp.parseExternal(embedMap)

	case embedType == "app.bsky.embed.record" || embedType == "app.bsky.embed.record#view":
		media.Type = "record"
		if recordVal, ok := embedMap["record"]; ok {
			recordMap := toMapStringInterface(recordVal)
			if recordMap != nil {
				media.Record = mp.parseRecord(recordMap)
			}
		}

	case embedType == "app.bsky.embed.recordWithMedia" || embedType == "app.bsky.embed.recordWithMedia#view":
		media.Type = "recordWithMedia"
		if recordVal, ok := embedMap["record"]; ok {
			recordMap := toMapStringInterface(recordVal)
			if recordMap != nil {
				if innerRecord, ok := recordMap["record"]; ok {
					innerRecordMap := toMapStringInterface(innerRecord)
					if innerRecordMap != nil {
						media.Record = mp.parseRecord(innerRecordMap)
					}
				} else {
					media.Record = mp.parseRecord(recordMap)
				}
			}
		}
		if mediaVal, ok := embedMap["media"]; ok {
			mediaMap := toMapStringInterface(mediaVal)
			if mediaMap != nil {
				mp.parseMediaUnion(media, mediaMap, did)
			}
		}

	default:
		// Fallback detection for unknown types (e.g., gallery)
		if _, ok := embedMap["images"]; ok {
			media.Type = "images"
			media.Images = mp.parseImages(embedMap, did)
		} else if _, ok := embedMap["items"]; ok {
			media.Type = "images"
			media.Images = mp.parseImages(embedMap, did)
		} else if _, ok := embedMap["video"]; ok {
			media.Type = "video"
			media.Video = mp.parseVideo(embedMap, did)
		} else if _, ok := embedMap["external"]; ok {
			media.Type = "external"
			media.External = mp.parseExternal(embedMap)
		} else if _, ok := embedMap["record"]; ok {
			media.Type = "record"
			recordMap := toMapStringInterface(embedMap["record"])
			if recordMap != nil {
				media.Record = mp.parseRecord(recordMap)
			}
		} else {
			return nil
		}
	}

	if media.Type == "" || (len(media.Images) == 0 && media.Video == nil && media.External == nil && media.Record == nil) {
		return nil
	}

	data, err := json.Marshal(media)
	if err != nil {
		mp.logger.Printf("⚠️ Failed to marshal media: %v", err)
		return nil
	}

	return data
}

// parseImages extracts image information from the embed
func (mp *MediaParser) parseImages(embedMap map[string]interface{}, did string) []models.ImageInfo {
	imagesVal, ok := embedMap["images"]
	if !ok {
		imagesVal, ok = embedMap["items"]
		if !ok {
			return nil
		}
	}

	imagesList, ok := imagesVal.([]interface{})
	if !ok || len(imagesList) == 0 {
		return nil
	}

	result := make([]models.ImageInfo, 0, len(imagesList))

	for _, img := range imagesList {
		imgMap := toMapStringInterface(img)
		if imgMap == nil {
			continue
		}

		info := models.ImageInfo{}
		var cidStr string

		if imageVal, ok := imgMap["image"]; ok {
			imageMap := toMapStringInterface(imageVal)
			if imageMap != nil {
				if refVal, ok := imageMap["ref"]; ok {
					cidStr = extractCID(refVal)
				}
				if cidStr == "" {
					if c, ok := imageMap["cid"].(string); ok && c != "" {
						cidStr = c
					}
				}
			}
		}

		if cidStr == "" {
			if refVal, ok := imgMap["ref"]; ok {
				cidStr = extractCID(refVal)
			}
		}

		if cidStr == "" {
			if c, ok := imgMap["cid"].(string); ok && c != "" {
				cidStr = c
			}
		}

		if cidStr != "" && len(cidStr) > 5 {
			info.CID = cidStr
			info.URL = fmt.Sprintf("https://cdn.bsky.app/img/feed_thumbnail/plain/%s/%s", did, cidStr)

			if alt, ok := imgMap["alt"].(string); ok && alt != "" {
				info.Alt = sanitizeText(alt)
			} else if imageVal, ok := imgMap["image"]; ok {
				imageMap := toMapStringInterface(imageVal)
				if imageMap != nil {
					if alt, ok := imageMap["alt"].(string); ok && alt != "" {
						info.Alt = sanitizeText(alt)
					}
				}
			}

			result = append(result, info)
		}
	}

	return result
}

// extractCID extracts CID from various ref types
func extractCID(refVal interface{}) string {
	if refVal == nil {
		return ""
	}

	if tag, ok := refVal.(cbor.Tag); ok {
		if content, ok := tag.Content.([]byte); ok && len(content) > 0 {
			if c, err := cid.Cast(content); err == nil {
				return c.String()
			}
			if len(content) > 4 {
				for offset := 0; offset < 4 && offset < len(content); offset++ {
					if c, err := cid.Cast(content[offset:]); err == nil {
						return c.String()
					}
				}
			}
		}
	}

	if content, ok := refVal.([]byte); ok && len(content) > 0 {
		if c, err := cid.Cast(content); err == nil {
			return c.String()
		}
	}

	if str, ok := refVal.(string); ok && str != "" {
		if c, err := cid.Parse(str); err == nil {
			return c.String()
		}
		return str
	}

	refMap := toMapStringInterface(refVal)
	if refMap != nil {
		if link, ok := refMap["$link"].(string); ok && link != "" {
			return link
		}
		if cid, ok := refMap["cid"].(string); ok && cid != "" {
			return cid
		}
	}

	return ""
}

// parseVideo extracts video information from the embed
func (mp *MediaParser) parseVideo(embedMap map[string]interface{}, did string) *models.VideoInfo {
	videoVal, ok := embedMap["video"]
	if !ok {
		return nil
	}

	videoMap := toMapStringInterface(videoVal)
	if videoMap == nil {
		return nil
	}

	info := &models.VideoInfo{}
	var cidStr string

	if refVal, ok := videoMap["ref"]; ok {
		cidStr = extractCID(refVal)
	}

	if cidStr == "" {
		if c, ok := videoMap["cid"].(string); ok && c != "" {
			cidStr = c
		}
	}

	if cidStr != "" && len(cidStr) > 5 {
		info.CID = cidStr
		info.URL = fmt.Sprintf("https://cdn.bsky.app/img/feed_fullsize/plain/%s/%s", did, cidStr)
		return info
	}

	return nil
}

// parseExternal extracts external link card information
func (mp *MediaParser) parseExternal(embedMap map[string]interface{}) *models.ExternalInfo {
	externalVal, ok := embedMap["external"]
	if !ok {
		return nil
	}

	externalMap := toMapStringInterface(externalVal)
	if externalMap == nil {
		return nil
	}

	info := &models.ExternalInfo{}

	if uri, ok := externalMap["uri"].(string); ok {
		info.URI = sanitizeText(uri)
	}
	if title, ok := externalMap["title"].(string); ok {
		info.Title = sanitizeText(title)
	}
	if desc, ok := externalMap["description"].(string); ok {
		info.Description = sanitizeText(desc)
	}

	if thumbVal, ok := externalMap["thumb"]; ok {
		thumbMap := toMapStringInterface(thumbVal)
		if thumbMap != nil {
			if refVal, ok := thumbMap["ref"]; ok {
				cidStr := extractCID(refVal)
				if cidStr != "" {
					info.Thumb = fmt.Sprintf("https://cdn.bsky.app/img/feed_thumbnail/plain/placeholder/%s", cidStr)
				}
			}
			if info.Thumb == "" {
				if cid, ok := thumbMap["cid"].(string); ok && cid != "" {
					info.Thumb = fmt.Sprintf("https://cdn.bsky.app/img/feed_thumbnail/plain/placeholder/%s", cid)
				}
			}
		} else if thumbStr, ok := thumbVal.(string); ok && thumbStr != "" {
			info.Thumb = sanitizeText(thumbStr)
		}
	}

	if info.URI == "" {
		return nil
	}

	return info
}

// parseRecord extracts quoted record information
func (mp *MediaParser) parseRecord(recordMap map[string]interface{}) *models.RecordRef {
	info := &models.RecordRef{}

	if uri, ok := recordMap["uri"].(string); ok && uri != "" {
		info.URI = sanitizeText(uri)
	} else {
		return nil
	}

	if cid, ok := recordMap["cid"].(string); ok && cid != "" {
		info.CID = sanitizeText(cid)
	}

	return info
}

// parseMediaUnion parses the media part of a recordWithMedia embed
func (mp *MediaParser) parseMediaUnion(media *models.PostMedia, mediaMap map[string]interface{}, did string) {
	mediaType, _ := mediaMap["$type"].(string)

	switch {
	case mediaType == "app.bsky.embed.images#view" || mediaType == "app.bsky.embed.images":
		media.Images = mp.parseImages(mediaMap, did)
	case mediaType == "app.bsky.embed.video#view" || mediaType == "app.bsky.embed.video":
		media.Video = mp.parseVideo(mediaMap, did)
	case mediaType == "app.bsky.embed.external#view" || mediaType == "app.bsky.embed.external":
		media.External = mp.parseExternal(mediaMap)
	default:
		if _, ok := mediaMap["images"]; ok {
			media.Images = mp.parseImages(mediaMap, did)
		} else if _, ok := mediaMap["items"]; ok {
			media.Images = mp.parseImages(mediaMap, did)
		} else if _, ok := mediaMap["video"]; ok {
			media.Video = mp.parseVideo(mediaMap, did)
		} else if _, ok := mediaMap["external"]; ok {
			media.External = mp.parseExternal(mediaMap)
		}
	}
}

// toMapStringInterface converts map with interface{} keys to map[string]interface{}
func toMapStringInterface(m interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}

	switch v := m.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for key, val := range v {
			if nestedMap, ok := val.(map[interface{}]interface{}); ok {
				result[key] = toMapStringInterface(nestedMap)
			} else if nestedMap, ok := val.(map[string]interface{}); ok {
				result[key] = toMapStringInterface(nestedMap)
			} else {
				result[key] = val
			}
		}
		return result

	case map[interface{}]interface{}:
		result := make(map[string]interface{})
		for key, val := range v {
			strKey, ok := key.(string)
			if !ok {
				continue
			}
			if nestedMap, ok := val.(map[interface{}]interface{}); ok {
				result[strKey] = toMapStringInterface(nestedMap)
			} else if nestedMap, ok := val.(map[string]interface{}); ok {
				result[strKey] = toMapStringInterface(nestedMap)
			} else {
				result[strKey] = val
			}
		}
		return result

	default:
		return nil
	}
}
