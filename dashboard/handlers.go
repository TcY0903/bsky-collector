package main

import (
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"bsky-collector/models"
	"bsky-collector/storage"
)

// Handler holds dependencies for HTTP handlers.
type Handler struct {
	db *storage.DB
}

// --- Custom Template Functions ---
func parsePagination(r *http.Request) (int, int) {
	limit := 50
	offset := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	return limit, offset
}

// parseIntPtr parses a string to *int, returning nil if empty or invalid.
func parseIntPtr(s string) *int {
	if s == "" {
		return nil
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return nil
	}
	return &v
}

// detectURIType determines the type of a given URI (post, actor, or unknown).
func detectURIType(uri string) string {
	if strings.HasPrefix(uri, "at://") && strings.Contains(uri, "/app.bsky.feed.post/") {
		return "post"
	}
	if strings.HasPrefix(uri, "did:") {
		return "actor"
	}
	return "unknown"
}

func truncate(s string, length int) string {
	runes := []rune(s)
	if len(runes) <= length {
		return s
	}
	return string(runes[:length]) + "..."
}

func formatTime(t time.Time, layout string) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(layout)
}

func render(w http.ResponseWriter, page string, data interface{}) {
	funcMap := template.FuncMap{
		"truncate":   truncate,
		"formatTime": formatTime,
		"trimPrefix": func(s, prefix string) string {
			return strings.TrimPrefix(s, prefix)
		},
		"deref": func(s *string) string {
			if s == nil {
				return ""
			}
			return *s
		},
	}

	paths := []string{
		"templates/layout.html", "templates/" + page,
		"dashboard/templates/layout.html", "dashboard/templates/" + page,
	}

	var tmpl *template.Template
	var err error

	for i := 0; i < len(paths); i += 2 {
		tmpl, err = template.New("layout").Funcs(funcMap).ParseFiles(paths[i], paths[i+1])
		if err == nil {
			break
		}
	}

	if err != nil {
		log.Printf("Template parse error: %v", err)
		http.Error(w, "Template error", http.StatusInternalServerError)
		return
	}

	if err := tmpl.ExecuteTemplate(w, "layout", data); err != nil {
		log.Printf("Template execute error: %v", err)
	}
}

// ---------------- Overview ----------------
func (h *Handler) Dashboard(w http.ResponseWriter, r *http.Request) {
	stats, err := h.db.GetStats()
	if err != nil {
		log.Printf("GetStats error: %v", err)
		stats = map[string]int64{}
	}
	render(w, "index.html", stats)
}

// ---------------- Posts ----------------
func (h *Handler) PostsPage(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")
	keyword := r.URL.Query().Get("keyword")
	did := r.URL.Query().Get("did")
	startTime := r.URL.Query().Get("start")
	endTime := r.URL.Query().Get("end")
	limit, offset := parsePagination(r)

	posts, err := h.db.SearchPosts(uri, keyword, did, startTime, endTime, limit, offset)
	if err != nil {
		log.Printf("SearchPosts error: %v", err)
		http.Error(w, "Failed to query posts", http.StatusInternalServerError)
		return
	}
	render(w, "posts.html", map[string]interface{}{
		"Posts":     posts,
		"URI":       uri,
		"Keyword":   keyword,
		"DID":       did,
		"StartTime": startTime,
		"EndTime":   endTime,
		"Limit":     limit,
		"Offset":    offset,
		"Next":      offset + limit,
		"Prev":      offset - limit,
	})
}

func (h *Handler) PostDetailPage(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")
	if uri == "" {
		http.Error(w, "Missing URI parameter", http.StatusBadRequest)
		return
	}

	post, err := h.db.GetPostByURI(uri)
	if err != nil {
		log.Printf("GetPostByURI error: %v", err)
		http.Error(w, "Failed to query post", http.StatusInternalServerError)
		return
	}
	if post == nil {
		http.Error(w, "Post not found", http.StatusNotFound)
		return
	}

	actor, _ := h.db.GetActor(post.DID)

	var media *models.PostMedia
	if len(post.Media) > 0 {
		if err := json.Unmarshal(post.Media, &media); err != nil {
			log.Printf("Failed to parse media JSON: %v", err)
		}
	}

	render(w, "post_detail.html", map[string]interface{}{
		"Post":  post,
		"Actor": actor,
		"Media": media,
	})
}

// ---------------- Post Histories ----------------
func (h *Handler) PostHistoryPage(w http.ResponseWriter, r *http.Request) {
	postURI := r.URL.Query().Get("post_uri")
	limit, offset := parsePagination(r)

	histories, err := h.db.SearchPostHistory(postURI, limit, offset)
	if err != nil {
		log.Printf("SearchPostHistory error: %v", err)
		http.Error(w, "Failed to query post history", http.StatusInternalServerError)
		return
	}
	render(w, "post_histories.html", map[string]interface{}{
		"Histories": histories,
		"PostURI":   postURI,
		"Limit":     limit,
		"Offset":    offset,
		"Next":      offset + limit,
		"Prev":      offset - limit,
	})
}

func (h *Handler) PostHistoryDetailPage(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Query().Get("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid ID parameter", http.StatusBadRequest)
		return
	}

	history, err := h.db.GetPostHistoryByID(id)
	if err != nil {
		log.Printf("GetPostHistoryByID error: %v", err)
		http.Error(w, "Failed to query history", http.StatusInternalServerError)
		return
	}
	if history == nil {
		http.Error(w, "History record not found", http.StatusNotFound)
		return
	}

	var media *models.PostMedia
	if len(history.Media) > 0 {
		if err := json.Unmarshal(history.Media, &media); err != nil {
			log.Printf("Failed to parse media JSON: %v", err)
		}
	}

	// Extract post DID from PostURI
	var postDID string
	parts := strings.Split(history.PostURI, "/")
	if len(parts) >= 3 {
		postDID = parts[2]
	}

	render(w, "post_history_detail.html", map[string]interface{}{
		"History": history,
		"Media":   media,
		"PostDID": postDID,
	})
}

// ---------------- Actors ----------------
func (h *Handler) ActorsPage(w http.ResponseWriter, r *http.Request) {
	did := r.URL.Query().Get("did")
	keyword := r.URL.Query().Get("keyword")
	limit, offset := parsePagination(r)

	minFollowers := parseIntPtr(r.URL.Query().Get("min_followers"))
	maxFollowers := parseIntPtr(r.URL.Query().Get("max_followers"))
	minFollows := parseIntPtr(r.URL.Query().Get("min_follows"))
	maxFollows := parseIntPtr(r.URL.Query().Get("max_follows"))
	minPosts := parseIntPtr(r.URL.Query().Get("min_posts"))
	maxPosts := parseIntPtr(r.URL.Query().Get("max_posts"))

	// status is always empty since the backend only processes active actors
	actors, err := h.db.SearchActors(
		did, keyword, "",
		minFollowers, maxFollowers,
		minFollows, maxFollows,
		minPosts, maxPosts,
		limit, offset,
	)
	if err != nil {
		log.Printf("SearchActors error: %v", err)
		http.Error(w, "Failed to query actors", http.StatusInternalServerError)
		return
	}

	render(w, "actors.html", map[string]interface{}{
		"Actors":       actors,
		"DID":          did,
		"Keyword":      keyword,
		"MinFollowers": r.URL.Query().Get("min_followers"),
		"MaxFollowers": r.URL.Query().Get("max_followers"),
		"MinFollows":   r.URL.Query().Get("min_follows"),
		"MaxFollows":   r.URL.Query().Get("max_follows"),
		"MinPosts":     r.URL.Query().Get("min_posts"),
		"MaxPosts":     r.URL.Query().Get("max_posts"),
		"Limit":        limit,
		"Offset":       offset,
		"Next":         offset + limit,
		"Prev":         offset - limit,
	})
}

func (h *Handler) ActorDetailPage(w http.ResponseWriter, r *http.Request) {
	did := r.URL.Query().Get("did")
	if did == "" {
		http.Error(w, "Missing DID parameter", http.StatusBadRequest)
		return
	}

	actor, err := h.db.GetActor(did)
	if err != nil {
		log.Printf("GetActor error: %v", err)
		http.Error(w, "Failed to query actor", http.StatusInternalServerError)
		return
	}
	if actor == nil {
		http.Error(w, "Actor not found", http.StatusNotFound)
		return
	}

	render(w, "actor_detail.html", actor)
}

// ---------------- Blocks ----------------
func (h *Handler) BlocksPage(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")
	did := r.URL.Query().Get("did")
	subjectDID := r.URL.Query().Get("subject_did")
	// status is kept for backward compatibility but not exposed in the UI
	status := r.URL.Query().Get("status")
	limit, offset := parsePagination(r)

	blocks, err := h.db.SearchBlocks(uri, did, subjectDID, status, limit, offset)
	if err != nil {
		log.Printf("SearchBlocks error: %v", err)
		http.Error(w, "Failed to query blocks", http.StatusInternalServerError)
		return
	}
	render(w, "blocks.html", map[string]interface{}{
		"Blocks":     blocks,
		"URI":        uri,
		"DID":        did,
		"SubjectDID": subjectDID,
		"Status":     status,
		"Limit":      limit,
		"Offset":     offset,
		"Next":       offset + limit,
		"Prev":       offset - limit,
	})
}

func (h *Handler) BlockDetailPage(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")
	if uri == "" {
		http.Error(w, "Missing URI parameter", http.StatusBadRequest)
		return
	}

	block, err := h.db.GetBlockByURI(uri)
	if err != nil {
		log.Printf("GetBlockByURI error: %v", err)
		http.Error(w, "Failed to query block", http.StatusInternalServerError)
		return
	}
	if block == nil {
		http.Error(w, "Block not found", http.StatusNotFound)
		return
	}

	// Fetch blocker and blocked actors for context
	blocker, _ := h.db.GetActor(block.DID)
	blocked, _ := h.db.GetActor(block.SubjectDID)

	render(w, "block_detail.html", map[string]interface{}{
		"Block":   block,
		"Blocker": blocker,
		"Blocked": blocked,
	})
}

// ---------------- Labels ----------------
func (h *Handler) LabelsPage(w http.ResponseWriter, r *http.Request) {
	val := r.URL.Query().Get("val")
	src := r.URL.Query().Get("src")
	uri := r.URL.Query().Get("uri")

	var neg *bool
	if v := r.URL.Query().Get("neg"); v == "true" || v == "false" {
		b := v == "true"
		neg = &b
	}

	limit, offset := parsePagination(r)

	labels, err := h.db.SearchLabels(val, src, uri, neg, limit, offset)
	if err != nil {
		log.Printf("SearchLabels error: %v", err)
		http.Error(w, "Failed to query labels", http.StatusInternalServerError)
		return
	}
	render(w, "labels.html", map[string]interface{}{
		"Labels": labels,
		"Val":    val,
		"Src":    src,
		"URI":    uri,
		"Neg":    r.URL.Query().Get("neg"),
		"Limit":  limit,
		"Offset": offset,
		"Next":   offset + limit,
		"Prev":   offset - limit,
	})
}

func (h *Handler) LabelDetailPage(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Query().Get("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid ID parameter", http.StatusBadRequest)
		return
	}

	label, err := h.db.GetLabelByID(id)
	if err != nil {
		log.Printf("GetLabelByID error: %v", err)
		http.Error(w, "Failed to query label", http.StatusInternalServerError)
		return
	}
	if label == nil {
		http.Error(w, "Label not found", http.StatusNotFound)
		return
	}

	render(w, "label_detail.html", map[string]interface{}{
		"Label":      label,
		"TargetType": detectURIType(label.URI),
	})
}

// ---------------- Labelers ----------------
func (h *Handler) LabelersPage(w http.ResponseWriter, r *http.Request) {
	did := r.URL.Query().Get("did")
	limit, offset := parsePagination(r)

	labelers, err := h.db.SearchLabelers(did, limit, offset)
	if err != nil {
		log.Printf("SearchLabelers error: %v", err)
		http.Error(w, "Failed to query labelers", http.StatusInternalServerError)
		return
	}
	render(w, "labelers.html", map[string]interface{}{
		"Labelers": labelers,
		"DID":      did,
		"Limit":    limit,
		"Offset":   offset,
		"Next":     offset + limit,
		"Prev":     offset - limit,
	})
}

func (h *Handler) LabelerDetailPage(w http.ResponseWriter, r *http.Request) {
	did := r.URL.Query().Get("did")
	if did == "" {
		http.Error(w, "Missing DID parameter", http.StatusBadRequest)
		return
	}

	labeler, err := h.db.GetLabelerByDID(did)
	if err != nil {
		log.Printf("GetLabelerByDID error: %v", err)
		http.Error(w, "Failed to query labeler", http.StatusInternalServerError)
		return
	}
	if labeler == nil {
		http.Error(w, "Labeler not found", http.StatusNotFound)
		return
	}

	render(w, "labeler_detail.html", labeler)
}

// ---------------- Lists ----------------
func (h *Handler) ListsPage(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")
	did := r.URL.Query().Get("did")
	keyword := r.URL.Query().Get("name")
	limit, offset := parsePagination(r)

	lists, err := h.db.SearchLists(uri, did, keyword, limit, offset)
	if err != nil {
		log.Printf("SearchLists error: %v", err)
		http.Error(w, "Failed to query lists", http.StatusInternalServerError)
		return
	}
	render(w, "lists.html", map[string]interface{}{
		"Lists":   lists,
		"URI":     uri,
		"DID":     did,
		"Keyword": keyword,
		"Limit":   limit,
		"Offset":  offset,
		"Next":    offset + limit,
		"Prev":    offset - limit,
	})
}

func (h *Handler) ListDetailPage(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")
	if uri == "" {
		http.Error(w, "Missing URI parameter", http.StatusBadRequest)
		return
	}

	list, err := h.db.GetListByURI(uri)
	if err != nil {
		log.Printf("GetListByURI error: %v", err)
		http.Error(w, "Failed to query list", http.StatusInternalServerError)
		return
	}
	if list == nil {
		http.Error(w, "List not found", http.StatusNotFound)
		return
	}

	render(w, "list_detail.html", list)
}

// ---------------- List Items ----------------
func (h *Handler) ListItemsPage(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")
	listURI := r.URL.Query().Get("list_uri")
	did := r.URL.Query().Get("did")
	subjectDID := r.URL.Query().Get("subject_did")
	limit, offset := parsePagination(r)

	items, err := h.db.SearchListItems(uri, listURI, did, subjectDID, limit, offset)
	if err != nil {
		log.Printf("SearchListItems error: %v", err)
		http.Error(w, "Failed to query list items", http.StatusInternalServerError)
		return
	}
	render(w, "list_items.html", map[string]interface{}{
		"Items":      items,
		"URI":        uri,
		"ListURI":    listURI,
		"DID":        did,
		"SubjectDID": subjectDID,
		"Limit":      limit,
		"Offset":     offset,
		"Next":       offset + limit,
		"Prev":       offset - limit,
	})
}

func (h *Handler) ListItemDetailPage(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")
	if uri == "" {
		http.Error(w, "Missing URI parameter", http.StatusBadRequest)
		return
	}

	item, err := h.db.GetListItemByURI(uri)
	if err != nil {
		log.Printf("GetListItemByURI error: %v", err)
		http.Error(w, "Failed to query list item", http.StatusInternalServerError)
		return
	}
	if item == nil {
		http.Error(w, "List item not found", http.StatusNotFound)
		return
	}

	render(w, "list_item_detail.html", item)
}

// ---------------- List Blocks ----------------
func (h *Handler) ListBlocksPage(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")
	subjectList := r.URL.Query().Get("subject_list")
	did := r.URL.Query().Get("did")
	limit, offset := parsePagination(r)

	blocks, err := h.db.SearchListBlocks(uri, subjectList, did, limit, offset)
	if err != nil {
		log.Printf("SearchListBlocks error: %v", err)
		http.Error(w, "Failed to query list blocks", http.StatusInternalServerError)
		return
	}
	render(w, "list_blocks.html", map[string]interface{}{
		"Blocks":      blocks,
		"URI":         uri,
		"SubjectList": subjectList,
		"DID":         did,
		"Limit":       limit,
		"Offset":      offset,
		"Next":        offset + limit,
		"Prev":        offset - limit,
	})
}

func (h *Handler) ListBlockDetailPage(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")
	if uri == "" {
		http.Error(w, "Missing URI parameter", http.StatusBadRequest)
		return
	}

	block, err := h.db.GetListBlockByURI(uri)
	if err != nil {
		log.Printf("GetListBlockByURI error: %v", err)
		http.Error(w, "Failed to query list block", http.StatusInternalServerError)
		return
	}
	if block == nil {
		http.Error(w, "List block not found", http.StatusNotFound)
		return
	}

	blocker, _ := h.db.GetActor(block.DID)

	render(w, "list_block_detail.html", map[string]interface{}{
		"Block":   block,
		"Blocker": blocker,
	})
}

// ---------------- Conversation Thread ----------------
func (h *Handler) ThreadPage(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")
	if uri == "" {
		http.Error(w, "Missing URI parameter", http.StatusBadRequest)
		return
	}

	posts, err := h.db.GetConversation(uri)
	if err != nil {
		log.Printf("GetConversation error: %v", err)
		http.Error(w, "Failed to load conversation", http.StatusInternalServerError)
		return
	}
	if len(posts) == 0 {
		http.Error(w, "Conversation not found", http.StatusNotFound)
		return
	}

	// Build map uri -> depth (how many levels below root)
	depth := make(map[string]int)
	postByURI := make(map[string]*models.Post)
	for _, p := range posts {
		postByURI[p.URI] = p
	}
	// Root is the post with no ReplyParent (or the deepest ancestor)
	var rootURI string
	for _, p := range posts {
		if p.ReplyParent == "" {
			rootURI = p.URI
			break
		}
	}
	if rootURI == "" {
		rootURI = posts[0].URI
	}
	depth[rootURI] = 0
	// BFS over reply_parent relations
	changed := true
	for changed {
		changed = false
		for _, p := range posts {
			if p.ReplyParent != "" {
				if d, ok := depth[p.ReplyParent]; ok {
					if _, seen := depth[p.URI]; !seen {
						depth[p.URI] = d + 1
						changed = true
					}
				}
			}
		}
	}

	// Enrich posts with media, labels, depth, is_target
	type postEnriched struct {
		Post        *models.Post
		Media       *models.PostMedia
		Labels      []*models.Label
		IndentClass string // e.g. "ml-0", "ml-6", "ml-12", ...
		IsTarget    bool
	}
	var enriched []postEnriched
	for _, p := range posts {
		var media *models.PostMedia
		if len(p.Media) > 0 {
			_ = json.Unmarshal(p.Media, &media)
		}

		labels, err := h.db.GetLabelsByURI(p.URI)
		if err != nil {
			log.Printf("GetLabelsByURI error for %s: %v", p.URI, err)
			labels = nil
		}

		// Map depth to a Tailwind margin-left class (max ~12 levels, then clamp)
		classes := []string{
			"ml-0", "ml-4", "ml-8", "ml-12", "ml-16", "ml-20",
			"ml-24", "ml-28", "ml-32", "ml-36", "ml-40", "ml-44",
		}
		d := depth[p.URI]
		if d >= len(classes) {
			d = len(classes) - 1
		}

		enriched = append(enriched, postEnriched{
			Post:        p,
			Media:       media,
			Labels:      labels,
			IndentClass: classes[d],
			IsTarget:    p.URI == uri,
		})
	}

	render(w, "thread.html", map[string]interface{}{
		"TargetURI": uri,
		"Posts":     enriched,
	})
}
