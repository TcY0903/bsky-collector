package main

import (
	"log"
	"net/http"
	"os"

	"bsky-collector/config"
	"bsky-collector/storage"
)

func main() {
	cfg := config.Load()

	dbURL := cfg.DatabaseURL
	if envURL := os.Getenv("DATABASE_URL"); envURL != "" {
		dbURL = envURL
	}

	db, err := storage.NewDB(dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	h := &Handler{db: db}

	// --- Routes ---
	// Overview
	http.HandleFunc("/", h.Dashboard)

	// Posts
	http.HandleFunc("/posts", h.PostsPage)
	http.HandleFunc("/post", h.PostDetailPage)

	// Post Histories
	http.HandleFunc("/history", h.PostHistoryPage)
	http.HandleFunc("/history/detail", h.PostHistoryDetailPage)

	// Actors
	http.HandleFunc("/actors", h.ActorsPage)
	http.HandleFunc("/actor", h.ActorDetailPage)

	// Blocks
	http.HandleFunc("/blocks", h.BlocksPage)
	http.HandleFunc("/block", h.BlockDetailPage)

	// Labels
	http.HandleFunc("/labels", h.LabelsPage)
	http.HandleFunc("/label", h.LabelDetailPage)

	// Labelers
	http.HandleFunc("/labelers", h.LabelersPage)
	http.HandleFunc("/labeler", h.LabelerDetailPage)

	// Lists
	http.HandleFunc("/lists", h.ListsPage)
	http.HandleFunc("/list", h.ListDetailPage)

	// List Items
	http.HandleFunc("/list-items", h.ListItemsPage)
	http.HandleFunc("/list-item", h.ListItemDetailPage)

	// List Blocks
	http.HandleFunc("/list-blocks", h.ListBlocksPage)
	http.HandleFunc("/list-block", h.ListBlockDetailPage)

	// Conversation Thread
	http.HandleFunc("/thread", h.ThreadPage)

	// Static files
	fs := http.FileServer(http.Dir("./dashboard/static"))
	http.Handle("/static/", http.StripPrefix("/static/", fs))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Dashboard listening on http://localhost:%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
