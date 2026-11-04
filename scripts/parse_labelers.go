package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// Regex to match each labeler row in the markdown table
// Format: | <a href=https://bsky.app/profile/DID><b>NAME</b></a><br>DESCRIPTION | LIKES |
var rowRegex = regexp.MustCompile(`^\|\s*<a href=https://bsky\.app/profile/(did:[^>]+)><b>(.*?)</b></a><br>(.*?)\s*\|\s*(\d+)\s*\|$`)

type Labeler struct {
	DID         string
	Name        string
	Description string
	Likes       string
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run parse_labelers.go <input.md> <output.csv>")
		os.Exit(1)
	}

	inputFile := os.Args[1]
	outputFile := os.Args[2]

	data, err := os.ReadFile(inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read input file: %v\n", err)
		os.Exit(1)
	}

	lines := strings.Split(string(data), "\n")

	var labelers []Labeler
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		matches := rowRegex.FindStringSubmatch(line)
		if matches == nil {
			continue
		}

		did := matches[1]
		name := strings.TrimSpace(matches[2])
		description := strings.TrimSpace(matches[3])
		likes := matches[4]

		// Decode common HTML entities in description
		description = strings.ReplaceAll(description, "&#38;", "&")
		description = strings.ReplaceAll(description, "&#60;", "<")
		description = strings.ReplaceAll(description, "&#62;", ">")
		description = strings.ReplaceAll(description, "&#124;", "|")
		description = strings.ReplaceAll(description, "<i>No description</i>", "")

		name = strings.ReplaceAll(name, "&#38;", "&")

		labelers = append(labelers, Labeler{
			DID:         did,
			Name:        name,
			Description: description,
			Likes:       likes,
		})
	}

	if len(labelers) == 0 {
		fmt.Fprintln(os.Stderr, "No labelers found. Check the input file format.")
		os.Exit(1)
	}

	// Write CSV
	out, err := os.Create(outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output file: %v\n", err)
		os.Exit(1)
	}
	defer out.Close()

	writer := csv.NewWriter(out)
	defer writer.Flush()

	// Header
	if err := writer.Write([]string{"did", "name", "description", "likes"}); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write header: %v\n", err)
		os.Exit(1)
	}

	for _, l := range labelers {
		if err := writer.Write([]string{l.DID, l.Name, l.Description, l.Likes}); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write row: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Printf("Parsed %d labelers\n", len(labelers))
	fmt.Printf("Output: %s\n", outputFile)
}
