package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	baseURL    = "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollowers?actor=did%3Aplc%3Az72i7hdynmk6r22z27h6tvur&limit=30"
	dbFile     = "followers.db"
	tableName  = "followers"
	maxRetries = 5
)

// Follower represents a follower's structure as per the JSON response.
type Follower struct {
	DID         string    `json:"did"`
	Handle      string    `json:"handle"`
	DisplayName string    `json:"displayName"`
	Avatar      string    `json:"avatar"`
	Viewer      Viewer    `json:"viewer"`
	Labels      []Label   `json:"labels"`
	CreatedAt   time.Time `json:"createdAt"`
	Description string    `json:"description"`
	IndexedAt   time.Time `json:"indexedAt"`
}

// Viewer represents the viewer-specific information within a follower.
type Viewer struct {
	Muted     bool   `json:"muted"`
	BlockedBy bool   `json:"blockedBy"`
	Following string `json:"following"`
}

// Label represents each label object in the labels array
type Label struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// APIResponse represents the full structure of the API response.
type APIResponse struct {
	Followers []Follower `json:"followers"`
	Cursor    string     `json:"cursor"`
}

func main() {
	// Parse the starting cursor from command-line arguments.
	startCursor := flag.String("cursor", "", "The starting cursor for fetching followers. If empty, starts from scratch.")
	flag.Parse()

	// Initialize the SQLite database.
	log.Println("Initializing the database...")
	db, err := initializeDB(dbFile)
	if err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}
	defer db.Close()
	log.Println("Database initialized successfully.")

	// Start fetching followers from the specified cursor or from scratch.
	cursor := *startCursor
	for {
		log.Printf("Fetching followers with cursor: %s\n", cursor)

		// Fetch data from API and parse the result.
		followers, newCursor, err := fetchFollowers(cursor)
		if err != nil {
			log.Printf("Error fetching followers: %v. Applying backoff and retrying...\n", err)
			time.Sleep(2 * time.Second) // Short delay before retrying
			continue
		}
		log.Printf("Fetched %d followers with cursor: %s\n", len(followers), cursor)

		// Insert followers into the database in a single transaction for performance.
		log.Println("Starting database transaction to save followers.")
		if err := saveFollowers(db, followers); err != nil {
			log.Printf("Error saving followers batch: %v", err)
			continue
		}
		log.Println("Followers saved successfully.")

		// If there is no new cursor, we reached the end of the data.
		if newCursor == "" {
			log.Println("No new cursor found, all followers processed.")
			break
		}

		// Update cursor for the next iteration.
		log.Printf("Updating cursor to: %s\n", newCursor)
		cursor = newCursor
	}
}

// initializeDB sets up the SQLite database.
func initializeDB(dbFile string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			did TEXT PRIMARY KEY,
			handle TEXT,
			displayName TEXT,
			avatar TEXT,
			viewer_muted BOOLEAN,
			viewer_blockedBy BOOLEAN,
			viewer_following TEXT,
			labels TEXT,
			createdAt DATETIME,
			description TEXT,
			indexedAt DATETIME
		);
	`, tableName)
	_, err = db.Exec(createTableQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return db, nil
}

// fetchFollowers makes an API request to get followers and returns them along with a cursor.
func fetchFollowers(cursor string) ([]Follower, string, error) {
	url := baseURL
	if cursor != "" {
		url += "&cursor=" + cursor
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("Attempt %d: Making API request to URL: %s\n", attempt, url)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Failed to make API request: %v. Retrying...\n", err)
			time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff
			continue
		}

		// Check HTTP status code
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			log.Printf("API returned status %d: %s. Retrying...\n", resp.StatusCode, resp.Status)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		log.Println("API request successful, reading response body.")
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Printf("Failed to read response body: %v. Retrying...\n", err)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		// Check if the response is HTML (likely an error page)
		if http.DetectContentType(body) == "text/html; charset=utf-8" {
			log.Printf("Received HTML response (likely an error page), retrying after backoff...\n")
			time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff
			continue
		}

		// Attempt to parse JSON response
		log.Println("Parsing JSON response.")
		var apiResp APIResponse
		if err := json.Unmarshal(body, &apiResp); err != nil {
			log.Printf("Failed to unmarshal JSON: %v. Retrying after backoff...\n", err)
			time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff
			continue
		}

		log.Printf("Parsed %d followers from response, new cursor: %s\n", len(apiResp.Followers), apiResp.Cursor)
		return apiResp.Followers, apiResp.Cursor, nil
	}

	return nil, "", fmt.Errorf("exceeded max retries for cursor %s", cursor)
}

// saveFollowers inserts followers data into the database in a single transaction for batch efficiency.
func saveFollowers(db *sql.DB, followers []Follower) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	log.Println("Database transaction started.")

	stmt, err := tx.Prepare(fmt.Sprintf(`
		INSERT OR REPLACE INTO %s (did, handle, displayName, avatar, viewer_muted, viewer_blockedBy, viewer_following, labels, createdAt, description, indexedAt) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`, tableName))
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()
	//log.Println("Prepared statement for inserting followers.")

	for _, follower := range followers {
		// Convert labels to a comma-separated string of "type:value"
		var labels []string
		for _, label := range follower.Labels {
			labels = append(labels, fmt.Sprintf("%s:%s", label.Type, label.Value))
		}
		labelStr := strings.Join(labels, ",")

		_, err := stmt.Exec(
			follower.DID,
			follower.Handle,
			follower.DisplayName,
			follower.Avatar,
			follower.Viewer.Muted,
			follower.Viewer.BlockedBy,
			follower.Viewer.Following,
			labelStr,
			follower.CreatedAt,
			follower.Description,
			follower.IndexedAt,
		)
		if err != nil {
			log.Printf("Failed to save follower %s: %v", follower.DID, err)
			tx.Rollback()
			return fmt.Errorf("failed to execute statement for follower %s: %w", follower.DID, err)
		}
		//log.Printf("Follower %s saved.", follower.DID)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	log.Println("Transaction committed successfully.")

	return nil
}
