package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	baseURL   = "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollowers?actor=did%3Aplc%3Az72i7hdynmk6r22z27h6tvur&limit=30"
	dbFile    = "followers.db"
	tableName = "followers"
)

// Follower represents a follower's structure as per the JSON response.
type Follower struct {
	DID         string    `json:"did"`
	Handle      string    `json:"handle"`
	DisplayName string    `json:"displayName"`
	Avatar      string    `json:"avatar"`
	CreatedAt   time.Time `json:"createdAt"`
	IndexedAt   time.Time `json:"indexedAt"`
}

// APIResponse represents the full structure of the API response.
type APIResponse struct {
	Followers []Follower `json:"followers"`
	Cursor    string     `json:"cursor"`
}

func main() {
	// Initialize the SQLite database.
	log.Println("Initializing the database...")
	db, err := initializeDB(dbFile)
	if err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}
	defer db.Close()
	log.Println("Database initialized successfully.")

	// Start fetching followers recursively.
	cursor := ""
	for {
		log.Printf("Fetching followers with cursor: %s\n", cursor)

		// Fetch data from API and parse the result.
		followers, newCursor, err := fetchFollowers(cursor)
		if err != nil {
			log.Fatalf("Error fetching followers: %v", err)
		}
		log.Printf("Fetched %d followers.\n", len(followers))

		// Insert followers into the database.
		log.Println("Saving followers to the database...")
		if err := saveFollowers(db, followers); err != nil {
			log.Fatalf("Error saving followers: %v", err)
		}
		log.Println("Followers saved successfully.")

		// If there is no new cursor, we reached the end of the data.
		if newCursor == "" {
			log.Println("All followers processed.")
			break
		}
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
			createdAt DATETIME,
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

	resp, err := http.Get(url)
	if err != nil {
		return nil, "", fmt.Errorf("failed to make API request: %w", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read response body: %w", err)
	}

	var apiResp APIResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, "", fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	log.Printf("Parsed %d followers from response.\n", len(apiResp.Followers))
	return apiResp.Followers, apiResp.Cursor, nil
}

// saveFollowers inserts followers data into the database.
func saveFollowers(db *sql.DB, followers []Follower) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf(`
		INSERT OR REPLACE INTO %s (did, handle, displayName, avatar, createdAt, indexedAt) 
		VALUES (?, ?, ?, ?, ?, ?);
	`, tableName))
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, follower := range followers {
		_, err := stmt.Exec(
			follower.DID,
			follower.Handle,
			follower.DisplayName,
			follower.Avatar,
			follower.CreatedAt,
			follower.IndexedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to execute statement: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
