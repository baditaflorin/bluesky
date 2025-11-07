package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
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

// Logger interface for structured logging
type Logger interface {
	Info(msg string, fields map[string]interface{})
	Error(msg string, fields map[string]interface{})
}

// TextLogger implements Logger with text output
type TextLogger struct{}

func (l *TextLogger) Info(msg string, fields map[string]interface{}) {
	if len(fields) == 0 {
		log.Println(msg)
	} else {
		log.Printf("%s %v\n", msg, fields)
	}
}

func (l *TextLogger) Error(msg string, fields map[string]interface{}) {
	if len(fields) == 0 {
		log.Println("ERROR:", msg)
	} else {
		log.Printf("ERROR: %s %v\n", msg, fields)
	}
}

// JSONLogger implements Logger with JSON output
type JSONLogger struct{}

func (l *JSONLogger) Info(msg string, fields map[string]interface{}) {
	l.log("INFO", msg, fields)
}

func (l *JSONLogger) Error(msg string, fields map[string]interface{}) {
	l.log("ERROR", msg, fields)
}

func (l *JSONLogger) log(level, msg string, fields map[string]interface{}) {
	logEntry := map[string]interface{}{
		"level":     level,
		"message":   msg,
		"timestamp": time.Now().Format(time.RFC3339),
	}
	for k, v := range fields {
		logEntry[k] = v
	}
	jsonBytes, _ := json.Marshal(logEntry)
	fmt.Println(string(jsonBytes))
}

var logger Logger = &TextLogger{}

func main() {
	// Parse the starting cursor from command-line arguments.
	startCursor := flag.String("cursor", "", "The starting cursor for fetching followers. If empty, starts from scratch.")
	jsonLog := flag.Bool("json", false, "Enable JSON logging format")
	flag.Parse()

	// Set logger based on flag
	if *jsonLog {
		logger = &JSONLogger{}
		log.SetOutput(io.Discard) // Disable default logger
	}

	// Set up context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("Received interrupt signal, shutting down gracefully...", nil)
		cancel()
	}()

	// Initialize the SQLite database.
	logger.Info("Initializing the database...", nil)
	db, err := initializeDB(dbFile)
	if err != nil {
		logger.Error("Database initialization failed", map[string]interface{}{"error": err.Error()})
		os.Exit(1)
	}
	defer db.Close()
	logger.Info("Database initialized successfully", nil)

	// Start fetching followers from the specified cursor or from scratch.
	cursor := *startCursor
	for {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			logger.Info("Context cancelled, stopping fetch", map[string]interface{}{"last_cursor": cursor})
			return
		default:
		}

		logger.Info("Fetching followers", map[string]interface{}{"cursor": cursor})

		// Fetch data from API and parse the result.
		followers, newCursor, err := fetchFollowers(ctx, cursor)
		if err != nil {
			if ctx.Err() != nil {
				logger.Info("Context cancelled during fetch", nil)
				return
			}
			logger.Error("Error fetching followers. Applying backoff and retrying...", map[string]interface{}{"error": err.Error()})
			time.Sleep(2 * time.Second) // Short delay before retrying
			continue
		}
		logger.Info("Fetched followers", map[string]interface{}{"count": len(followers), "cursor": cursor})

		// Insert followers into the database in a single transaction for performance.
		logger.Info("Starting database transaction to save followers", nil)
		if err := saveFollowers(db, followers); err != nil {
			logger.Error("Error saving followers batch", map[string]interface{}{"error": err.Error()})
			continue
		}
		logger.Info("Followers saved successfully", nil)

		// If there is no new cursor, we reached the end of the data.
		if newCursor == "" {
			logger.Info("No new cursor found, all followers processed", nil)
			break
		}

		// Update cursor for the next iteration.
		logger.Info("Updating cursor", map[string]interface{}{"new_cursor": newCursor})
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
func fetchFollowers(ctx context.Context, cursor string) ([]Follower, string, error) {
	url := baseURL
	if cursor != "" {
		url += "&cursor=" + cursor
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Check if context is cancelled
		if ctx.Err() != nil {
			return nil, "", ctx.Err()
		}

		logger.Info("Making API request", map[string]interface{}{"attempt": attempt, "url": url})

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			logger.Error("Failed to create request", map[string]interface{}{"error": err.Error()})
			return nil, "", err
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			logger.Error("Failed to make API request. Retrying...", map[string]interface{}{"error": err.Error()})
			time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff
			continue
		}

		// Check HTTP status code
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			logger.Error("API returned non-OK status. Retrying...", map[string]interface{}{"status": resp.StatusCode})
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		logger.Info("API request successful, reading response body", nil)
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			logger.Error("Failed to read response body. Retrying...", map[string]interface{}{"error": err.Error()})
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		// Check if the response is HTML (likely an error page)
		if http.DetectContentType(body) == "text/html; charset=utf-8" {
			logger.Error("Received HTML response (likely an error page), retrying after backoff...", nil)
			time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff
			continue
		}

		// Attempt to parse JSON response
		logger.Info("Parsing JSON response", nil)
		var apiResp APIResponse
		if err := json.Unmarshal(body, &apiResp); err != nil {
			logger.Error("Failed to unmarshal JSON. Retrying after backoff...", map[string]interface{}{"error": err.Error()})
			time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff
			continue
		}

		logger.Info("Parsed followers from response", map[string]interface{}{"count": len(apiResp.Followers), "new_cursor": apiResp.Cursor})
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
	logger.Info("Database transaction started", nil)

	stmt, err := tx.Prepare(fmt.Sprintf(`
		INSERT OR REPLACE INTO %s (did, handle, displayName, avatar, viewer_muted, viewer_blockedBy, viewer_following, labels, createdAt, description, indexedAt) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`, tableName))
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

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
			logger.Error("Failed to save follower", map[string]interface{}{"did": follower.DID, "error": err.Error()})
			tx.Rollback()
			return fmt.Errorf("failed to execute statement for follower %s: %w", follower.DID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	logger.Info("Transaction committed successfully", nil)

	return nil
}
