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
	CreatedAt   time.Time `json:"createdAt"`
	IndexedAt   time.Time `json:"indexedAt"`
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
	jsonBytes, err := json.Marshal(logEntry)
	if err != nil {
		// Fallback to stderr if JSON marshaling fails
		fmt.Fprintf(os.Stderr, "ERROR: Failed to marshal log entry: %v\n", err)
		return
	}
	fmt.Println(string(jsonBytes))
}

var logger Logger = &TextLogger{}

func main() {
	// Parse command-line flags
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

	// Start fetching followers recursively.
	cursor := ""
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
			logger.Error("Error fetching followers", map[string]interface{}{"error": err.Error()})
			os.Exit(1)
		}
		logger.Info("Fetched followers", map[string]interface{}{"count": len(followers)})

		// Insert followers into the database.
		logger.Info("Saving followers to the database...", nil)
		if err := saveFollowers(db, followers); err != nil {
			logger.Error("Error saving followers", map[string]interface{}{"error": err.Error()})
			os.Exit(1)
		}
		logger.Info("Followers saved successfully", nil)

		// If there is no new cursor, we reached the end of the data.
		if newCursor == "" {
			logger.Info("All followers processed", nil)
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

		// Log time before making the request
		start := time.Now()
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
		logger.Info("API request successful", map[string]interface{}{"duration": time.Since(start).String()})

		// Check HTTP status code
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			logger.Error("API returned non-OK status. Retrying...", map[string]interface{}{"status": resp.StatusCode})
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		logger.Info("Reading response body...", nil)

		// Log time taken to read the response body
		bodyStart := time.Now()
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			logger.Error("Failed to read response body. Retrying...", map[string]interface{}{"error": err.Error(), "duration": time.Since(bodyStart).String()})
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}
		logger.Info("Response body read", map[string]interface{}{"duration": time.Since(bodyStart).String()})

		// Check if the response is HTML (likely an error page)
		contentType := http.DetectContentType(body)
		if strings.HasPrefix(contentType, "text/html") {
			logger.Error("Received HTML response (likely an error page), retrying after backoff...", nil)
			time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff
			continue
		}

		// Log time taken to parse JSON
		parseStart := time.Now()
		logger.Info("Parsing JSON response", nil)
		var apiResp APIResponse
		if err := json.Unmarshal(body, &apiResp); err != nil {
			logger.Error("Failed to unmarshal JSON. Retrying after backoff...", map[string]interface{}{"error": err.Error(), "duration": time.Since(parseStart).String()})
			time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff
			continue
		}
		logger.Info("JSON parsed", map[string]interface{}{"duration": time.Since(parseStart).String(), "new_cursor": apiResp.Cursor})

		// If all goes well, return the parsed followers and new cursor
		logger.Info("Returning followers", map[string]interface{}{"count": len(apiResp.Followers), "cursor": apiResp.Cursor})
		return apiResp.Followers, apiResp.Cursor, nil
	}

	return nil, "", fmt.Errorf("exceeded max retries for cursor %s", cursor)
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
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("failed to prepare statement: %w, rollback failed: %v", err, rbErr)
		}
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
			if rbErr := tx.Rollback(); rbErr != nil {
				return fmt.Errorf("failed to execute statement: %w, rollback failed: %v", err, rbErr)
			}
			return fmt.Errorf("failed to execute statement: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
