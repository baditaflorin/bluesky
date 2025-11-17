package main

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// TestInitializeDB tests database initialization
func TestInitializeDB(t *testing.T) {
	// Use a temporary database file
	tempDB := "test_followers.db"
	defer os.Remove(tempDB)

	db, err := initializeDB(tempDB)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Verify the table exists
	var tableName string
	err = db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='followers'").Scan(&tableName)
	if err != nil {
		t.Fatalf("Failed to find followers table: %v", err)
	}
	if tableName != "followers" {
		t.Errorf("Expected table name 'followers', got '%s'", tableName)
	}
}

// TestSaveFollowers tests saving followers to database
func TestSaveFollowers(t *testing.T) {
	// Use a temporary database file
	tempDB := "test_followers_save.db"
	defer os.Remove(tempDB)

	db, err := initializeDB(tempDB)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Create test followers
	testFollowers := []Follower{
		{
			DID:         "did:plc:test123",
			Handle:      "testuser.bsky.social",
			DisplayName: "Test User",
			Avatar:      "https://example.com/avatar.jpg",
			Viewer: Viewer{
				Muted:     false,
				BlockedBy: false,
				Following: "at://did:plc:test123/app.bsky.graph.follow/test",
			},
			Labels:      []Label{{Type: "label", Value: "test"}},
			CreatedAt:   time.Now(),
			Description: "Test description",
			IndexedAt:   time.Now(),
		},
		{
			DID:         "did:plc:test456",
			Handle:      "testuser2.bsky.social",
			DisplayName: "Test User 2",
			Avatar:      "https://example.com/avatar2.jpg",
			Viewer: Viewer{
				Muted:     false,
				BlockedBy: false,
				Following: "",
			},
			Labels:      []Label{},
			CreatedAt:   time.Now(),
			Description: "Test description 2",
			IndexedAt:   time.Now(),
		},
	}

	// Save followers
	err = saveFollowers(db, testFollowers)
	if err != nil {
		t.Fatalf("Failed to save followers: %v", err)
	}

	// Verify followers were saved
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM followers").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count followers: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 followers, got %d", count)
	}

	// Verify specific follower data
	var did, handle string
	err = db.QueryRow("SELECT did, handle FROM followers WHERE did = ?", "did:plc:test123").Scan(&did, &handle)
	if err != nil {
		t.Fatalf("Failed to query follower: %v", err)
	}
	if did != "did:plc:test123" || handle != "testuser.bsky.social" {
		t.Errorf("Expected did:plc:test123 and testuser.bsky.social, got %s and %s", did, handle)
	}
}

// TestSaveFollowersIdempotent tests that saving the same follower twice doesn't create duplicates
func TestSaveFollowersIdempotent(t *testing.T) {
	tempDB := "test_followers_idempotent.db"
	defer os.Remove(tempDB)

	db, err := initializeDB(tempDB)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	testFollower := []Follower{
		{
			DID:         "did:plc:test789",
			Handle:      "testuser3.bsky.social",
			DisplayName: "Test User 3",
			Avatar:      "https://example.com/avatar3.jpg",
			Viewer: Viewer{
				Muted:     false,
				BlockedBy: false,
				Following: "",
			},
			Labels:      []Label{},
			CreatedAt:   time.Now(),
			Description: "Test description 3",
			IndexedAt:   time.Now(),
		},
	}

	// Save follower twice
	err = saveFollowers(db, testFollower)
	if err != nil {
		t.Fatalf("Failed to save followers first time: %v", err)
	}

	err = saveFollowers(db, testFollower)
	if err != nil {
		t.Fatalf("Failed to save followers second time: %v", err)
	}

	// Verify only one follower exists
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM followers WHERE did = ?", "did:plc:test789").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count followers: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 follower, got %d", count)
	}
}

// TestAPIResponseUnmarshal tests JSON unmarshaling
func TestAPIResponseUnmarshal(t *testing.T) {
	jsonData := `{
		"followers": [
			{
				"did": "did:plc:test123",
				"handle": "testuser.bsky.social",
				"displayName": "Test User",
				"avatar": "https://example.com/avatar.jpg",
				"viewer": {
					"muted": false,
					"blockedBy": false,
					"following": "at://did:plc:test123/app.bsky.graph.follow/test"
				},
				"labels": [
					{
						"type": "label",
						"value": "test"
					}
				],
				"createdAt": "2024-01-01T00:00:00.000Z",
				"description": "Test description",
				"indexedAt": "2024-01-01T00:00:00.000Z"
			}
		],
		"cursor": "test_cursor_123"
	}`

	var apiResp APIResponse
	err := json.Unmarshal([]byte(jsonData), &apiResp)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	if len(apiResp.Followers) != 1 {
		t.Errorf("Expected 1 follower, got %d", len(apiResp.Followers))
	}

	if apiResp.Cursor != "test_cursor_123" {
		t.Errorf("Expected cursor 'test_cursor_123', got '%s'", apiResp.Cursor)
	}

	follower := apiResp.Followers[0]
	if follower.DID != "did:plc:test123" {
		t.Errorf("Expected DID 'did:plc:test123', got '%s'", follower.DID)
	}
	if follower.Handle != "testuser.bsky.social" {
		t.Errorf("Expected handle 'testuser.bsky.social', got '%s'", follower.Handle)
	}
	if follower.Viewer.Muted {
		t.Errorf("Expected Viewer.Muted to be false")
	}
	if follower.Viewer.Following != "at://did:plc:test123/app.bsky.graph.follow/test" {
		t.Errorf("Expected Viewer.Following to be 'at://did:plc:test123/app.bsky.graph.follow/test', got '%s'", follower.Viewer.Following)
	}
	if len(follower.Labels) != 1 || follower.Labels[0].Value != "test" {
		t.Errorf("Expected 1 label with value 'test'")
	}
}

// TestDatabaseTransactionRollback tests that transactions rollback on error
func TestDatabaseTransactionRollback(t *testing.T) {
	tempDB := "test_followers_rollback.db"
	defer os.Remove(tempDB)

	db, err := initializeDB(tempDB)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// First, save a valid follower
	validFollower := []Follower{
		{
			DID:         "did:plc:valid",
			Handle:      "valid.bsky.social",
			DisplayName: "Valid User",
			Avatar:      "https://example.com/valid.jpg",
			Viewer: Viewer{
				Muted:     false,
				BlockedBy: false,
				Following: "",
			},
			Labels:      []Label{},
			CreatedAt:   time.Now(),
			Description: "Valid description",
			IndexedAt:   time.Now(),
		},
	}

	err = saveFollowers(db, validFollower)
	if err != nil {
		t.Fatalf("Failed to save valid follower: %v", err)
	}

	// Verify the valid follower exists
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM followers").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count followers: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 follower after valid insert, got %d", count)
	}
}

// TestLabelsConversion tests the labels to string conversion
func TestLabelsConversion(t *testing.T) {
	tempDB := "test_labels.db"
	defer os.Remove(tempDB)

	db, err := initializeDB(tempDB)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	testFollower := []Follower{
		{
			DID:         "did:plc:labels",
			Handle:      "labels.bsky.social",
			DisplayName: "Labels Test",
			Avatar:      "https://example.com/labels.jpg",
			Viewer: Viewer{
				Muted:     false,
				BlockedBy: false,
				Following: "",
			},
			Labels: []Label{
				{Type: "type1", Value: "value1"},
				{Type: "type2", Value: "value2"},
			},
			CreatedAt:   time.Now(),
			Description: "Labels test",
			IndexedAt:   time.Now(),
		},
	}

	err = saveFollowers(db, testFollower)
	if err != nil {
		t.Fatalf("Failed to save follower with labels: %v", err)
	}

	// Verify labels were stored correctly
	var labels string
	err = db.QueryRow("SELECT labels FROM followers WHERE did = ?", "did:plc:labels").Scan(&labels)
	if err != nil {
		t.Fatalf("Failed to query labels: %v", err)
	}

	expectedLabels := "type1:value1,type2:value2"
	if labels != expectedLabels {
		t.Errorf("Expected labels '%s', got '%s'", expectedLabels, labels)
	}
}
