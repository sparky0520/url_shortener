package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func connectDB() (*sql.DB, error) {
	// Open connection
	dsn := "root:1234@tcp(127.0.0.1:3306)/url_shortener"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(24)
	db.SetMaxIdleConns(18)
	db.SetConnMaxLifetime(1 * time.Minute)

	// Test connection
	if err = db.Ping(); err != nil {
		return nil, err
	}

	createTableQuery := `
	CREATE TABLE IF NOT EXISTS main(
		id INT AUTO_INCREMENT PRIMARY KEY,
		url TEXT NOT NULL,
    	clicks INT DEFAULT 0,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)
	`

	_, err = db.Exec(createTableQuery)
	if err != nil {
		return nil, err
	}

	log.Println("Database initialized successfully")
	return db, nil
}

// create record
func addUrl(url string, db *sql.DB) (int64, error) {
	result, err := db.Exec("INSERT INTO main (url) VALUES ?", url)
	if err != nil {
		return 0, err
	}

	// Get auto generated id
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	return id, nil
}

// fetch record
func getUrl(id int64, db *sql.DB) (string, error) {
	var url string
	err := db.QueryRow("SELECT url FROM main WHERE id = ?", id).Scan(url)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("No record with id %d not found", id)
	}
	if err != nil {
		return "", err
	}

	// Increment clicks
	db.Exec("UPDATE main SET clicks = clicks + 1 WHERE id = ?", id)

	return url, nil
}
