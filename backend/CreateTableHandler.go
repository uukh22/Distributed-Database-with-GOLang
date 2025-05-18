package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
)

func createTableHandler(w http.ResponseWriter, r *http.Request) {
	if currentRole != RoleMaster {
		http.Error(w, "Only master can create tables", http.StatusForbidden)
		return
	}

	dbName := r.FormValue("db")
	tableName := r.FormValue("name")
	shardID := r.FormValue("shard_id")
	columns := r.FormValue("columns")

	if dbName == "" || tableName == "" || shardID == "" || columns == "" {
		http.Error(w, "Database name, table name, shard ID, and columns are required", http.StatusBadRequest)
		return
	}

	if !isValidIdentifier(dbName) || !isValidIdentifier(tableName) {
		http.Error(w, "Invalid database or table name", http.StatusBadRequest)
		return
	}

	shardIDInt, err := strconv.Atoi(shardID)
	if err != nil {
		http.Error(w, "Invalid shard ID: must be an integer", http.StatusBadRequest)
		return
	}

	if shardIDInt < 0 || shardIDInt >= config.ShardCount {
		http.Error(w, fmt.Sprintf("Shard ID must be between 0 and %d", config.ShardCount-1), http.StatusBadRequest)
		return
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s)", dbName, tableName, columns)
	if err := executeSQL(query); err != nil {
		http.Error(w, fmt.Sprintf("Error creating table: %v", err), http.StatusInternalServerError)
		return
	}

	insertShardQuery := "INSERT INTO cluster.table_shards (db_name, table_name, shard_id) VALUES (?, ?, ?)"
	_, err = db.Exec(insertShardQuery, dbName, tableName, shardIDInt)
	if err != nil {
		log.Printf("Error storing shard information: %v", err)
		http.Error(w, "Failed to store shard information", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Table '%s.%s' created in shard %d successfully\n", dbName, tableName, shardIDInt)

	go notifySlaves(fmt.Sprintf("/api/create-table?db=%s&name=%s&shard_id=%d&columns=%s",
		url.QueryEscape(dbName), url.QueryEscape(tableName), shardIDInt, url.QueryEscape(columns)))
}
