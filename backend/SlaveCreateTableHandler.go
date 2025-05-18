package main

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
)

func slaveCreateTableHandler(w http.ResponseWriter, r *http.Request) {
	if currentRole != RoleSlave {
		http.Error(w, "Only slaves should handle table creation requests from master", http.StatusForbidden)
		return
	}

	dbName := r.FormValue("db")
	tableName := r.FormValue("name")
	shardID := r.FormValue("shard_id")

	if dbName == "" || tableName == "" || shardID == "" {
		http.Error(w, "Database name, table name, and shard ID are required", http.StatusBadRequest)
		return
	}

	if !isValidIdentifier(dbName) || !isValidIdentifier(tableName) {
		http.Error(w, "Invalid database or table name", http.StatusBadRequest)
		return
	}

	shardIDInt, err := strconv.Atoi(shardID)
	if err != nil {
		http.Error(w, "Shard ID must be an integer", http.StatusBadRequest)
		return
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (id INT AUTO_INCREMENT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)", dbName, tableName)
	if err := executeSQL(query); err != nil {
		http.Error(w, fmt.Sprintf("Error creating table on slave: %v", err), http.StatusInternalServerError)
		return
	}

	insertShardQuery := "INSERT INTO cluster.table_shards (db_name, table_name, shard_id) VALUES (?, ?, ?)"
	_, err = db.Exec(insertShardQuery, dbName, tableName, shardIDInt)
	if err != nil {
		log.Printf("Error storing shard information on slave: %v", err)
		http.Error(w, "Failed to store shard information on slave", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Table '%s.%s' created on slave in shard %d successfully\n", dbName, tableName, shardIDInt)
}
