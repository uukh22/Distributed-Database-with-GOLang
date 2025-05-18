package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

func dropTableHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if db == nil {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Database not connected"})
		return
	}

	if currentRole == RoleSlave {
		log.Printf("Slave node received dropTable request. Operation not allowed.")
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Dropping tables is only allowed on the master node."})
		return
	}

	// --- Master Logic ---
	var req struct {
		DBName    string `json:"dbName"`
		TableName string `json:"tableName"`
	}
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Invalid request body: " + err.Error()})
		return
	}
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Invalid request: " + err.Error()})
		return
	}
	if req.DBName == "" || req.TableName == "" {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "DB name and table name required"})
		return
	}

	safeDBName := SanitizeIdentifier(req.DBName)
	safeTableName := SanitizeIdentifier(req.TableName)

	dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		config.MySQL.User, config.MySQL.Password, config.MySQL.Host, config.MySQL.Port, safeDBName))
	if err != nil {
		log.Printf("Error connecting to database '%s' on master: %v", safeDBName, err)
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Error connecting to DB: " + err.Error()})
		return
	}
	defer dbConn.Close()

	_, err = dbConn.Exec("DROP TABLE IF EXISTS " + safeTableName)
	if err != nil {
		log.Printf("Error dropping table '%s.%s' on master: %v", safeDBName, safeTableName, err)
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Error dropping table: " + err.Error()})
		return
	}
	log.Printf("Table '%s.%s' dropped successfully on master.", safeDBName, safeTableName)

	_, err = db.Exec("DELETE FROM cluster.table_shards WHERE db_name = ? AND table_name = ?", safeDBName, safeTableName)
	if err != nil {
		log.Printf("Error removing table shard mapping for '%s.%s' on master: %v", safeDBName, safeTableName, err)
	}

	// Determine a representative shardId for the dropped table for notification purposes
	shardID := calculateShardID(safeDBName + "." + safeTableName)
	replicateToNodes(map[string]interface{}{
		"operation": "drop_table",
		"dbName":    safeDBName,
		"tableName": safeTableName,
		"shardId":   float64(shardID),
	})

	json.NewEncoder(w).Encode(Response{Success: true, Message: fmt.Sprintf("Table '%s' dropped", safeTableName)})
}
