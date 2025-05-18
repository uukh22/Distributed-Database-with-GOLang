package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

func createDatabaseHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if db == nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database connection not established. Please check MySQL settings.",
		})
		return
	}

	if currentRole == RoleSlave {
		log.Println("Slave node received createDatabase request, forwarding to master.")
		forwardRequestToMaster(w, r)
		return
	}

	var req struct {
		DBName string `json:"dbName"`
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Invalid request body: " + err.Error()})
		return
	}
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Invalid request format: " + err.Error(),
		})
		return
	}

	if req.DBName == "" {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database name is required",
		})
		return
	}

	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + SanitizeIdentifier(req.DBName))
	if err != nil {
		log.Printf("Error creating database '%s' on master: %v", req.DBName, err)
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error creating database: " + err.Error(),
		})
		return
	}

	log.Printf("Database '%s' created successfully on master.", req.DBName)

	operation := map[string]interface{}{
		"operation": "create_database",
		"dbName":    req.DBName,
		"shardId":   float64(calculateShardID(req.DBName)),
	}
	replicateToNodes(operation)

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: fmt.Sprintf("Database '%s' created successfully on master and replication initiated.", req.DBName),
	})
}
