package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

func dropDatabaseHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if db == nil {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Database not connected"})
		return
	}

	if currentRole == RoleSlave {
		log.Printf("Slave node received dropDatabase request. Operation not allowed.")
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Dropping databases is only allowed on the master node."})
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
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Invalid request: " + err.Error()})
		return
	}
	if req.DBName == "" {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Database name required"})
		return
	}

	protectedDBs := map[string]bool{"information_schema": true, "mysql": true, "performance_schema": true, "sys": true, "cluster": true}
	if protectedDBs[strings.ToLower(req.DBName)] {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Cannot drop system database: " + req.DBName})
		return
	}

	_, err = db.Exec("DROP DATABASE IF EXISTS " + SanitizeIdentifier(req.DBName))
	if err != nil {
		log.Printf("Error dropping database '%s' on master: %v", req.DBName, err)
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Error dropping database: " + err.Error()})
		return
	}
	log.Printf("Database '%s' dropped successfully on master.", req.DBName)

	replicateToNodes(map[string]interface{}{
		"operation": "drop_database",
		"dbName":    req.DBName,
		"shardId":   float64(calculateShardID(req.DBName)),
	})

	json.NewEncoder(w).Encode(Response{Success: true, Message: fmt.Sprintf("Database '%s' dropped", req.DBName)})
}
