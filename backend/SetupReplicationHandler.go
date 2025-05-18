package main

import (
	"encoding/json"
	"net/http"
)

func setupReplicationHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if db == nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database connection not established. Please check MySQL settings.",
		})
		return
	}

	var req struct {
		MasterHost string `json:"masterHost"`
		MasterPort int    `json:"masterPort"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Invalid request format",
		})
		return
	}

	if req.MasterHost == "" || req.MasterPort == 0 {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Master host and port are required",
		})
		return
	}

	err := configureSlave(db, req.MasterHost, req.MasterPort)
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error setting up replication: " + err.Error(),
		})
		return
	}

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Replication setup successfully",
	})
}
