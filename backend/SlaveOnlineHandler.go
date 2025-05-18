package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

func slaveOnlineHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if currentRole != RoleMaster {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Only master node can handle slave online notifications",
		})
		return
	}

	var req struct {
		SlaveURL string `json:"slaveURL"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Invalid request format",
		})
		return
	}

	if req.SlaveURL == "" {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Slave URL is required",
		})
		return
	}

	stateMutex.Lock()
	defer stateMutex.Unlock()

	var exists bool
	err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM cluster.nodes WHERE url = ?)", req.SlaveURL).Scan(&exists)
	if err != nil {
		log.Printf("Error checking node existence: %v", err)
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error checking node existence",
		})
		return
	}

	now := time.Now()
	if exists {
		_, err = db.Exec(`
			UPDATE cluster.nodes 
			SET is_healthy = ?, last_seen = ? 
			WHERE url = ?`, true, now, req.SlaveURL)
	} else {
		registerNode(req.SlaveURL, RoleSlave, calculateShardID(req.SlaveURL))
	}

	if err != nil {
		log.Printf("Error updating slave status: %v", err)
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error updating slave status",
		})
		return
	}

	loadNodesFromDB()
	log.Printf("Slave %s is back online", req.SlaveURL)
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: fmt.Sprintf("Slave %s online status updated", req.SlaveURL),
	})
}
