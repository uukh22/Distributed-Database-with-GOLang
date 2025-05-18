package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
)

func registerHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}

	var newNode struct {
		URL     string `json:"url"`
		Role    string `json:"role"`
		ShardID int    `json:"shardId"`
	}

	if err := json.Unmarshal(body, &newNode); err != nil {
		log.Printf("Error decoding registration: %v", err)
		http.Error(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	if newNode.URL == "" || newNode.Role == "" {
		log.Println("Missing required fields")
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	if newNode.URL == config.SelfURL && config.SelfURL == config.MasterURL {
		newNode.Role = RoleMaster
	}

	registerNode(newNode.URL, newNode.Role, newNode.ShardID)

	log.Printf("Successfully registered node: %s (%s) in shard %d", newNode.URL, newNode.Role, newNode.ShardID)
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Node registered successfully",
	})
}
