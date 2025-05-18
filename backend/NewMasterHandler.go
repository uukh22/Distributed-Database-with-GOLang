package main

import (
	"encoding/json"
	"log"
	"net/http"
)

func newMasterHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		MasterURL string `json:"masterURL"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if req.MasterURL == "" {
		http.Error(w, "Master URL is required", http.StatusBadRequest)
		return
	}

	stateMutex.Lock()
	defer stateMutex.Unlock()

	state.CurrentMaster = req.MasterURL
	_, err := db.Exec(`
		UPDATE cluster.nodes 
		SET role = ? 
		WHERE url = ?`, RoleMaster, req.MasterURL)
	if err != nil {
		log.Printf("Error updating master role: %v", err)
	}

	if config.SelfURL != req.MasterURL {
		currentRole = RoleSlave
		_, err = db.Exec(`
			UPDATE cluster.nodes 
			SET role = ? 
			WHERE url = ?`, RoleSlave, config.SelfURL)
		if err != nil {
			log.Printf("Error updating slave role: %v", err)
		}
	}

	loadNodesFromDB()
	w.WriteHeader(http.StatusOK)
}
