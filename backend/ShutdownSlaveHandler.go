package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"runtime"
	"time"
)

func shutdownSlaveHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if currentRole != RoleMaster {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Only master node can shutdown slaves",
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

	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Post(req.SlaveURL+"/api/shutdown", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusOK {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: fmt.Sprintf("Failed to shutdown slave %s", req.SlaveURL),
		})
		return
	}

	_, err = db.Exec(`
		UPDATE cluster.nodes 
		SET is_healthy = ?, last_seen = ? 
		WHERE url = ?`, false, time.Now(), req.SlaveURL)
	if err != nil {
		log.Printf("Error updating node status: %v", err)
	}

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: fmt.Sprintf("Shutdown command sent to slave %s", req.SlaveURL),
	})
}

func shutdownHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	log.Println("Received shutdown command, initiating graceful shutdown...")

	if currentRole == RoleMaster {
		log.Println("Master node shutting down, transitioning to slave role")
		stateMutex.Lock()
		currentRole = RoleSlave
		_, err := db.Exec(`
			UPDATE cluster.nodes 
			SET role = ?, is_healthy = ?, last_seen = ? 
			WHERE url = ?`, RoleSlave, false, time.Now(), config.SelfURL)
		if err != nil {
			log.Printf("Error updating node role to slave: %v", err)
		}
		stateMutex.Unlock()

		loadNodesFromDB()
		for _, node := range state.Nodes {
			if node.URL != config.SelfURL && node.IsHealthy && node.Role == RoleSlave {
				go func(url string) {
					if sendElectionRequest(url) {
						log.Printf("Initiated election with node %s", url)
					}
				}(node.URL)
			}
		}
	}

	response := Response{
		Success: true,
		Message: "Shutdown initiated",
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Failed to send shutdown response: %v", err)
	}

	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "linux":
		cmd = exec.Command("sudo", "shutdown", "-h", "now")
	case "windows":
		cmd = exec.Command("shutdown", "/s", "/t", "0")
	case "darwin":
		cmd = exec.Command("sudo", "shutdown", "-h", "now")
	default:
		log.Printf("Unsupported OS: %s", runtime.GOOS)
		return
	}

	go func() {
		if err := cmd.Run(); err != nil {
			log.Printf("Failed to execute shutdown command: %v", err)
		} else {
			log.Println("Shutdown command executed successfully")
		}
	}()
}
