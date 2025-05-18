package main

import (
	"net/http"
	"time"
)

func electionHandler(w http.ResponseWriter, r *http.Request) {
	if currentRole == RoleMaster {
		w.WriteHeader(http.StatusOK)
		return
	}

	stateMutex.Lock()
	defer stateMutex.Unlock()

	loadNodesFromDB()
	for _, node := range state.Nodes {
		if node.URL == state.CurrentMaster && time.Since(node.LastSeen) > ElectionTimeout {
			promoteToMaster()
			break
		}
	}
	w.WriteHeader(http.StatusOK)
}
