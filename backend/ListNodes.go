package main

import (
	"encoding/json"
	"net/http"
)

func listNodes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	loadNodesFromDB()
	var nodeList []Node
	for _, node := range state.Nodes {
		nodeList = append(nodeList, *node)
	}

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Result:  nodeList,
	})
}
