package main

import (
	"encoding/json"
	"net/http"
)

func nodeRoleHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	result := map[string]interface{}{
		"role":    currentRole,
		"shardId": calculateShardID(config.SelfURL),
	}

	if currentRole == RoleSlave {
		result["masterUrl"] = state.CurrentMaster
	}

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Result:  result,
	})
}
