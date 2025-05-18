package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func replicationHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if currentRole == RoleMaster {
		log.Printf("Master node received a /api/replicate call. This should only be called on slaves. Ignoring.")
		http.Error(w, "This is a master node. Replication endpoint is for slaves.", http.StatusBadRequest)
		return
	}

	if db == nil {
		log.Printf("Slave received replication signal, but DB not connected.")
		json.NewEncoder(w).Encode(Response{Success: false, Message: "DB not connected on slave"})
		return
	}

	var req map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Slave: Error decoding replication request: %v", err)
		http.Error(w, "Invalid replication request format", http.StatusBadRequest)
		return
	}

	operation, _ := req["operation"].(string)
	dbName, _ := req["dbName"].(string)
	shardIDFloat, shardIdOk := req["shardId"].(float64)

	log.Printf("Slave (%s) received HTTP replication signal: Op=%s, DBName=%s, data_shardId=%v, FullReq: %+v",
		config.SelfURL, operation, dbName, req["shardId"], req)

	if !shardIdOk {
		log.Printf("Slave: Received replication signal without a valid data_shardId. Req: %+v", req)
		json.NewEncoder(w).Encode(Response{
			Success: true,
			Message: "Slave acknowledged replication signal, but data_shardId was missing or invalid.",
		})
		return
	}

	slaveOwnsShardID := calculateShardID(config.SelfURL)

	log.Printf("Slave (serves shard %d) acknowledged replication signal for operation '%s' concerning data_shardId '%.0f'. MySQL replication is primary for data consistency.",
		slaveOwnsShardID, operation, shardIDFloat)

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: fmt.Sprintf("Slave acknowledged replication signal for operation: %s, data_shardId: %.0f.", operation, shardIDFloat),
	})
}
