package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

func crudHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if db == nil {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Database not connected"})
		return
	}

	var req struct {
		DBName        string                 `json:"dbName"`
		Operation     string                 `json:"operation"`
		Table         string                 `json:"table"`
		Data          map[string]interface{} `json:"data,omitempty"`
		Where         map[string]interface{} `json:"where,omitempty"`
		ShardKeyValue interface{}            `json:"shardKeyValue,omitempty"`
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Invalid request body: " + err.Error()})
		return
	}

	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Invalid request format: " + err.Error()})
		return
	}

	if req.DBName == "" || req.Table == "" || req.Operation == "" {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "DB name, table, and operation required"})
		return
	}

	req.DBName = SanitizeIdentifier(req.DBName)
	req.Table = SanitizeIdentifier(req.Table)

	isWriteOperation := (req.Operation == "create" || req.Operation == "update" || req.Operation == "delete")

	var shardIDForRequest int
	var tableShardKeyCol sql.NullString
	var registeredTableShardID int

	err = db.QueryRow("SELECT shard_id, shard_key FROM cluster.table_shards WHERE db_name = ? AND table_name = ?",
		req.DBName, req.Table).Scan(&registeredTableShardID, &tableShardKeyCol)

	if err != nil {
		if err == sql.ErrNoRows {
			log.Printf("Warning: Table '%s.%s' not found in cluster.table_shards. Calculating shardID based on table name for routing.", req.DBName, req.Table)
			shardIDForRequest = calculateShardID(req.DBName + "." + req.Table)
		} else {
			log.Printf("Error retrieving shard info for '%s.%s': %v", req.DBName, req.Table, err)
			json.NewEncoder(w).Encode(Response{Success: false, Message: "Error retrieving shard info: " + err.Error()})
			return
		}
	} else {
		if tableShardKeyCol.Valid && tableShardKeyCol.String != "" {

			if req.ShardKeyValue != nil {
				shardIDForRequest = calculateShardID(fmt.Sprintf("%v", req.ShardKeyValue))
				log.Printf("Calculated shardID %d for table '%s.%s' based on provided ShardKeyValue '%v' (shard_key column: '%s')",
					shardIDForRequest, req.DBName, req.Table, req.ShardKeyValue, tableShardKeyCol.String)
			} else if isWriteOperation {

				var inferredShardValue interface{}
				if req.Operation == "create" || req.Operation == "update" {
					if val, ok := req.Data[tableShardKeyCol.String]; ok {
						inferredShardValue = val
					}
				}
				if inferredShardValue != nil {
					shardIDForRequest = calculateShardID(fmt.Sprintf("%v", inferredShardValue))
					log.Printf("Calculated shardID %d for table '%s.%s' based on inferred ShardKeyValue '%v' from Data (shard_key column: '%s')",
						shardIDForRequest, req.DBName, req.Table, inferredShardValue, tableShardKeyCol.String)
				} else {
					log.Printf("Error: Write operation on sharded table '%s.%s' (key: '%s') but ShardKeyValue not provided and not inferable from Data.",
						req.DBName, req.Table, tableShardKeyCol.String)
					json.NewEncoder(w).Encode(Response{Success: false, Message: fmt.Sprintf("ShardKeyValue for column '%s' is required for this operation on table '%s.%s'", tableShardKeyCol.String, req.DBName, req.Table)})
					return
				}
			} else {
				shardIDForRequest = registeredTableShardID
				log.Printf("Read operation on sharded table '%s.%s' (key: '%s') without ShardKeyValue. Using table's registered shardID: %d.",
					req.DBName, req.Table, tableShardKeyCol.String, shardIDForRequest)
			}
		} else {
			shardIDForRequest = registeredTableShardID
			log.Printf("Table '%s.%s' has no specific shard_key column in cluster.table_shards. Using its registered shardID: %d.",
				req.DBName, req.Table, shardIDForRequest)
		}
	}

	if currentRole == RoleSlave {
		if isWriteOperation {
			log.Printf("Slave node received WRITE op (%s) for '%s.%s' (data shard %d), forwarding to master.", req.Operation, req.DBName, req.Table, shardIDForRequest)
			forwardRequestToMaster(w, r)
			return
		}

		slaveOwnsShardID := calculateShardID(config.SelfURL)
		if shardIDForRequest != slaveOwnsShardID {
			log.Printf("Slave (serves shard %d) received READ request for data in shard %d of '%s.%s'. Denying access.",
				slaveOwnsShardID, shardIDForRequest, req.DBName, req.Table)
			json.NewEncoder(w).Encode(Response{
				Success: false,
				Message: fmt.Sprintf("This slave node (serves shard %d) does not handle requests for data in shard %d.", slaveOwnsShardID, shardIDForRequest),
			})
			return
		}
		log.Printf("Slave (serves shard %d) handling READ request for its shard for '%s.%s'.", slaveOwnsShardID, req.DBName, req.Table)
	}

	dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		config.MySQL.User, config.MySQL.Password, config.MySQL.Host, config.MySQL.Port, req.DBName))
	if err != nil {
		log.Printf("Error connecting to database '%s': %v", req.DBName, err)
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Error connecting to DB: " + err.Error()})
		return
	}
	defer dbConn.Close()

	var result interface{}
	var execErr error

	switch req.Operation {
	case "create":
		if req.Data == nil {
			json.NewEncoder(w).Encode(Response{Success: false, Message: "Data required for create"})
			return
		}
		result, execErr = executeCreate(dbConn, req.Table, req.Data)
	case "read":
		result, execErr = executeRead(dbConn, req.Table, req.Where)
	case "update":
		if req.Data == nil || req.Where == nil {
			json.NewEncoder(w).Encode(Response{Success: false, Message: "Data and where required for update"})
			return
		}
		result, execErr = executeUpdate(dbConn, req.Table, req.Data, req.Where)
	case "delete":
		if req.Where == nil {
			json.NewEncoder(w).Encode(Response{Success: false, Message: "Where required for delete"})
			return
		}
		result, execErr = executeDelete(dbConn, req.Table, req.Where)
	default:
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Invalid operation"})
		return
	}

	if execErr != nil {
		log.Printf("Error executing %s on '%s.%s': %v", req.Operation, req.DBName, req.Table, execErr)
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Error executing operation: " + execErr.Error()})
		return
	}

	if isWriteOperation && currentRole == RoleMaster {
		log.Printf("Master executed %s on '%s.%s' for data shard %d. Initiating HTTP replication signal.", req.Operation, req.DBName, req.Table, shardIDForRequest)
		replicateToNodes(map[string]interface{}{
			"operation":     req.Operation,
			"dbName":        req.DBName,
			"table":         req.Table,
			"data":          req.Data,
			"where":         req.Where,
			"shardId":       float64(shardIDForRequest),
			"shardKeyValue": req.ShardKeyValue,
		})
	}

	json.NewEncoder(w).Encode(Response{Success: true, Result: result})
}
