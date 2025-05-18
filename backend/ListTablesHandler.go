package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func listTablesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if db == nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database connection not established. Please check MySQL settings.",
		})
		return
	}

	dbName := r.URL.Query().Get("db")
	if dbName == "" {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database name is required",
		})
		return
	}

	dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		config.MySQL.User, config.MySQL.Password, config.MySQL.Host, config.MySQL.Port, dbName))
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error connecting to database: " + err.Error(),
		})
		return
	}
	defer dbConn.Close()

	rows, err := dbConn.Query("SHOW TABLES")
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error listing tables: " + err.Error(),
		})
		return
	}
	defer rows.Close()

	var tables []map[string]interface{}
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			json.NewEncoder(w).Encode(Response{
				Success: false,
				Message: "Error scanning table name: " + err.Error(),
			})
			return
		}

		columnsRows, err := dbConn.Query(fmt.Sprintf("DESCRIBE %s", tableName))
		if err != nil {
			json.NewEncoder(w).Encode(Response{
				Success: false,
				Message: "Error getting table columns: " + err.Error(),
			})
			return
		}

		var columns []map[string]string
		for columnsRows.Next() {
			var field, fieldType, null, key string
			var defaultVal, extra sql.NullString
			if err := columnsRows.Scan(&field, &fieldType, &null, &key, &defaultVal, &extra); err != nil {
				columnsRows.Close()
				json.NewEncoder(w).Encode(Response{
					Success: false,
					Message: "Error scanning column info: " + err.Error(),
				})
				return
			}
			columns = append(columns, map[string]string{
				"name": field,
				"type": fieldType,
			})
		}
		columnsRows.Close()

		var shardID int
		var shardKey sql.NullString
		err = db.QueryRow("SELECT shard_id, shard_key FROM cluster.table_shards WHERE db_name = ? AND table_name = ?",
			dbName, tableName).Scan(&shardID, &shardKey)
		if err != nil && err != sql.ErrNoRows {
			log.Printf("Error retrieving shard info: %v", err)
		}

		tableInfo := map[string]interface{}{
			"name":     tableName,
			"columns":  columns,
			"shardId":  shardID,
			"shardKey": "",
		}
		if shardKey.Valid {
			tableInfo["shardKey"] = shardKey.String
		}

		tables = append(tables, tableInfo)
	}

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Result:  tables,
	})
}
