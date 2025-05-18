package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
)

func listDatabasesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if db == nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database connection not established. Please check MySQL settings.",
		})
		return
	}

	if currentRole == RoleSlave {
		rows, err := db.Query("SHOW SLAVE STATUS")
		if err != nil {
			log.Printf("Error checking slave status: %v", err)
		} else {
			defer rows.Close()
			if rows.Next() {
				columns, _ := rows.Columns()
				values := make([]sql.RawBytes, len(columns))
				scanArgs := make([]interface{}, len(values))
				for i := range values {
					scanArgs[i] = &values[i]
				}
				if err := rows.Scan(scanArgs...); err != nil {
					log.Printf("Error scanning slave status: %v", err)
				} else {
					var slaveIORunning, slaveSQLRunning, lastError string
					var secondsBehindMaster sql.NullInt64
					for i, col := range columns {
						switch col {
						case "Slave_IO_Running":
							slaveIORunning = string(values[i])
						case "Slave_SQL_Running":
							slaveSQLRunning = string(values[i])
						case "Last_Error":
							lastError = string(values[i])
						case "Seconds_Behind_Master":
							if values[i] != nil {
								var val int64
								fmt.Sscanf(string(values[i]), "%d", &val)
								secondsBehindMaster = sql.NullInt64{Int64: val, Valid: true}
							}
						}
					}
					log.Printf("Replication Status: IO_Running=%s, SQL_Running=%s, Last_Error=%s, Seconds_Behind=%v",
						slaveIORunning, slaveSQLRunning, lastError, secondsBehindMaster)
					if slaveIORunning != "Yes" || slaveSQLRunning != "Yes" {
						log.Printf("Replication issue detected, attempting to restart...")
						parts := strings.Split(config.MasterURL, ":")
						if len(parts) >= 3 {
							host := strings.TrimPrefix(parts[1], "//")
							if err := configureSlave(db, host, 3306); err != nil {
								log.Printf("Failed to restart replication: %v", err)
							}
						}
					}
				}
			}
		}
	}

	rows, err := db.Query("SHOW DATABASES")
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error listing databases: " + err.Error(),
		})
		return
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			json.NewEncoder(w).Encode(Response{
				Success: false,
				Message: "Error scanning database name: " + err.Error(),
			})
			return
		}
		if dbName != "information_schema" && dbName != "mysql" && dbName != "performance_schema" && dbName != "sys" {
			databases = append(databases, dbName)
		}
	}

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Result:  databases,
	})
}
