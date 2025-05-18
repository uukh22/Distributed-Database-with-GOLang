package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

func linkTablesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if db == nil {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Database not connected"})
		return
	}

	if currentRole == RoleSlave {
		log.Println("Slave node received linkTables request, forwarding to master.")
		forwardRequestToMaster(w, r)
		return
	}

	// --- Master Logic ---
	var req struct {
		DBName         string `json:"dbName"`
		Table1         string `json:"table1"`
		Table2         string `json:"table2"`
		Column1        string `json:"column1"`
		Column2        string `json:"column2"`
		ConstraintName string `json:"constraintName,omitempty"`
	}
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Invalid request body: " + err.Error()})
		return
	}
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Invalid request: " + err.Error()})
		return
	}
	if req.DBName == "" || req.Table1 == "" || req.Table2 == "" || req.Column1 == "" || req.Column2 == "" {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "All fields required for linking tables"})
		return
	}

	safeDBName := SanitizeIdentifier(req.DBName)
	safeTable1 := SanitizeIdentifier(req.Table1)
	safeTable2 := SanitizeIdentifier(req.Table2)
	safeCol1 := SanitizeIdentifier(req.Column1)
	safeCol2 := SanitizeIdentifier(req.Column2)
	constraintName := SanitizeIdentifier(req.ConstraintName)
	if constraintName == "" {
		constraintName = SanitizeIdentifier(fmt.Sprintf("fk_%s_%s_%s_%s", safeTable2, safeCol2, safeTable1, safeCol1))
	}

	dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		config.MySQL.User, config.MySQL.Password, config.MySQL.Host, config.MySQL.Port, safeDBName))
	if err != nil {
		log.Printf("Error connecting to database '%s' on master for linkTables: %v", safeDBName, err)
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Error connecting to DB: " + err.Error()})
		return
	}
	defer dbConn.Close()

	query := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)",
		safeTable2, constraintName, safeCol2, safeTable1, safeCol1)

	_, err = dbConn.Exec(query)
	if err != nil {
		log.Printf("Error linking tables ('%s' to '%s') on master: %v", safeTable2, safeTable1, err)
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Error linking tables: " + err.Error()})
		return
	}
	log.Printf("Tables '%s' and '%s' linked successfully on master.", safeTable2, safeTable1)

	shardID := calculateShardID(safeDBName + "." + safeTable2)
	replicateToNodes(map[string]interface{}{
		"operation": "link_tables",
		"dbName":    safeDBName,
		"table1":    safeTable1,
		"table2":    safeTable2,
		"shardId":   float64(shardID),
	})

	json.NewEncoder(w).Encode(Response{Success: true, Message: "Tables linked successfully"})
}
