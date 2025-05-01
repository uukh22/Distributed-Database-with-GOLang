package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

const (
	RoleMaster      = "master"
	RoleSlave       = "slave"
	ElectionTimeout = 5 * time.Second
	// MySQL connection details - adjust these as needed
	MySQLUser     = "root"
	MySQLPassword = "root"
	MySQLHost     = "127.0.0.1"
	MySQLPort     = "3306"
)

// Node represents a database node in the cluster
type Node struct {
	ID        string    `json:"id"`
	Role      string    `json:"role"`
	URL       string    `json:"url"`
	IsHealthy bool      `json:"isHealthy"`
	LastSeen  time.Time `json:"lastSeen"`
}

// SystemState tracks the current state of the cluster
type SystemState struct {
	CurrentMaster string
	Nodes         []*Node
}

// Response struct for API responses
type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Result  interface{} `json:"result,omitempty"`
}

var (
	db          *sql.DB
	nodes       = make(map[string]*Node)
	state       SystemState
	stateMutex  = &sync.Mutex{}
	currentRole string
	masterURL   string
	selfURL     string
)

func main() {
	// Default values if not provided as arguments
	selfURL = "http://192.168.84.229:8080"
	masterURL = "http://192.168.84.54:8080"

	// Override with command line arguments if provided
	if len(os.Args) >= 2 {
		selfURL = os.Args[1]
	}
	if len(os.Args) >= 3 {
		masterURL = os.Args[2]
	}

	log.Printf("Starting node with selfURL=%s, masterURL=%s", selfURL, masterURL)

	initializeNode()
	go startHTTPServer()
	go monitorMaster()
	go heartbeat()

	// Keep the main goroutine alive
	select {}
}

func initializeNode() {
	var err error
	// Build MySQL connection string
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/", MySQLUser, MySQLPassword, MySQLHost, MySQLPort)
	log.Printf("Connecting to MySQL with: %s", connStr)

	db, err = sql.Open("mysql", connStr)
	if err != nil {
		log.Printf("Database connection failed: %v", err)
		// Continue execution even if database connection fails
		// This allows the API server to start and provide diagnostic information
	} else {
		// Set connection pool parameters
		db.SetMaxOpenConns(25)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(5 * time.Minute)

		// Test the connection
		err = db.Ping()
		if err != nil {
			log.Printf("Database ping failed: %v", err)
		} else {
			log.Println("Successfully connected to MySQL database")
		}
	}

	if selfURL == masterURL {
		currentRole = RoleMaster
		state.CurrentMaster = selfURL
		registerNode(selfURL, RoleMaster)
		log.Println("Initialized as MASTER node")
		if db != nil {
			configureMaster(db)
		}
	} else {
		currentRole = RoleSlave
		state.CurrentMaster = masterURL
		registerNode(selfURL, RoleSlave)
		registerNode(masterURL, RoleMaster)
		log.Println("Initialized as SLAVE node with master:", masterURL)

		// Extract host and port from masterURL
		parts := strings.Split(masterURL, ":")
		if len(parts) >= 3 && db != nil {
			host := strings.TrimPrefix(parts[1], "//")
			//port, _ := strconv.Atoi(parts[2])
			configureSlave(db, host, 3306)
		}
	}
}

func startHTTPServer() {
	r := mux.NewRouter()
	r.Use(corsMiddleware)
	r.Use(loggingMiddleware)

	// Cluster management endpoints
	r.HandleFunc("/api/nodes", listNodes).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/health", healthCheck).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/election", electionHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/new-master", newMasterHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/node-role", nodeRoleHandler).Methods("GET", "OPTIONS")

	// Database management endpoints
	r.HandleFunc("/api/create-db", createDatabaseHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/list-databases", listDatabasesHandler).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/drop-db", dropDatabaseHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/create-table", createTableHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/list-tables", listTablesHandler).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/drop-table", dropTableHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/link-tables", linkTablesHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/crud", crudHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/replicate", replicationHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/setup-replication", setupReplicationHandler).Methods("POST", "OPTIONS")

	// Serve frontend files
	r.PathPrefix("/").Handler(http.FileServer(http.Dir("./frontend")))

	// Extract port from selfURL
	parts := strings.Split(selfURL, ":")
	port := "8080"
	if len(parts) > 2 {
		port = parts[2]
	}

	log.Println("Starting server on port", port)
	log.Fatal(http.ListenAndServe(":"+port, r))
}

func registerNode(url, role string) {
	stateMutex.Lock()
	defer stateMutex.Unlock()

	if _, exists := nodes[url]; !exists {
		node := &Node{
			ID:        fmt.Sprintf("node-%d", len(nodes)+1),
			Role:      role,
			URL:       url,
			IsHealthy: true,
			LastSeen:  time.Now(),
		}
		nodes[url] = node
		state.Nodes = append(state.Nodes, node)
	}
}

func monitorMaster() {
	for {
		time.Sleep(2 * time.Second)
		if currentRole == RoleSlave && !checkNodeHealth(state.CurrentMaster) {
			log.Println("Master is down! Starting election...")
			startElection()
		}
	}
}

func startElection() {
	stateMutex.Lock()
	defer stateMutex.Unlock()

	for _, node := range nodes {
		if node.Role == RoleSlave && node.URL != selfURL && checkNodeHealth(node.URL) {
			if sendElectionRequest(node.URL) {
				return
			}
		}
	}

	promoteToMaster()
}

func promoteToMaster() {
	log.Println("Promoting self to master")
	currentRole = RoleMaster
	state.CurrentMaster = selfURL
	nodes[selfURL].Role = RoleMaster
	if db != nil {
		configureMaster(db)
	}

	for _, node := range nodes {
		if node.URL != selfURL && node.IsHealthy {
			go sendNewMasterNotification(node.URL)
		}
	}
}
func configureMaster(db *sql.DB) error {
	// Create user
	_, err := db.Exec(`CREATE USER IF NOT EXISTS 'replica'@'%' IDENTIFIED BY 'password'`)
	if err != nil {
		log.Printf("Error creating user: %v", err)
		return err
	}

	// Grant replication privileges
	_, err = db.Exec(`GRANT REPLICATION SLAVE ON *.* TO 'replica'@'%'`)
	if err != nil {
		log.Printf("Error granting privileges: %v", err)
		return err
	}

	// Flush privileges
	_, err = db.Exec(`FLUSH PRIVILEGES`)
	if err != nil {
		log.Printf("Error flushing privileges: %v", err)
		return err
	}

	return nil
}
func configureSlave(db *sql.DB, masterHost string, masterPort int) error {
	// Stop any existing replication first
	_, err := db.Exec("STOP SLAVE;")
	if err != nil {
		log.Printf("Warning: Failed to stop slave - %v", err)
	}

	// Proceed with configuration
	masterDB, err := sql.Open("mysql", fmt.Sprintf("replica:1012003KhaleD@tcp(%s:%d)/", masterHost, masterPort))
	if err != nil {
		log.Printf("Error connecting to master: %v", err)
		return fmt.Errorf("failed to connect to master: %w", err)
	}
	defer masterDB.Close()

	err = masterDB.Ping()
	if err != nil {
		log.Printf("Master MySQL ping failed: %v", err)
		return fmt.Errorf("master ping failed: %w", err)
	}

	var (
		logFile    string
		logPos     int
		binlogDoDB string
		ignoredDB  string
	)

	err = masterDB.QueryRow("SHOW MASTER STATUS").Scan(&logFile, &logPos, &binlogDoDB, &ignoredDB)
	if err != nil {
		log.Printf("Error getting master status: %v", err)
		return fmt.Errorf("failed to get master status: %w", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
        CHANGE MASTER TO
        MASTER_HOST='%s',
        MASTER_PORT=%d,
        MASTER_USER='replica',
        MASTER_PASSWORD='1012003KhaleD',
        MASTER_LOG_FILE='%s',
        MASTER_LOG_POS=%d;
    `, masterHost, masterPort, logFile, logPos))
	if err != nil {
		log.Printf("Error configuring slave: %v", err)
		return fmt.Errorf("failed to configure slave: %w", err)
	}

	_, err = db.Exec("START SLAVE;")
	if err != nil {
		log.Printf("Error starting slave: %v", err)
		return fmt.Errorf("failed to start slave: %w", err)
	}

	log.Println("Slave configured successfully")
	return nil
}
func checkNodeHealth(url string) bool {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url + "/api/health")
	return err == nil && resp.StatusCode == http.StatusOK
}

func replicateToNodes(operation map[string]interface{}) {
	for _, node := range nodes {
		if node.Role == RoleSlave && node.URL != selfURL && node.IsHealthy {
			go func(url string) {
				jsonData, _ := json.Marshal(operation)
				client := http.Client{Timeout: 5 * time.Second}
				_, err := client.Post(url+"/api/replicate", "application/json", strings.NewReader(string(jsonData)))
				if err != nil {
					log.Printf("Replication to %s failed: %v\n", url, err)
				}
			}(node.URL)
		}
	}
}

// API Handlers

func nodeRoleHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	result := map[string]string{
		"role": currentRole,
	}

	if currentRole == RoleSlave {
		result["masterUrl"] = state.CurrentMaster
	}

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Result:  result,
	})
}

func listNodes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	stateMutex.Lock()
	defer stateMutex.Unlock()

	var nodeList []Node
	for _, node := range nodes {
		nodeList = append(nodeList, *node)
	}

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Result:  nodeList,
	})
}

func createDatabaseHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Check if database is connected
	if db == nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database connection not established. Please check MySQL settings.",
		})
		return
	}

	// For write operations, redirect to master if we're a slave
	if currentRole != RoleMaster {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: fmt.Sprintf("This is a slave node. Please send write operations to the master: %s", state.CurrentMaster),
		})
		return
	}

	var req struct {
		DBName string `json:"dbName"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Invalid request format",
		})
		return
	}

	if req.DBName == "" {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database name is required",
		})
		return
	}

	_, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + req.DBName)
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error creating database: " + err.Error(),
		})
		return
	}

	// Replicate to slave nodes
	replicateToNodes(map[string]interface{}{
		"operation": "create_db",
		"dbName":    req.DBName,
	})

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: fmt.Sprintf("Database '%s' created successfully", req.DBName),
	})
}

func dropDatabaseHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Check if database is connected
	if db == nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database connection not established. Please check MySQL settings.",
		})
		return
	}

	// For write operations, redirect to master if we're a slave
	if currentRole != RoleMaster {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: fmt.Sprintf("This is a slave node. Please send write operations to the master: %s", state.CurrentMaster),
		})
		return
	}

	var req struct {
		DBName string `json:"dbName"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Invalid request format",
		})
		return
	}

	if req.DBName == "" {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database name is required",
		})
		return
	}

	// Prevent dropping system databases
	if req.DBName == "information_schema" || req.DBName == "mysql" || req.DBName == "performance_schema" || req.DBName == "sys" {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Cannot drop system database",
		})
		return
	}

	_, err := db.Exec("DROP DATABASE IF EXISTS " + req.DBName)
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error dropping database: " + err.Error(),
		})
		return
	}

	// Replicate to slave nodes
	replicateToNodes(map[string]interface{}{
		"operation": "drop_db",
		"dbName":    req.DBName,
	})

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: fmt.Sprintf("Database '%s' dropped successfully", req.DBName),
	})
}

func dropTableHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Check if database is connected
	if db == nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database connection not established. Please check MySQL settings.",
		})
		return
	}

	// For write operations, redirect to master if we're a slave
	if currentRole != RoleMaster {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: fmt.Sprintf("This is a slave node. Please send write operations to the master: %s", state.CurrentMaster),
		})
		return
	}

	var req struct {
		DBName    string `json:"dbName"`
		TableName string `json:"tableName"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Invalid request format",
		})
		return
	}

	if req.DBName == "" || req.TableName == "" {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database name and table name are required",
		})
		return
	}

	// Connect to the specific database
	dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		MySQLUser, MySQLPassword, MySQLHost, MySQLPort, req.DBName))
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error connecting to database: " + err.Error(),
		})
		return
	}
	defer dbConn.Close()

	_, err = dbConn.Exec("DROP TABLE IF EXISTS " + req.TableName)
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error dropping table: " + err.Error(),
		})
		return
	}

	// Replicate to slave nodes
	replicateToNodes(map[string]interface{}{
		"operation": "drop_table",
		"dbName":    req.DBName,
		"tableName": req.TableName,
	})

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: fmt.Sprintf("Table '%s' dropped successfully", req.TableName),
	})
}

func listDatabasesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Check if database is connected
	if db == nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database connection not established. Please check MySQL settings.",
		})
		return
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
		// Skip system databases
		if dbName != "information_schema" && dbName != "mysql" && dbName != "performance_schema" && dbName != "sys" {
			databases = append(databases, dbName)
		}
	}

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Result:  databases,
	})
}

func createTableHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Check if database is connected
	if db == nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database connection not established. Please check MySQL settings.",
		})
		return
	}

	if currentRole != RoleMaster {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: fmt.Sprintf("This is a slave node. Please send write operations to the master: %s", state.CurrentMaster),
		})
		return
	}

	var req struct {
		DBName    string            `json:"dbName"`
		TableName string            `json:"tableName"`
		Columns   map[string]string `json:"columns"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Invalid request format",
		})
		return
	}

	if req.DBName == "" || req.TableName == "" || len(req.Columns) == 0 {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database name, table name, and columns are required",
		})
		return
	}

	// Connect to the specific database
	dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		MySQLUser, MySQLPassword, MySQLHost, MySQLPort, req.DBName))
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error connecting to database: " + err.Error(),
		})
		return
	}
	defer dbConn.Close()

	// Build the CREATE TABLE query
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", req.TableName)
	columnDefs := []string{}
	primaryKeys := []string{}

	for name, dataType := range req.Columns {
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", name, dataType))
		if strings.HasSuffix(name, "_id") || strings.ToLower(name) == "id" {
			primaryKeys = append(primaryKeys, name)
		}
	}

	query += strings.Join(columnDefs, ", ")

	if len(primaryKeys) > 0 {
		query += ", PRIMARY KEY (" + strings.Join(primaryKeys, ", ") + ")"
	}

	query += ")"

	_, err = dbConn.Exec(query)
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error creating table: " + err.Error(),
		})
		return
	}

	// Replicate to slave nodes
	replicateToNodes(map[string]interface{}{
		"operation": "create_table",
		"dbName":    req.DBName,
		"tableName": req.TableName,
		"query":     query,
	})

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: fmt.Sprintf("Table '%s' created successfully", req.TableName),
	})
}

func listTablesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Check if database is connected
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

	dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		MySQLUser, MySQLPassword, MySQLHost, MySQLPort, dbName))
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

		// Get columns for this table
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
			var defaultVal, extra sql.NullString // استخدام sql.NullString للتعامل مع القيم NULL
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

		tables = append(tables, map[string]interface{}{
			"name":    tableName,
			"columns": columns,
		})
	}

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Result:  tables,
	})
}

func linkTablesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Check if database is connected
	if db == nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database connection not established. Please check MySQL settings.",
		})
		return
	}

	if currentRole != RoleMaster {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: fmt.Sprintf("This is a slave node. Please send write operations to the master: %s", state.CurrentMaster),
		})
		return
	}

	var req struct {
		DBName         string `json:"dbName"`
		Table1         string `json:"table1"`
		Table2         string `json:"table2"`
		Column1        string `json:"column1"`
		Column2        string `json:"column2"`
		ConstraintType string `json:"constraintType"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Invalid request format",
		})
		return
	}

	if req.DBName == "" || req.Table1 == "" || req.Table2 == "" || req.Column1 == "" || req.Column2 == "" {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "All fields are required",
		})
		return
	}

	dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		MySQLUser, MySQLPassword, MySQLHost, MySQLPort, req.DBName))
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error connecting to database: " + err.Error(),
		})
		return
	}
	defer dbConn.Close()

	constraintName := fmt.Sprintf("fk_%s_%s", req.Table1, req.Table2)
	query := fmt.Sprintf(`
		ALTER TABLE %s
		ADD CONSTRAINT %s
		FOREIGN KEY (%s) REFERENCES %s(%s);
	`, req.Table2, constraintName, req.Column2, req.Table1, req.Column1)

	_, err = dbConn.Exec(query)
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error linking tables: " + err.Error(),
		})
		return
	}

	// Replicate to slave nodes
	replicateToNodes(map[string]interface{}{
		"operation": "link_tables",
		"dbName":    req.DBName,
		"query":     query,
	})

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Tables linked successfully!",
	})
}

func crudHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Check if database is connected
	if db == nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database connection not established. Please check MySQL settings.",
		})
		return
	}

	var req struct {
		DBName    string                 `json:"dbName"`
		Operation string                 `json:"operation"`
		Table     string                 `json:"table"`
		Data      map[string]interface{} `json:"data"`
		Where     map[string]interface{} `json:"where"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Invalid request format",
		})
		return
	}

	// For write operations, redirect to master if we're a slave
	if req.Operation != "read" && currentRole != RoleMaster {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: fmt.Sprintf("This is a slave node. Please send write operations to the master: %s", state.CurrentMaster),
		})
		return
	}

	if req.DBName == "" || req.Table == "" || req.Operation == "" {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database name, table name, and operation are required",
		})
		return
	}

	// Connect to the specific database
	dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		MySQLUser, MySQLPassword, MySQLHost, MySQLPort, req.DBName))
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error connecting to database: " + err.Error(),
		})
		return
	}
	defer dbConn.Close()

	var result interface{}

	switch req.Operation {
	case "create":
		if req.Data == nil || len(req.Data) == 0 {
			json.NewEncoder(w).Encode(Response{
				Success: false,
				Message: "Data is required for create operation",
			})
			return
		}
		result, err = executeCreate(dbConn, req.Table, req.Data)

	case "read":
		result, err = executeRead(dbConn, req.Table, req.Where)

	case "update":
		if req.Data == nil || len(req.Data) == 0 || req.Where == nil || len(req.Where) == 0 {
			json.NewEncoder(w).Encode(Response{
				Success: false,
				Message: "Data and where conditions are required for update operation",
			})
			return
		}
		result, err = executeUpdate(dbConn, req.Table, req.Data, req.Where)

	case "delete":
		if req.Where == nil || len(req.Where) == 0 {
			json.NewEncoder(w).Encode(Response{
				Success: false,
				Message: "Where conditions are required for delete operation",
			})
			return
		}
		result, err = executeDelete(dbConn, req.Table, req.Where)

	default:
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Invalid operation",
		})
		return
	}

	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error executing operation: " + err.Error(),
		})
		return
	}

	// Replicate write operations to slave nodes
	if req.Operation != "read" {
		replicateToNodes(map[string]interface{}{
			"operation": req.Operation,
			"dbName":    req.DBName,
			"table":     req.Table,
			"data":      req.Data,
			"where":     req.Where,
		})
	}

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Result:  result,
	})
}

func replicationHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Check if database is connected
	if db == nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database connection not established. Please check MySQL settings.",
		})
		return
	}

	var req map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid replication request", http.StatusBadRequest)
		return
	}

	operation, ok := req["operation"].(string)
	if !ok {
		http.Error(w, "Invalid operation in replication request", http.StatusBadRequest)
		return
	}

	dbName, ok := req["dbName"].(string)
	if !ok {
		http.Error(w, "Missing database name in replication request", http.StatusBadRequest)
		return
	}

	var err error

	switch operation {
	case "create_db":
		_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + dbName)

	case "drop_db":
		_, err = db.Exec("DROP DATABASE IF EXISTS " + dbName)

	case "create_table":
		query, ok := req["query"].(string)
		if !ok {
			http.Error(w, "Missing query in replication request", http.StatusBadRequest)
			return
		}

		dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
			MySQLUser, MySQLPassword, MySQLHost, MySQLPort, dbName))
		if err != nil {
			http.Error(w, "Error connecting to database: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer dbConn.Close()

		_, err = dbConn.Exec(query)

	case "drop_table":
		tableName, ok := req["tableName"].(string)
		if !ok {
			http.Error(w, "Missing table name in replication request", http.StatusBadRequest)
			return
		}

		dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
			MySQLUser, MySQLPassword, MySQLHost, MySQLPort, dbName))
		if err != nil {
			http.Error(w, "Error connecting to database: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer dbConn.Close()

		_, err = dbConn.Exec("DROP TABLE IF EXISTS " + tableName)

	case "link_tables":
		query, ok := req["query"].(string)
		if !ok {
			http.Error(w, "Missing query in replication request", http.StatusBadRequest)
			return
		}

		dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
			MySQLUser, MySQLPassword, MySQLHost, MySQLPort, dbName))
		if err != nil {
			http.Error(w, "Error connecting to database: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer dbConn.Close()

		_, err = dbConn.Exec(query)

	case "create", "update", "delete":
		table, ok := req["table"].(string)
		if !ok {
			http.Error(w, "Missing table in replication request", http.StatusBadRequest)
			return
		}

		dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
			MySQLUser, MySQLPassword, MySQLHost, MySQLPort, dbName))
		if err != nil {
			http.Error(w, "Error connecting to database: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer dbConn.Close()

		data, _ := req["data"].(map[string]interface{})
		where, _ := req["where"].(map[string]interface{})

		switch operation {
		case "create":
			_, err = executeCreate(dbConn, table, data)
		case "update":
			_, err = executeUpdate(dbConn, table, data, where)
		case "delete":
			_, err = executeDelete(dbConn, table, where)
		}
	}

	if err != nil {
		http.Error(w, "Replication failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Replication successful",
	})
}

func setupReplicationHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Check if database is connected
	if db == nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database connection not established. Please check MySQL settings.",
		})
		return
	}

	var req struct {
		MasterHost string `json:"masterHost"`
		MasterPort int    `json:"masterPort"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Invalid request format",
		})
		return
	}

	if req.MasterHost == "" || req.MasterPort == 0 {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Master host and port are required",
		})
		return
	}

	// Configure replication
	err := configureSlave(db, req.MasterHost, req.MasterPort)
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error setting up replication: " + err.Error(),
		})
		return
	}

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Replication setup successfully",
	})
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Check if database is connected
	if db == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database connection not established. Please check MySQL settings.",
		})
		return
	}

	if err := db.Ping(); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database connection failed: " + err.Error(),
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Server is healthy",
	})
}

func electionHandler(w http.ResponseWriter, r *http.Request) {
	if currentRole == RoleMaster {
		w.WriteHeader(http.StatusOK)
		return
	}

	stateMutex.Lock()
	defer stateMutex.Unlock()

	if time.Since(nodes[state.CurrentMaster].LastSeen) > ElectionTimeout {
		promoteToMaster()
	}
	w.WriteHeader(http.StatusOK)
}

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
	if node, exists := nodes[req.MasterURL]; exists {
		node.Role = RoleMaster
	} else {
		registerNode(req.MasterURL, RoleMaster)
	}

	if selfURL != req.MasterURL {
		currentRole = RoleSlave
		nodes[selfURL].Role = RoleSlave
	}

	w.WriteHeader(http.StatusOK)
}

func sendElectionRequest(url string) bool {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Post(url+"/api/election", "application/json", nil)
	return err == nil && resp.StatusCode == http.StatusOK
}

func sendNewMasterNotification(url string) {
	client := http.Client{Timeout: 2 * time.Second}
	payload := fmt.Sprintf(`{"masterURL": "%s"}`, selfURL)
	client.Post(url+"/api/new-master", "application/json", strings.NewReader(payload))
}

func heartbeat() {
	for {
		time.Sleep(3 * time.Second)
		stateMutex.Lock()
		for _, node := range nodes {
			if node.URL != selfURL {
				node.IsHealthy = checkNodeHealth(node.URL)
				if node.IsHealthy {
					node.LastSeen = time.Now()
				}
			}
		}
		stateMutex.Unlock()
	}
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		log.Printf("Request: %s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
		log.Printf("Response: %s %s - %s", r.Method, r.URL.Path, time.Since(start))
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Helper functions for CRUD operations
func executeCreate(db *sql.DB, table string, data map[string]interface{}) (interface{}, error) {
	columns := strings.Join(keys(data), ", ")
	placeholders := strings.Join(createPlaceholders(len(data)), ", ")
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, columns, placeholders)

	values := make([]interface{}, 0, len(data))
	for _, k := range keys(data) {
		values = append(values, data[k])
	}

	result, err := db.Exec(query, values...)
	if err != nil {
		return nil, err
	}

	id, _ := result.LastInsertId()
	return map[string]interface{}{"id": id}, nil
}

func executeRead(db *sql.DB, table string, where map[string]interface{}) (interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %s", table)

	values := make([]interface{}, 0)
	if where != nil && len(where) > 0 {
		whereClause, whereValues := buildWhereClause(where)
		query += " WHERE " + whereClause
		values = whereValues
	}

	rows, err := db.Query(query, values...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return rowsToJSON(rows)
}

func executeUpdate(db *sql.DB, table string, data map[string]interface{}, where map[string]interface{}) (interface{}, error) {
	setClause, setValues := buildSetClause(data)
	whereClause, whereValues := buildWhereClause(where)

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, setClause, whereClause)

	// Combine values from SET and WHERE clauses
	values := append(setValues, whereValues...)

	result, err := db.Exec(query, values...)
	if err != nil {
		return nil, err
	}

	rowsAffected, _ := result.RowsAffected()
	return map[string]interface{}{"rowsAffected": rowsAffected}, nil
}

func executeDelete(db *sql.DB, table string, where map[string]interface{}) (interface{}, error) {
	whereClause, whereValues := buildWhereClause(where)
	query := fmt.Sprintf("DELETE FROM %s WHERE %s", table, whereClause)

	result, err := db.Exec(query, whereValues...)
	if err != nil {
		return nil, err
	}

	rowsAffected, _ := result.RowsAffected()
	return map[string]interface{}{"rowsAffected": rowsAffected}, nil
}

// Utility functions
func keys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func createPlaceholders(n int) []string {
	placeholders := make([]string, n)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return placeholders
}

func buildSetClause(data map[string]interface{}) (string, []interface{}) {
	clauses := make([]string, 0, len(data))
	values := make([]interface{}, 0, len(data))

	for k, v := range data {
		clauses = append(clauses, fmt.Sprintf("%s = ?", k))
		values = append(values, v)
	}

	return strings.Join(clauses, ", "), values
}

func buildWhereClause(where map[string]interface{}) (string, []interface{}) {
	if where == nil || len(where) == 0 {
		return "1=1", []interface{}{}
	}

	clauses := make([]string, 0, len(where))
	values := make([]interface{}, 0, len(where))

	for k, v := range where {
		clauses = append(clauses, fmt.Sprintf("%s = ?", k))
		values = append(values, v)
	}

	return strings.Join(clauses, " AND "), values
}

func rowsToJSON(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	result := make([]map[string]interface{}, 0)

	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// Create a map for this row
		entry := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]

			// Handle []byte to string conversion for text fields
			b, ok := val.([]byte)
			if ok {
				entry[col] = string(b)
			} else {
				entry[col] = val
			}
		}

		result = append(result, entry)
	}

	return result, nil
}
