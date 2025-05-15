package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

// Constants for node roles and election timeout
const (
	RoleMaster      = "master"
	RoleSlave       = "slave"
	ElectionTimeout = 5 * time.Second
)

// Config holds the configuration loaded from config.json
type Config struct {
	SelfURL     string            `json:"self_url"`
	MasterURL   string            `json:"master_url"`
	MySQL       MySQLConfig       `json:"mysql"`
	Replication ReplicationConfig `json:"replication"`
	ShardCount  int               `json:"shard_count"`
}

// MySQLConfig holds MySQL connection details
type MySQLConfig struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     string `json:"port"`
}

// ReplicationConfig holds replication user credentials
type ReplicationConfig struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

// Node represents a node in the cluster
type Node struct {
	ID        string    `json:"id"`
	Role      string    `json:"role"`
	URL       string    `json:"url"`
	IsHealthy bool      `json:"isHealthy"`
	LastSeen  time.Time `json:"lastSeen"`
	ShardID   int       `json:"shardId"`
	CreatedAt time.Time `json:"createdAt"`
}

// SystemState tracks the current master and node list
type SystemState struct {
	CurrentMaster string
	Nodes         []*Node
}

// Response is the standard API response format
type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Result  interface{} `json:"result,omitempty"`
}

// Global variables
var (
	config      Config
	db          *sql.DB
	state       SystemState
	stateMutex  = &sync.Mutex{}
	currentRole string
)

// loadConfig loads and validates the configuration from config.json
func loadConfig() error {
	file, err := os.Open("config.json")
	if err != nil {
		return fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return fmt.Errorf("failed to decode config file: %w", err)
	}

	// Normalize URLs
	config.SelfURL = strings.TrimSuffix(config.SelfURL, "/")
	config.MasterURL = strings.TrimSuffix(config.MasterURL, "/")

	// Set defaults
	if config.SelfURL == "" {
		config.SelfURL = "http://localhost:8080"
	}
	if config.MasterURL == "" {
		config.MasterURL = config.SelfURL
	}
	if config.MySQL.User == "" {
		config.MySQL.User = "root"
	}
	if config.MySQL.Host == "" {
		config.MySQL.Host = "127.0.0.1"
	}
	if config.MySQL.Port == "" {
		config.MySQL.Port = "3306"
	}
	if config.ShardCount == 0 {
		config.ShardCount = 3
	}
	if config.Replication.User == "" {
		config.Replication.User = "replica"
	}
	// Validate URLs
	if !strings.HasPrefix(config.SelfURL, "http://") || !strings.HasPrefix(config.MasterURL, "http://") {
		return fmt.Errorf("self_url and master_url must start with http://")
	}

	return nil
}

// main is the entry point of the application
func main() {
	if err := loadConfig(); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if len(os.Args) >= 2 {
		config.SelfURL = strings.TrimSuffix(os.Args[1], "/")
	}
	if len(os.Args) >= 3 {
		config.MasterURL = strings.TrimSuffix(os.Args[2], "/")
	}

	log.Printf("Starting node with selfURL=%s, masterURL=%s", config.SelfURL, config.MasterURL)

	initializeNode()
	go startHTTPServer()
	go monitorMaster()
	go heartbeat()
	go registerWithMasterRetry()

	select {}
}

// initializeNode sets up the database and node role
func initializeNode() {
	var err error
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/?parseTime=true",
		config.MySQL.User, config.MySQL.Password, config.MySQL.Host, config.MySQL.Port)
	log.Printf("Connecting to MySQL with: %s", connStr)

	db, err = sql.Open("mysql", connStr)
	if err != nil {
		log.Printf("Database connection failed: %v", err)
	} else {
		db.SetMaxOpenConns(50)
		db.SetMaxIdleConns(10)
		db.SetConnMaxLifetime(3 * time.Minute)

		err = db.Ping()
		if err != nil {
			log.Printf("Database ping failed: %v", err)
			return
		}
		log.Println("Successfully connected to MySQL database")
	}

	_, err = db.Exec(`CREATE DATABASE IF NOT EXISTS cluster`)
	if err != nil {
		log.Printf("Failed to create cluster database: %v", err)
		return
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS cluster.nodes (
			id VARCHAR(50) PRIMARY KEY,
			role VARCHAR(20) NOT NULL,
			url VARCHAR(255) NOT NULL UNIQUE,
			is_healthy BOOLEAN NOT NULL,
			last_seen DATETIME NOT NULL,
			shard_id INT NOT NULL,
			created_at DATETIME NOT NULL
		)
	`)
	if err != nil {
		log.Printf("Failed to create nodes table: %v", err)
		return
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS cluster.table_shards (
			db_name VARCHAR(255) NOT NULL,
			table_name VARCHAR(255) NOT NULL,
			shard_id INT NOT NULL,
			shard_key VARCHAR(255),
			PRIMARY KEY (db_name, table_name)
		)
	`)
	if err != nil {
		log.Printf("Failed to create table_shards table: %v", err)
		return
	}

	log.Printf("Checking role: selfURL=%s, masterURL=%s", config.SelfURL, config.MasterURL)
	if config.SelfURL == config.MasterURL {
		currentRole = RoleMaster
		state.CurrentMaster = config.SelfURL
		log.Println("Registering as MASTER node")
		registerNode(config.SelfURL, RoleMaster, calculateShardID(config.SelfURL))
		var role string
		err = db.QueryRow("SELECT role FROM cluster.nodes WHERE url = ?", config.SelfURL).Scan(&role)
		if err != nil {
			log.Printf("Error verifying master role in database: %v", err)
		} else if role != RoleMaster {
			log.Printf("Master role mismatch in database: expected %s, got %s", RoleMaster, role)
			_, err = db.Exec("UPDATE cluster.nodes SET role = ? WHERE url = ?", RoleMaster, config.SelfURL)
			if err != nil {
				log.Printf("Error correcting master role: %v", err)
			}
		}
		log.Println("Initialized as MASTER node")
		if db != nil {
			configureMaster(db)
		}
	} else {
		currentRole = RoleSlave
		state.CurrentMaster = config.MasterURL
		//		shardID := calculateShardID(config.SelfURL)
		log.Println("Registering as SLAVE node")
		//		registerNode(config.SelfURL, RoleSlave, shardID)
		//		registerNode(config.MasterURL, RoleMaster, calculateShardID(config.MasterURL))
		log.Println("Initialized as SLAVE node with master:", config.MasterURL)

		parts := strings.Split(config.MasterURL, ":")
		if len(parts) >= 3 && db != nil {
			host := strings.TrimPrefix(parts[1], "//")
			configureSlave(db, host, 3306)
		}
	}
}

// calculateShardID computes a shard ID based on a key
func calculateShardID(key string) int {
	if config.ShardCount <= 0 {
		log.Printf("Warning: ShardCount in config is %d (not positive). Defaulting shardID to 0 for key '%s'.", config.ShardCount, key)
		return 0 // Avoid division by zero or negative.
	}
	hash := 0
	for _, char := range key {
		hash = (hash*31 + int(char)) % config.ShardCount // A slightly better hash
	}
	return (hash & 0x7fffffff) % config.ShardCount // Ensure positive result
}

// registerWithMaster registers this node with the master
func registerWithMaster() error {
	client := http.Client{Timeout: 30 * time.Second}
	payload := fmt.Sprintf(`{"url": "%s", "role": "%s", "shardId": %d}`, config.SelfURL, RoleSlave, calculateShardID(config.SelfURL))

	resp, err := client.Post(config.MasterURL+"/api/register", "application/json", strings.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to initiate registration POST request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("registration failed with status code %d: %s", resp.StatusCode, string(body))
	}

	var result Response
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to decode response: %w. Response body: %s", err, body)
	}

	if !result.Success {
		return fmt.Errorf("registration rejected by master: %s", result.Message)
	}

	return nil
}

// registerWithMasterRetry attempts to register with the master with retries
func registerWithMasterRetry() {
	maxRetries := 5
	retryDelay := 3 * time.Second

	for {
		for i := 0; i < maxRetries; i++ {
			err := registerWithMaster()
			if err == nil {
				log.Println("Successfully registered with master!")
				return
			}
			log.Printf("Registration attempt %d failed: %v", i+1, err)
			time.Sleep(retryDelay)
			retryDelay *= 2
		}
		log.Println("All registration attempts failed, retrying after 30 seconds")
		time.Sleep(30 * time.Second)
		retryDelay = 3 * time.Second
	}
}

// registerHandler handles node registration requests
func registerHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}

	var newNode struct {
		URL     string `json:"url"`
		Role    string `json:"role"`
		ShardID int    `json:"shardId"`
	}

	if err := json.Unmarshal(body, &newNode); err != nil {
		log.Printf("Error decoding registration: %v", err)
		http.Error(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	if newNode.URL == "" || newNode.Role == "" {
		log.Println("Missing required fields")
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	if newNode.URL == config.SelfURL && config.SelfURL == config.MasterURL {
		newNode.Role = RoleMaster
	}

	registerNode(newNode.URL, newNode.Role, newNode.ShardID)

	log.Printf("Successfully registered node: %s (%s) in shard %d", newNode.URL, newNode.Role, newNode.ShardID)
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Node registered successfully",
	})
}

// startHTTPServer sets up and starts the HTTP server
func startHTTPServer() {
	r := mux.NewRouter()
	r.Use(corsMiddleware)
	r.Use(loggingMiddleware)

	r.HandleFunc("/api/register", registerHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/nodes", listNodes).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/health", healthCheck).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/election", electionHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/new-master", newMasterHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/node-role", nodeRoleHandler).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/shutdown-slave", shutdownSlaveHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/shutdown", shutdownHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/slave-online", slaveOnlineHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/create-db", createDatabaseHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/list-databases", listDatabasesHandler).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/drop-db", dropDatabaseHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/create-table", createTableHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/slave-create-table", slaveCreateTableHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/list-tables", listTablesHandler).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/drop-table", dropTableHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/link-tables", linkTablesHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/crud", crudHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/replicate", replicationHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/setup-replication", setupReplicationHandler).Methods("POST", "OPTIONS")

	r.PathPrefix("/").Handler(http.FileServer(http.Dir("./frontend")))

	parts := strings.Split(config.SelfURL, ":")
	port := "8080"
	if len(parts) > 2 {
		port = parts[2]
	}

	log.Println("Starting server on port", port)
	log.Fatal(http.ListenAndServe(":"+port, r))
}

// registerNode adds or updates a node in the database
func registerNode(url, role string, shardID int) {
	stateMutex.Lock()
	defer stateMutex.Unlock()

	log.Printf("Registering node: url=%s, role=%s, shardID=%d", url, role, shardID)

	var exists bool
	err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM cluster.nodes WHERE url = ?)", url).Scan(&exists)
	if err != nil {
		log.Printf("Error checking node existence: %v", err)
		return
	}

	now := time.Now()
	if exists {
		_, err = db.Exec(`
			UPDATE cluster.nodes 
			SET role = ?, is_healthy = ?, last_seen = ?, shard_id = ?
			WHERE url = ?`,
			role, true, now, shardID, url)
		if err != nil {
			log.Printf("Error updating node in database: %v", err)
			return
		}
	} else {
		nodeID := fmt.Sprintf("node-%d", time.Now().UnixNano())
		_, err = db.Exec(`
			INSERT INTO cluster.nodes (id, role, url, is_healthy, last_seen, shard_id, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			nodeID, role, url, true, now, shardID, now)
		if err != nil {
			log.Printf("Error inserting node in database: %v", err)
			return
		}
	}

	loadNodesFromDB()
}

// loadNodesFromDB refreshes the in-memory node list from the database
func loadNodesFromDB() {
	rows, err := db.Query(`
		SELECT id, role, url, is_healthy, last_seen, shard_id, created_at 
		FROM cluster.nodes`)
	if err != nil {
		log.Printf("Error loading nodes from database: %v", err)
		return
	}
	defer rows.Close()

	nodes := make([]*Node, 0)
	for rows.Next() {
		var node Node
		err := rows.Scan(&node.ID, &node.Role, &node.URL, &node.IsHealthy,
			&node.LastSeen, &node.ShardID, &node.CreatedAt)
		if err != nil {
			log.Printf("Error scanning node: %v", err)
			continue
		}
		nodes = append(nodes, &node)
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating rows: %v", err)
	}

	state.Nodes = nodes
}

// monitorMaster checks the master's health and triggers an election if necessary
func monitorMaster() {
	for {
		time.Sleep(2 * time.Second)
		if currentRole == RoleSlave && !checkNodeHealth(state.CurrentMaster) {
			log.Println("Master is down! Starting election...")
			startElection()
		}
	}
}

// startElection initiates a leader election among healthy slaves
func startElection() {
	stateMutex.Lock()
	defer stateMutex.Unlock()

	rows, err := db.Query(`
		SELECT url 
		FROM cluster.nodes 
		WHERE role = ? AND is_healthy = ? 
		ORDER BY created_at ASC 
		LIMIT 1`, RoleSlave, true)
	if err != nil {
		log.Printf("Error querying for election: %v", err)
		return
	}
	defer rows.Close()

	var oldestNodeURL string
	if rows.Next() {
		err = rows.Scan(&oldestNodeURL)
		if err != nil {
			log.Printf("Error scanning oldest node: %v", err)
			return
		}
	}

	if oldestNodeURL == "" {
		log.Println("No healthy slaves available for election")
		return
	}

	if oldestNodeURL == config.SelfURL {
		promoteToMaster()
	} else if sendElectionRequest(oldestNodeURL) {
		return
	}
}

// promoteToMaster promotes this node to master
func promoteToMaster() {
	log.Println("Promoting self to master")
	currentRole = RoleMaster
	state.CurrentMaster = config.SelfURL

	_, err := db.Exec(`
		UPDATE cluster.nodes 
		SET role = ?, last_seen = ? 
		WHERE url = ?`, RoleMaster, time.Now(), config.SelfURL)
	if err != nil {
		log.Printf("Error updating node role in database: %v", err)
	}

	if db != nil {
		configureMaster(db)
	}

	loadNodesFromDB()
	for _, node := range state.Nodes {
		if node.URL != config.SelfURL && node.IsHealthy {
			go sendNewMasterNotification(node.URL)
		}
	}
}

func configureMaster(db *sql.DB) error {
	_, err := db.Exec(fmt.Sprintf(`CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s'`,
		config.Replication.User, config.Replication.Password))
	if err != nil {
		log.Printf("Error creating replication user: %v", err)
		return err
	}

	_, err = db.Exec(fmt.Sprintf(`GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '%s'@'%%'`,
		config.Replication.User))
	if err != nil {
		log.Printf("Error granting replication privileges: %v", err)
		return err
	}

	_, err = db.Exec(`FLUSH PRIVILEGES`)
	if err != nil {
		log.Printf("Error flushing privileges: %v", err)
		return err
	}

	// Verify binary logging
	rows, err := db.Query("SHOW MASTER STATUS")
	if err != nil {
		log.Printf("Error checking master status: %v", err)
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		log.Println("Binary logging is not enabled on master")
		return fmt.Errorf("binary logging not enabled")
	}

	log.Println("Master configured for replication")
	return nil
}

func configureSlave(db *sql.DB, masterHost string, masterPort int) error {
	// Stop any existing replication first
	_, err := db.Exec("STOP SLAVE;")
	if err != nil {
		log.Printf("Warning: Failed to stop slave - %v", err)
	}

	// Connect to master using credentials from config
	masterConnStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		config.Replication.User,
		config.Replication.Password,
		masterHost,
		masterPort,
	)
	masterDB, err := sql.Open("mysql", masterConnStr)
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
		// Add this variable to catch the extra column.
		executedGtidSet string
	)

	// Use the correct number of variables to match the columns returned by SHOW MASTER STATUS
	err = masterDB.QueryRow("SHOW MASTER STATUS").Scan(&logFile, &logPos, &binlogDoDB, &ignoredDB, &executedGtidSet)
	if err != nil {
		log.Printf("Error getting master status: %v", err)
		return fmt.Errorf("failed to get master status: %w", err)
	}

	changeMasterQuery := fmt.Sprintf(`
        CHANGE MASTER TO
        MASTER_HOST='%s',
        MASTER_PORT=%d,
        MASTER_USER='%s',
        MASTER_PASSWORD='%s',
        MASTER_LOG_FILE='%s',
        MASTER_LOG_POS=%d;
    `, masterHost, masterPort,
		config.Replication.User,
		config.Replication.Password,
		logFile, logPos)

	_, err = db.Exec(changeMasterQuery)
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

// checkNodeHealth checks if a node is responsive
func checkNodeHealth(url string) bool {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url + "/api/health")
	return err == nil && resp.StatusCode == http.StatusOK
}

// replicateToNodes sends replication operations to relevant slave nodes
func replicateToNodes(operationData map[string]interface{}) {
	shardIDInterface, shardIdOk := operationData["shardId"]
	if !shardIdOk {
		log.Printf("CRITICAL: replicateToNodes called without shardId in operationData: %+v. Skipping replication signal.", operationData)
		return // Or assign a default, but this indicates a programming error
	}
	// Ensure shardID is a number, as it's often float64 from JSON decoding
	var shardID float64
	switch v := shardIDInterface.(type) {
	case float64:
		shardID = v
	case int:
		shardID = float64(v)
	default:
		log.Printf("CRITICAL: replicateToNodes called with non-numeric shardId: %T, value: %v. Skipping.", shardIDInterface, shardIDInterface)
		return
	}

	stateMutex.Lock()
	nodesToReplicate := make([]*Node, 0, len(state.Nodes))
	for _, node := range state.Nodes {
		if node.Role == RoleSlave && node.URL != config.SelfURL && node.IsHealthy {
			nodesToReplicate = append(nodesToReplicate, node)
		}
	}
	stateMutex.Unlock()

	if len(nodesToReplicate) == 0 {
		log.Println("No healthy slave nodes found to send HTTP replication signal to.")
		return
	}

	jsonData, err := json.Marshal(operationData)
	if err != nil {
		log.Printf("Error marshaling replication data: %v. Data: %+v", err, operationData)
		return
	}

	log.Printf("Master initiating HTTP replication signal to %d slaves for operation: %s, data_shardId: %.0f",
		len(nodesToReplicate), operationData["operation"], shardID)

	for _, node := range nodesToReplicate {
		go func(slaveNode *Node, data []byte, opData map[string]interface{}) { // Pass opData for logging
			targetURL := slaveNode.URL + "/api/replicate"
			// Log the specific shardId being sent in this iteration
			log.Printf("Sending HTTP replication signal to slave %s (%s) for operation %s, data_shardId: %.0f",
				slaveNode.ID, slaveNode.URL, opData["operation"], opData["shardId"].(float64))

			client := http.Client{Timeout: 10 * time.Second}
			resp, err := client.Post(targetURL, "application/json", bytes.NewBuffer(data))
			if err != nil {
				log.Printf("HTTP replication signal to %s FAILED: %v", slaveNode.URL, err)
				setNodeHealthStatus(slaveNode.URL, false) // Mark slave as potentially unhealthy
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				bodyBytes, _ := io.ReadAll(resp.Body)
				log.Printf("HTTP replication signal to %s returned status %d. Response: %s", slaveNode.URL, resp.StatusCode, string(bodyBytes))
			} else {
				log.Printf("Successfully sent HTTP replication signal to %s (status %d)", slaveNode.URL, resp.StatusCode)
			}
		}(node, jsonData, operationData) // Pass operationData to goroutine
	}
}

func setNodeHealthStatus(nodeURL string, isHealthy bool) {
	if db == nil {
		log.Printf("setNodeHealthStatus: DB not available to update health for %s", nodeURL)
		return
	}
	// Ensure stateMutex is used if 'db' or 'state.Nodes' updates are concurrent elsewhere
	// stateMutex.Lock()
	// defer stateMutex.Unlock()
	_, err := db.Exec(`UPDATE cluster.nodes SET is_healthy = ?, last_seen = ? WHERE url = ?`,
		isHealthy, time.Now().UTC(), nodeURL) // Store time in UTC
	if err != nil {
		log.Printf("Error updating node health in DB for %s: %v", nodeURL, err)
	} else {
		log.Printf("Node health for %s set to %v in DB.", nodeURL, isHealthy)
	}
}

// nodeRoleHandler returns the node's role and shard information
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

// listNodes returns the list of nodes in the cluster
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

// createDatabaseHandler creates a new database with MySQL and HTTP replication
func createDatabaseHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if db == nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database connection not established. Please check MySQL settings.",
		})
		return
	}

	// If this node is a slave, forward the request to the master
	if currentRole == RoleSlave {
		log.Println("Slave node received createDatabase request, forwarding to master.")
		forwardRequestToMaster(w, r)
		return
	}

	// --- Master Logic ---
	var req struct {
		DBName string `json:"dbName"`
	}
	// Read body for master processing (forwardRequestToMaster has already read it for slaves)
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Invalid request body: " + err.Error()})
		return
	}
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Invalid request format: " + err.Error(),
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

	// Execute on master
	// MySQL will replicate this DDL statement to slaves.
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + SanitizeIdentifier(req.DBName)) // Ensure DBName is sanitized
	if err != nil {
		log.Printf("Error creating database '%s' on master: %v", req.DBName, err)
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error creating database: " + err.Error(),
		})
		return
	}

	log.Printf("Database '%s' created successfully on master.", req.DBName)

	// HTTP Replicate to slaves (as a notification/signal, MySQL handles data)
	operation := map[string]interface{}{
		"operation": "create_database",
		"dbName":    req.DBName,
		"shardId":   float64(calculateShardID(req.DBName)), // ShardID for a DB might be conceptual
	}
	replicateToNodes(operation)

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: fmt.Sprintf("Database '%s' created successfully on master and replication initiated.", req.DBName),
	})
}

// dropDatabaseHandler drops a database (replicated via MySQL)
func dropDatabaseHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if db == nil {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Database not connected"})
		return
	}

	if currentRole == RoleSlave {
		log.Printf("Slave node received dropDatabase request. Operation not allowed.")
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Dropping databases is only allowed on the master node."})
		return
	}

	// --- Master Logic ---
	var req struct {
		DBName string `json:"dbName"`
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
	if req.DBName == "" {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Database name required"})
		return
	}

	protectedDBs := map[string]bool{"information_schema": true, "mysql": true, "performance_schema": true, "sys": true, "cluster": true}
	if protectedDBs[strings.ToLower(req.DBName)] {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Cannot drop system database: " + req.DBName})
		return
	}

	_, err = db.Exec("DROP DATABASE IF EXISTS " + SanitizeIdentifier(req.DBName))
	if err != nil {
		log.Printf("Error dropping database '%s' on master: %v", req.DBName, err)
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Error dropping database: " + err.Error()})
		return
	}
	log.Printf("Database '%s' dropped successfully on master.", req.DBName)

	// Optional HTTP notification
	replicateToNodes(map[string]interface{}{
		"operation": "drop_database",
		"dbName":    req.DBName,
		"shardId":   float64(calculateShardID(req.DBName)),
	})

	json.NewEncoder(w).Encode(Response{Success: true, Message: fmt.Sprintf("Database '%s' dropped", req.DBName)})
}

// dropTableHandler drops a table (replicated via MySQL)
func dropTableHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if db == nil {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Database not connected"})
		return
	}

	if currentRole == RoleSlave {
		log.Printf("Slave node received dropTable request. Operation not allowed.")
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Dropping tables is only allowed on the master node."})
		return
	}

	// --- Master Logic ---
	var req struct {
		DBName    string `json:"dbName"`
		TableName string `json:"tableName"`
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
	if req.DBName == "" || req.TableName == "" {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "DB name and table name required"})
		return
	}

	safeDBName := SanitizeIdentifier(req.DBName)
	safeTableName := SanitizeIdentifier(req.TableName)

	dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		config.MySQL.User, config.MySQL.Password, config.MySQL.Host, config.MySQL.Port, safeDBName))
	if err != nil {
		log.Printf("Error connecting to database '%s' on master: %v", safeDBName, err)
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Error connecting to DB: " + err.Error()})
		return
	}
	defer dbConn.Close()

	_, err = dbConn.Exec("DROP TABLE IF EXISTS " + safeTableName)
	if err != nil {
		log.Printf("Error dropping table '%s.%s' on master: %v", safeDBName, safeTableName, err)
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Error dropping table: " + err.Error()})
		return
	}
	log.Printf("Table '%s.%s' dropped successfully on master.", safeDBName, safeTableName)

	_, err = db.Exec("DELETE FROM cluster.table_shards WHERE db_name = ? AND table_name = ?", safeDBName, safeTableName)
	if err != nil {
		log.Printf("Error removing table shard mapping for '%s.%s' on master: %v", safeDBName, safeTableName, err)
	}

	// Determine a representative shardId for the dropped table for notification purposes
	shardID := calculateShardID(safeDBName + "." + safeTableName)
	replicateToNodes(map[string]interface{}{
		"operation": "drop_table",
		"dbName":    safeDBName,
		"tableName": safeTableName,
		"shardId":   float64(shardID),
	})

	json.NewEncoder(w).Encode(Response{Success: true, Message: fmt.Sprintf("Table '%s' dropped", safeTableName)})
}

func listDatabasesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if db == nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Database connection not established. Please check MySQL settings.",
		})
		return
	}

	// Check replication status on slave
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

// createTableHandler creates a table with shard assignment (replicated via MySQL)
func createTableHandler(w http.ResponseWriter, r *http.Request) {
	if currentRole != RoleMaster {
		http.Error(w, "Only master can create tables", http.StatusForbidden)
		return
	}

	dbName := r.FormValue("db")
	tableName := r.FormValue("name")
	shardID := r.FormValue("shard_id")
	columns := r.FormValue("columns") // Added:  Comma-separated column definitions

	if dbName == "" || tableName == "" || shardID == "" || columns == "" {
		http.Error(w, "Database name, table name, shard ID, and columns are required", http.StatusBadRequest)
		return
	}

	// Basic validation
	if !isValidIdentifier(dbName) || !isValidIdentifier(tableName) {
		http.Error(w, "Invalid database or table name", http.StatusBadRequest)
		return
	}

	shardIDInt, err := strconv.Atoi(shardID)
	if err != nil {
		http.Error(w, "Invalid shard ID: must be an integer", http.StatusBadRequest)
		return
	}

	// Check if the shard ID is within the valid range (optional)
	if shardIDInt < 0 || shardIDInt >= config.ShardCount {
		http.Error(w, fmt.Sprintf("Shard ID must be between 0 and %d", config.ShardCount-1), http.StatusBadRequest)
		return
	}

	// Construct the CREATE TABLE query.
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s)", dbName, tableName, columns)
	if err := executeSQL(query); err != nil {
		http.Error(w, fmt.Sprintf("Error creating table: %v", err), http.StatusInternalServerError)
		return
	}

	// Store shard ID in cluster.table_shards
	insertShardQuery := "INSERT INTO cluster.table_shards (db_name, table_name, shard_id) VALUES (?, ?, ?)"
	_, err = db.Exec(insertShardQuery, dbName, tableName, shardIDInt)
	if err != nil {
		log.Printf("Error storing shard information: %v", err)
		http.Error(w, "Failed to store shard information", http.StatusInternalServerError)
		return // Consider if the table should be dropped on failure.
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Table '%s.%s' created in shard %d successfully\n", dbName, tableName, shardIDInt)

	// Notify slaves to create the table.  Include the shard ID.
	go notifySlaves(fmt.Sprintf("/api/create-table?db=%s&name=%s&shard_id=%d&columns=%s",
		url.QueryEscape(dbName), url.QueryEscape(tableName), shardIDInt, url.QueryEscape(columns)))
}

func notifySlaves(endpoint string) {
	stateMutex.Lock()
	defer stateMutex.Unlock()

	for _, node := range state.Nodes {
		if node.Role == RoleSlave {
			targetURL := node.URL + endpoint // Use the provided endpoint
			log.Printf("Notifying slave %s: %s", node.URL, targetURL)
			resp, err := http.Post(targetURL, "application/json", nil) //  No body is needed for create table
			if err != nil {
				log.Printf("Error notifying slave %s: %v", node.URL, err)
				continue // Continue to the next slave
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				log.Printf("Slave %s returned error: %s", node.URL, string(body))
			}
		}
	}
}

func slaveCreateTableHandler(w http.ResponseWriter, r *http.Request) {
	if currentRole != RoleSlave {
		http.Error(w, "Only slaves should handle table creation requests from master", http.StatusForbidden)
		return
	}

	dbName := r.FormValue("db")
	tableName := r.FormValue("name")
	shardID := r.FormValue("shard_id")

	if dbName == "" || tableName == "" || shardID == "" {
		http.Error(w, "Database name, table name, and shard ID are required", http.StatusBadRequest)
		return
	}
	//basic validation
	if !isValidIdentifier(dbName) || !isValidIdentifier(tableName) {
		http.Error(w, "Invalid database or table name", http.StatusBadRequest)
		return
	}

	shardIDInt, err := strconv.Atoi(shardID)
	if err != nil {
		http.Error(w, "Shard ID must be an integer", http.StatusBadRequest)
		return
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (id INT AUTO_INCREMENT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)", dbName, tableName)
	if err := executeSQL(query); err != nil {
		http.Error(w, fmt.Sprintf("Error creating table on slave: %v", err), http.StatusInternalServerError)
		return
	}

	// Store shard ID in cluster.table_shards on the slave
	insertShardQuery := "INSERT INTO cluster.table_shards (db_name, table_name, shard_id) VALUES (?, ?, ?)"
	_, err = db.Exec(insertShardQuery, dbName, tableName, shardIDInt)
	if err != nil {
		log.Printf("Error storing shard information on slave: %v", err)
		http.Error(w, "Failed to store shard information on slave", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Table '%s.%s' created on slave in shard %d successfully\n", dbName, tableName, shardIDInt)
}
func isValidIdentifier(identifier string) bool {
	// MySQL identifiers can contain letters, numbers, and underscores.
	// They cannot start with a number.
	reg := regexp.MustCompile("^[a-zA-Z_][a-zA-Z0-9_]*$")
	return reg.MatchString(identifier)
}

// executeSQL executes a SQL query and logs any errors.
func executeSQL(query string) error {
	_, err := db.Exec(query)
	if err != nil {
		log.Printf("Error executing SQL: %s - %v", query, err)
	}
	return err
}

// listTablesHandler lists tables and their columns in a database
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

// linkTablesHandler creates a foreign key relationship between tables
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
		Table1         string `json:"table1"`  // Parent table
		Table2         string `json:"table2"`  // Child table (table to add FK to)
		Column1        string `json:"column1"` // Column in parent table
		Column2        string `json:"column2"` // Column in child table
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

	// HTTP Replication for linking tables is generally not needed if DDL is replicated by MySQL.
	// ShardID for notification could be related to the child table.
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

// crudHandler handles CRUD operations
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
		ShardKeyValue interface{}            `json:"shardKeyValue,omitempty"` // Value of the shard_key column for routing
	}

	// Read body once. If forwarding, forwardRequestToMaster will re-use these bytes.
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Message: "Invalid request body: " + err.Error()})
		return
	}
	// Restore body for current handler if not forwarding, or for forwardToMaster if it is.
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

	// Determine the shardID for the request based on ShardKeyValue if provided and table is sharded.
	var shardIDForRequest int
	var tableShardKeyCol sql.NullString // To store the name of the shard_key column for the table
	var registeredTableShardID int      // The shard_id registered for the table itself in cluster.table_shards

	// Query cluster.table_shards for the table's shard_key column and its registered shard_id
	err = db.QueryRow("SELECT shard_id, shard_key FROM cluster.table_shards WHERE db_name = ? AND table_name = ?",
		req.DBName, req.Table).Scan(&registeredTableShardID, &tableShardKeyCol)

	if err != nil {
		if err == sql.ErrNoRows {
			log.Printf("Warning: Table '%s.%s' not found in cluster.table_shards. Calculating shardID based on table name for routing.", req.DBName, req.Table)
			shardIDForRequest = calculateShardID(req.DBName + "." + req.Table) // Fallback
		} else {
			log.Printf("Error retrieving shard info for '%s.%s': %v", req.DBName, req.Table, err)
			json.NewEncoder(w).Encode(Response{Success: false, Message: "Error retrieving shard info: " + err.Error()})
			return
		}
	} else {
		// Table is registered in cluster.table_shards
		if tableShardKeyCol.Valid && tableShardKeyCol.String != "" {
			// Table is sharded by a specific column key. Use req.ShardKeyValue if provided.
			if req.ShardKeyValue != nil {
				shardIDForRequest = calculateShardID(fmt.Sprintf("%v", req.ShardKeyValue))
				log.Printf("Calculated shardID %d for table '%s.%s' based on provided ShardKeyValue '%v' (shard_key column: '%s')",
					shardIDForRequest, req.DBName, req.Table, req.ShardKeyValue, tableShardKeyCol.String)
			} else if isWriteOperation {
				// For writes, if ShardKeyValue is not provided for a table sharded by a key, it's problematic.
				// Try to infer from Data if possible (e.g., on create)
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
				// For reads on a table sharded by key, if ShardKeyValue is not provided,
				// it might mean a scatter-gather query (not supported here) or query by non-shard key.
				// In such cases, the read might be directed to the table's "home" shard or all shards.
				// For simplicity here, we'll use the table's registered shardID.
				shardIDForRequest = registeredTableShardID
				log.Printf("Read operation on sharded table '%s.%s' (key: '%s') without ShardKeyValue. Using table's registered shardID: %d.",
					req.DBName, req.Table, tableShardKeyCol.String, shardIDForRequest)
			}
		} else {
			// Table is registered but has no specific shard_key column (e.g., sharded by table name or not sharded at data level)
			shardIDForRequest = registeredTableShardID
			log.Printf("Table '%s.%s' has no specific shard_key column in cluster.table_shards. Using its registered shardID: %d.",
				req.DBName, req.Table, shardIDForRequest)
		}
	}

	if currentRole == RoleSlave {
		if isWriteOperation {
			log.Printf("Slave node received WRITE op (%s) for '%s.%s' (data shard %d), forwarding to master.", req.Operation, req.DBName, req.Table, shardIDForRequest)
			forwardRequestToMaster(w, r) // r.Body was restored earlier
			return
		}
		// For READ operations on a slave:
		// "access the tables with it's shard id" - This means a slave serves reads for its *own assigned shard*.
		// We need to know which shard this slave node is responsible for.
		// This is typically determined when the slave starts or by its configuration.
		// Let's assume calculateShardID(config.SelfURL) gives the slave's *own* shard responsibility.
		slaveOwnsShardID := calculateShardID(config.SelfURL) // Example: slave's identity determines its shard
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

	// --- Master Logic (for writes) or Slave Logic (for allowed reads on its own shard) ---
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
			"shardId":       float64(shardIDForRequest), // This is the shardId of the *data* affected
			"shardKeyValue": req.ShardKeyValue,          // Pass along the original shard key value for context
		})
	}

	json.NewEncoder(w).Encode(Response{Success: true, Result: result})
}

// replicationHandler handles replication requests from the master
func replicationHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if currentRole == RoleMaster {
		log.Printf("Master node received a /api/replicate call. This should only be called on slaves. Ignoring.")
		http.Error(w, "This is a master node. Replication endpoint is for slaves.", http.StatusBadRequest)
		return
	}

	if db == nil { // Should not happen if slave is operational
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
	shardIDFloat, shardIdOk := req["shardId"].(float64) // This is the shardId of the *data* affected

	log.Printf("Slave (%s) received HTTP replication signal: Op=%s, DBName=%s, data_shardId=%v, FullReq: %+v",
		config.SelfURL, operation, dbName, req["shardId"], req)

	if !shardIdOk {
		log.Printf("Slave: Received replication signal without a valid data_shardId. Req: %+v", req)
		// Respond with success as it's just a signal, but log the issue.
		json.NewEncoder(w).Encode(Response{
			Success: true, // Or false if shardId is critical for any app logic
			Message: "Slave acknowledged replication signal, but data_shardId was missing or invalid.",
		})
		return
	}

	// Application-level logic for the slave based on the signal.
	// Example: If this slave is responsible for the data_shardId mentioned, or for all shards,
	// it might invalidate a cache or perform other shard-specific tasks.
	slaveOwnsShardID := calculateShardID(config.SelfURL) // Example: Slave's identity determines its shard responsibility

	log.Printf("Slave (serves shard %d) acknowledged replication signal for operation '%s' concerning data_shardId '%.0f'. MySQL replication is primary for data consistency.",
		slaveOwnsShardID, operation, shardIDFloat)

	// Example application-level task:
	// if int(shardIDFloat) == slaveOwnsShardID {
	//    log.Printf("Slave (shard %d) is responsible for this data_shardId. Performing app-level tasks (e.g., cache invalidation)...", slaveOwnsShardID)
	//    // invalidateCacheForShard(int(shardIDFloat), dbName, req["table"])
	// } else {
	//    log.Printf("Slave (shard %d) is NOT directly responsible for data_shardId %.0f. No specific app-level tasks for this shard.", slaveOwnsShardID, shardIDFloat)
	// }
	// If slaves are generic read replicas, they might always perform some action regardless of shardId,
	// or if the action is global (e.g. updating some shared metadata if not sharded).

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: fmt.Sprintf("Slave acknowledged replication signal for operation: %s, data_shardId: %.0f.", operation, shardIDFloat),
	})
}

// setupReplicationHandler configures replication for a slave node
func setupReplicationHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

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

func SanitizeDBName(name string) string {
	// Replace characters not allowed in DB names or simply use a whitelist.
	// For simplicity, this example is very basic.
	// In production, use a library or more comprehensive regex.
	return strings.ReplaceAll(name, "`", "") // Remove backticks at a minimum
}

// SanitizeIdentifier provides basic sanitization for table and column names.
func SanitizeIdentifier(name string) string {
	// Remove backticks and semicolons as a minimal measure.
	name = strings.ReplaceAll(name, "`", "")
	name = strings.ReplaceAll(name, ";", "")
	// Add more replacements or a regex for allowed characters, e.g., /^[a-zA-Z0-9_]+$/
	return name
}

// healthCheck checks the server's health
func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

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

// electionHandler handles election requests
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

// newMasterHandler updates the master node information
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
	_, err := db.Exec(`
		UPDATE cluster.nodes 
		SET role = ? 
		WHERE url = ?`, RoleMaster, req.MasterURL)
	if err != nil {
		log.Printf("Error updating master role: %v", err)
	}

	if config.SelfURL != req.MasterURL {
		currentRole = RoleSlave
		_, err = db.Exec(`
			UPDATE cluster.nodes 
			SET role = ? 
			WHERE url = ?`, RoleSlave, config.SelfURL)
		if err != nil {
			log.Printf("Error updating slave role: %v", err)
		}
	}

	loadNodesFromDB()
	w.WriteHeader(http.StatusOK)
}

// sendElectionRequest sends an election request to a node
func sendElectionRequest(url string) bool {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Post(url+"/api/election", "application/json", nil)
	return err == nil && resp.StatusCode == http.StatusOK
}

// sendNewMasterNotification notifies a node of a new master
func sendNewMasterNotification(url string) {
	client := http.Client{Timeout: 2 * time.Second}
	payload := fmt.Sprintf(`{"masterURL": "%s"}`, config.SelfURL)
	client.Post(url+"/api/new-master", "application/json", strings.NewReader(payload))
}

// heartbeat periodically checks node health
func heartbeat() {
	for {
		time.Sleep(3 * time.Second)
		stateMutex.Lock()
		loadNodesFromDB()
		for _, node := range state.Nodes {
			if node.URL != config.SelfURL {
				wasHealthy := node.IsHealthy
				node.IsHealthy = checkNodeHealth(node.URL)

				_, err := db.Exec(`
					UPDATE cluster.nodes 
					SET is_healthy = ?, last_seen = ? 
					WHERE url = ?`, node.IsHealthy, time.Now(), node.URL)
				if err != nil {
					log.Printf("Error updating node health: %v", err)
				}

				if node.IsHealthy && !wasHealthy && currentRole == RoleSlave && node.URL == state.CurrentMaster {
					notifyMasterOnline()
				}
			}
		}
		stateMutex.Unlock()
	}
}

// notifyMasterOnline notifies the master that this slave is online
func notifyMasterOnline() {
	maxRetries := 3
	retryDelay := 2 * time.Second

	for i := 0; i < maxRetries; i++ {
		client := http.Client{Timeout: 2 * time.Second}
		payload := fmt.Sprintf(`{"slaveURL": "%s"}`, config.SelfURL)
		resp, err := client.Post(state.CurrentMaster+"/api/slave-online", "application/json", strings.NewReader(payload))
		if err == nil && resp.StatusCode == http.StatusOK {
			log.Printf("Successfully notified master %s of online status", state.CurrentMaster)
			return
		}
		log.Printf("Failed to notify master about online status (attempt %d): %v", i+1, err)
		time.Sleep(retryDelay)
	}
	log.Printf("Failed to notify master after %d attempts", maxRetries)
}

// loggingMiddleware logs HTTP requests
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		log.Printf("Request: %s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
		log.Printf("Response: %s %s - %s", r.Method, r.URL.Path, time.Since(start))
	})
}

// corsMiddleware adds CORS headers
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

// executeCreate performs a CREATE operation
func executeCreate(dbConn *sql.DB, table string, data map[string]interface{}) (interface{}, error) {
	// Placeholder - Your actual implementation
	log.Printf("Executing CREATE on table %s with data %+v", table, data)
	if len(data) == 0 {
		return nil, fmt.Errorf("no data provided for create operation")
	}

	var columns []string
	var placeholders []string
	var values []interface{}

	for k, v := range data {
		columns = append(columns, SanitizeIdentifier(k))
		placeholders = append(placeholders, "?")
		values = append(values, v)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		SanitizeIdentifier(table),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	log.Printf("Executing SQL: %s with values: %v", query, values)
	res, err := dbConn.Exec(query, values...)
	if err != nil {
		return nil, fmt.Errorf("executeCreate failed: %w", err)
	}
	id, _ := res.LastInsertId()
	return map[string]interface{}{"id": id}, nil
}

// executeRead performs a READ operation
func executeRead(dbConn *sql.DB, table string, where map[string]interface{}) (interface{}, error) {
	// Placeholder - Your actual implementation
	log.Printf("Executing READ on table %s with where %+v", table, where)
	query := fmt.Sprintf("SELECT * FROM %s", SanitizeIdentifier(table))
	var values []interface{}
	if len(where) > 0 {
		var conditions []string
		for k, v := range where {
			conditions = append(conditions, fmt.Sprintf("%s = ?", SanitizeIdentifier(k)))
			values = append(values, v)
		}
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	log.Printf("Executing SQL: %s with values: %v", query, values)
	rows, err := dbConn.Query(query, values...)
	if err != nil {
		return nil, fmt.Errorf("executeRead query failed: %w", err)
	}
	defer rows.Close()
	// Convert rows to JSON or []map[string]interface{}
	// This is a simplified version of your rowsToJSON
	columns, _ := rows.Columns()
	var results []map[string]interface{}
	for rows.Next() {
		rowValues := make([]interface{}, len(columns))
		rowScanArgs := make([]interface{}, len(columns))
		for i := range rowValues {
			rowScanArgs[i] = &rowValues[i]
		}
		if err := rows.Scan(rowScanArgs...); err != nil {
			return nil, fmt.Errorf("executeRead row scan failed: %w", err)
		}
		entry := make(map[string]interface{})
		for i, col := range columns {
			if b, ok := rowValues[i].([]byte); ok {
				entry[col] = string(b)
			} else {
				entry[col] = rowValues[i]
			}
		}
		results = append(results, entry)
	}
	return results, nil
}

// executeUpdate performs an UPDATE operation
func executeUpdate(dbConn *sql.DB, table string, data map[string]interface{}, where map[string]interface{}) (interface{}, error) {
	// Placeholder - Your actual implementation
	log.Printf("Executing UPDATE on table %s with data %+v where %+v", table, data, where)
	if len(data) == 0 {
		return nil, fmt.Errorf("no data provided for update")
	}
	if len(where) == 0 {
		return nil, fmt.Errorf("no where clause for update; this would update all rows, which is usually unsafe")
	}

	var setClauses []string
	var values []interface{}
	for k, v := range data {
		setClauses = append(setClauses, fmt.Sprintf("%s = ?", SanitizeIdentifier(k)))
		values = append(values, v)
	}

	var whereClauses []string
	for k, v := range where {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", SanitizeIdentifier(k)))
		values = append(values, v) // Append where values after set values
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		SanitizeIdentifier(table),
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "))

	log.Printf("Executing SQL: %s with values: %v", query, values)
	res, err := dbConn.Exec(query, values...)
	if err != nil {
		return nil, fmt.Errorf("executeUpdate failed: %w", err)
	}
	rowsAffected, _ := res.RowsAffected()
	return map[string]interface{}{"rowsAffected": rowsAffected}, nil
}

// executeDelete performs a DELETE operation
func executeDelete(dbConn *sql.DB, table string, where map[string]interface{}) (interface{}, error) {
	// Placeholder - Your actual implementation
	log.Printf("Executing DELETE on table %s where %+v", table, where)
	if len(where) == 0 {
		return nil, fmt.Errorf("no where clause for delete; this would delete all rows, which is usually unsafe")
	}

	var whereClauses []string
	var values []interface{}
	for k, v := range where {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", SanitizeIdentifier(k)))
		values = append(values, v)
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s",
		SanitizeIdentifier(table),
		strings.Join(whereClauses, " AND "))

	log.Printf("Executing SQL: %s with values: %v", query, values)
	res, err := dbConn.Exec(query, values...)
	if err != nil {
		return nil, fmt.Errorf("executeDelete failed: %w", err)
	}
	rowsAffected, _ := res.RowsAffected()
	return map[string]interface{}{"rowsAffected": rowsAffected}, nil
}

// keys returns the keys of a map
func keys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// createPlaceholders creates a slice of placeholders for SQL queries
func createPlaceholders(n int) []string {
	placeholders := make([]string, n)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return placeholders
}

// buildSetClause builds the SET clause for UPDATE queries
func buildSetClause(data map[string]interface{}) (string, []interface{}) {
	clauses := make([]string, 0, len(data))
	values := make([]interface{}, 0, len(data))

	for k, v := range data {
		clauses = append(clauses, fmt.Sprintf("%s = ?", k))
		values = append(values, v)
	}

	return strings.Join(clauses, ", "), values
}

// buildWhereClause builds the WHERE clause for queries
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

// rowsToJSON converts SQL rows to JSON
func rowsToJSON(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	result := make([]map[string]interface{}, 0)

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		entry := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
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

// shutdownSlaveHandler shuts down a slave node
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

// shutdownHandler initiates a graceful shutdown
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

// slaveOnlineHandler updates the status of a slave node coming online
func slaveOnlineHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if currentRole != RoleMaster {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Only master node can handle slave online notifications",
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

	stateMutex.Lock()
	defer stateMutex.Unlock()

	var exists bool
	err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM cluster.nodes WHERE url = ?)", req.SlaveURL).Scan(&exists)
	if err != nil {
		log.Printf("Error checking node existence: %v", err)
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error checking node existence",
		})
		return
	}

	now := time.Now()
	if exists {
		_, err = db.Exec(`
			UPDATE cluster.nodes 
			SET is_healthy = ?, last_seen = ? 
			WHERE url = ?`, true, now, req.SlaveURL)
	} else {
		registerNode(req.SlaveURL, RoleSlave, calculateShardID(req.SlaveURL))
	}

	if err != nil {
		log.Printf("Error updating slave status: %v", err)
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error updating slave status",
		})
		return
	}

	loadNodesFromDB()
	log.Printf("Slave %s is back online", req.SlaveURL)
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: fmt.Sprintf("Slave %s online status updated", req.SlaveURL),
	})
}

func forwardRequestToMaster(w http.ResponseWriter, r *http.Request) {
	if state.CurrentMaster == "" {
		log.Printf("Error: Cannot forward request, master URL is not set.")
		http.Error(w, "Master node not available", http.StatusInternalServerError)
		return
	}
	if state.CurrentMaster == config.SelfURL {
		log.Printf("Error: Cannot forward request, this node IS the master.")
		// This case should ideally be caught before calling forwardRequestToMaster
		http.Error(w, "This node is the master, cannot forward to self", http.StatusInternalServerError)
		return
	}

	// Read the original request body
	originalBody, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading original request body for forwarding: %v", err)
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	// It's crucial to restore the request body so it can be read again by the HTTP client
	r.Body = io.NopCloser(bytes.NewBuffer(originalBody))

	// Construct the target URL on the master
	// r.URL.Path should already contain the correct API endpoint like "/api/crud"
	targetURL := state.CurrentMaster + r.URL.Path
	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}

	log.Printf("Forwarding request from slave (%s) to master (%s): %s %s", config.SelfURL, state.CurrentMaster, r.Method, targetURL)

	// Create a new request to the master.
	// Use a new buffer for the master request body.
	masterReq, err := http.NewRequest(r.Method, targetURL, bytes.NewBuffer(originalBody))
	if err != nil {
		log.Printf("Error creating request to master: %v", err)
		http.Error(w, "Failed to create request for master", http.StatusInternalServerError)
		return
	}

	// Copy headers from the original request to the new master request
	for name, headers := range r.Header {
		for _, h := range headers {
			masterReq.Header.Add(name, h)
		}
	}
	// Ensure Content-Type is set if there's a body, especially for POST/PUT
	if len(originalBody) > 0 && masterReq.Header.Get("Content-Type") == "" {
		// Prefer original Content-Type if available, otherwise default to application/json
		originalContentType := r.Header.Get("Content-Type")
		if originalContentType != "" {
			masterReq.Header.Set("Content-Type", originalContentType)
		} else {
			masterReq.Header.Set("Content-Type", "application/json")
		}
	}

	// Send the request to the master
	client := &http.Client{Timeout: 30 * time.Second} // Adjust timeout as needed
	resp, err := client.Do(masterReq)
	if err != nil {
		log.Printf("Error forwarding request to master %s: %v", targetURL, err)
		http.Error(w, fmt.Sprintf("Failed to forward request to master: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Copy the master's response headers and status code back to the original client
	for name, headers := range resp.Header {
		for _, h := range headers {
			w.Header().Add(name, h)
		}
	}
	w.WriteHeader(resp.StatusCode)

	// Copy the master's response body back to the original client
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("Error copying response body from master: %v", err)
		// Don't write an error header here as it might already be sent
	}
	log.Printf("Successfully forwarded request to master and relayed response (status: %d)", resp.StatusCode)
}
