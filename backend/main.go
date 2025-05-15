package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime"
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
)

type Config struct {
	SelfURL     string            `json:"self_url"`
	MasterURL   string            `json:"master_url"`
	MySQL       MySQLConfig       `json:"mysql"`
	Replication ReplicationConfig `json:"replication"`
	ShardCount  int               `json:"shard_count"`
}

type MySQLConfig struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     string `json:"port"`
}

type ReplicationConfig struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

type Node struct {
	ID        string    `json:"id"`
	Role      string    `json:"role"`
	URL       string    `json:"url"`
	IsHealthy bool      `json:"isHealthy"`
	LastSeen  time.Time `json:"lastSeen"`
	ShardID   int       `json:"shardId"`
	CreatedAt time.Time `json:"createdAt"`
}

type SystemState struct {
	CurrentMaster string
	Nodes         []*Node
}

type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Result  interface{} `json:"result,omitempty"`
}

var (
	config      Config
	db          *sql.DB
	state       SystemState
	stateMutex  = &sync.Mutex{}
	currentRole string
)

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

	// Normalize URLs to remove trailing slashes
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

func initializeNode() {
	var err error
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/?parseTime=true",
		config.MySQL.User, config.MySQL.Password, config.MySQL.Host, config.MySQL.Port)
	log.Printf("Connecting to MySQL with: %s", connStr)

	db, err = sql.Open("mysql", connStr)
	if err != nil {
		log.Printf("Database connection failed: %v", err)
	} else {
		db.SetMaxOpenConns(25)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(5 * time.Minute)

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

	log.Printf("Checking role: selfURL=%s, masterURL=%s", config.SelfURL, config.MasterURL)
	if config.SelfURL == config.MasterURL {
		currentRole = RoleMaster
		state.CurrentMaster = config.SelfURL
		log.Println("Registering as MASTER node")
		registerNode(config.SelfURL, RoleMaster, calculateShardID(config.SelfURL))
		// Verify role in database
		var role string
		err = db.QueryRow("SELECT role FROM cluster.nodes WHERE url = ?", config.SelfURL).Scan(&role)
		if err != nil {
			log.Printf("Error verifying master role in database: %v", err)
		} else if role != RoleMaster {
			log.Printf("Master role mismatch in database: expected %s, got %s", RoleMaster, role)
			// Force update
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
		shardID := calculateShardID(config.SelfURL)
		log.Println("Registering as SLAVE node")
		registerNode(config.SelfURL, RoleSlave, shardID)
		registerNode(config.MasterURL, RoleMaster, calculateShardID(config.MasterURL))
		log.Println("Initialized as SLAVE node with master:", config.MasterURL)

		parts := strings.Split(config.MasterURL, ":")
		if len(parts) >= 3 && db != nil {
			host := strings.TrimPrefix(parts[1], "//")
			configureSlave(db, host, 3306)
		}
	}
}

func calculateShardID(url string) int {
	hash := 0
	for _, c := range url {
		hash += int(c)
	}
	return hash % config.ShardCount
}

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

	// Force master role for selfURL if it's the master
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
		log.Printf("Error creating user: %v", err)
		return err
	}

	_, err = db.Exec(fmt.Sprintf(`GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '%s'@'%%'`,
		config.Replication.User))
	if err != nil {
		log.Printf("Error granting privileges: %v", err)
		return err
	}

	_, err = db.Exec(`FLUSH PRIVILEGES`)
	if err != nil {
		log.Printf("Error flushing privileges: %v", err)
		return err
	}

	return nil
}

func configureSlave(db *sql.DB, masterHost string, masterPort int) error {
	_, err := db.Exec("STOP SLAVE;")
	if err != nil {
		log.Printf("Warning: Failed to stop slave - %v", err)
	}

	masterDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/?parseTime=true",
		config.Replication.User, config.Replication.Password, masterHost, masterPort))
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
		MASTER_USER='%s',
		MASTER_PASSWORD='%s',
		MASTER_LOG_FILE='%s',
		MASTER_LOG_POS=%d;
	`, masterHost, masterPort, config.Replication.User, config.Replication.Password, logFile, logPos))
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
	loadNodesFromDB()
	for _, node := range state.Nodes {
		if node.Role == RoleSlave && node.URL != config.SelfURL && node.IsHealthy && node.ShardID == calculateShardID(node.URL) {
			go func(url string) {
				jsonData, _ := json.Marshal(operation)
				client := http.Client{Timeout: 5 * time.Second}
				_, err := client.Post(url+"/api/replicate", "application/json", strings.NewReader(string(jsonData)))
				if err != nil {
					log.Printf("Replication to %s failed: %v", url, err)
					_, err = db.Exec(`
						UPDATE cluster.nodes 
						SET is_healthy = ?, last_seen = ? 
						WHERE url = ?`, false, time.Now(), url)
					if err != nil {
						log.Printf("Error updating node health: %v", err)
					}
				}
			}(node.URL)
		}
	}
}

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

func createDatabaseHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

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

	dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		config.MySQL.User, config.MySQL.Password, config.MySQL.Host, config.MySQL.Port, req.DBName))
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

	dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		config.MySQL.User, config.MySQL.Password, config.MySQL.Host, config.MySQL.Port, req.DBName))
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Message: "Error connecting to database: " + err.Error(),
		})
		return
	}
	defer dbConn.Close()

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

	dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		config.MySQL.User, config.MySQL.Password, config.MySQL.Host, config.MySQL.Port, req.DBName))
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

	shardID := calculateShardID(req.Table)

	dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		config.MySQL.User, config.MySQL.Password, config.MySQL.Host, config.MySQL.Port, req.DBName))
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

	if req.Operation != "read" {
		replicateToNodes(map[string]interface{}{
			"operation": req.Operation,
			"dbName":    req.DBName,
			"table":     req.Table,
			"data":      req.Data,
			"where":     req.Where,
			"shardId":   shardID,
		})
	}

	json.NewEncoder(w).Encode(Response{
		Success: true,
		Result:  result,
	})
}

func replicationHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

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

	shardID, ok := req["shardId"].(float64)
	if !ok || int(shardID) != calculateShardID(config.SelfURL) {
		http.Error(w, "Invalid shard ID for this node", http.StatusBadRequest)
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

		dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
			config.MySQL.User, config.MySQL.Password, config.MySQL.Host, config.MySQL.Port, dbName))
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

		dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
			config.MySQL.User, config.MySQL.Password, config.MySQL.Host, config.MySQL.Port, dbName))
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

		dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
			config.MySQL.User, config.MySQL.Password, config.MySQL.Host, config.MySQL.Port, dbName))
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

		dbConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
			config.MySQL.User, config.MySQL.Password, config.MySQL.Host, config.MySQL.Port, dbName))
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

func sendElectionRequest(url string) bool {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Post(url+"/api/election", "application/json", nil)
	return err == nil && resp.StatusCode == http.StatusOK
}

func sendNewMasterNotification(url string) {
	client := http.Client{Timeout: 2 * time.Second}
	payload := fmt.Sprintf(`{"masterURL": "%s"}`, config.SelfURL)
	client.Post(url+"/api/new-master", "application/json", strings.NewReader(payload))
}

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
		Message: "Computer shutdown initiated",
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
