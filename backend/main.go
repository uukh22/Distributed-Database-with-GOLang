package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
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

	config.SelfURL = strings.TrimSuffix(config.SelfURL, "/")
	config.MasterURL = strings.TrimSuffix(config.MasterURL, "/")

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

func calculateShardID(key string) int {
	if config.ShardCount <= 0 {
		log.Printf("Warning: ShardCount in config is %d (not positive). Defaulting shardID to 0 for key '%s'.", config.ShardCount, key)
		return 0
	}
	hash := 0
	for _, char := range key {
		hash = (hash*31 + int(char)) % config.ShardCount
	}
	return (hash & 0x7fffffff) % config.ShardCount
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
	_, err := db.Exec("STOP SLAVE;")
	if err != nil {
		log.Printf("Warning: Failed to stop slave - %v", err)
	}

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
		logFile         string
		logPos          int
		binlogDoDB      string
		ignoredDB       string
		executedGtidSet string
	)

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

func checkNodeHealth(url string) bool {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url + "/api/health")
	return err == nil && resp.StatusCode == http.StatusOK
}

func replicateToNodes(operationData map[string]interface{}) {
	shardIDInterface, shardIdOk := operationData["shardId"]
	if !shardIdOk {
		log.Printf("CRITICAL: replicateToNodes called without shardId in operationData: %+v. Skipping replication signal.", operationData)
		return
	}

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
		go func(slaveNode *Node, data []byte, opData map[string]interface{}) {
			targetURL := slaveNode.URL + "/api/replicate"

			log.Printf("Sending HTTP replication signal to slave %s (%s) for operation %s, data_shardId: %.0f",
				slaveNode.ID, slaveNode.URL, opData["operation"], opData["shardId"].(float64))

			client := http.Client{Timeout: 10 * time.Second}
			resp, err := client.Post(targetURL, "application/json", bytes.NewBuffer(data))
			if err != nil {
				log.Printf("HTTP replication signal to %s FAILED: %v", slaveNode.URL, err)
				setNodeHealthStatus(slaveNode.URL, false)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				bodyBytes, _ := io.ReadAll(resp.Body)
				log.Printf("HTTP replication signal to %s returned status %d. Response: %s", slaveNode.URL, resp.StatusCode, string(bodyBytes))
			} else {
				log.Printf("Successfully sent HTTP replication signal to %s (status %d)", slaveNode.URL, resp.StatusCode)
			}
		}(node, jsonData, operationData)
	}
}

func setNodeHealthStatus(nodeURL string, isHealthy bool) {
	if db == nil {
		log.Printf("setNodeHealthStatus: DB not available to update health for %s", nodeURL)
		return
	}
	// stateMutex.Lock()
	// defer stateMutex.Unlock()
	_, err := db.Exec(`UPDATE cluster.nodes SET is_healthy = ?, last_seen = ? WHERE url = ?`,
		isHealthy, time.Now().UTC(), nodeURL)
	if err != nil {
		log.Printf("Error updating node health in DB for %s: %v", nodeURL, err)
	} else {
		log.Printf("Node health for %s set to %v in DB.", nodeURL, isHealthy)
	}
}

func notifySlaves(endpoint string) {
	stateMutex.Lock()
	defer stateMutex.Unlock()

	for _, node := range state.Nodes {
		if node.Role == RoleSlave {
			targetURL := node.URL + endpoint
			log.Printf("Notifying slave %s: %s", node.URL, targetURL)
			resp, err := http.Post(targetURL, "application/json", nil)
			if err != nil {
				log.Printf("Error notifying slave %s: %v", node.URL, err)
				continue
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				log.Printf("Slave %s returned error: %s", node.URL, string(body))
			}
		}
	}
}

func isValidIdentifier(identifier string) bool {
	reg := regexp.MustCompile("^[a-zA-Z_][a-zA-Z0-9_]*$")
	return reg.MatchString(identifier)
}

func executeSQL(query string) error {
	_, err := db.Exec(query)
	if err != nil {
		log.Printf("Error executing SQL: %s - %v", query, err)
	}
	return err
}

func SanitizeDBName(name string) string {
	return strings.ReplaceAll(name, "`", "")
}

func SanitizeIdentifier(name string) string {
	name = strings.ReplaceAll(name, "`", "")
	name = strings.ReplaceAll(name, ";", "")
	return name
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

func executeCreate(dbConn *sql.DB, table string, data map[string]interface{}) (interface{}, error) {
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

func executeRead(dbConn *sql.DB, table string, where map[string]interface{}) (interface{}, error) {
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

func executeUpdate(dbConn *sql.DB, table string, data map[string]interface{}, where map[string]interface{}) (interface{}, error) {
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
		values = append(values, v)
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

func executeDelete(dbConn *sql.DB, table string, where map[string]interface{}) (interface{}, error) {
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

func forwardRequestToMaster(w http.ResponseWriter, r *http.Request) {
	if state.CurrentMaster == "" {
		log.Printf("Error: Cannot forward request, master URL is not set.")
		http.Error(w, "Master node not available", http.StatusInternalServerError)
		return
	}
	if state.CurrentMaster == config.SelfURL {
		log.Printf("Error: Cannot forward request, this node IS the master.")
		http.Error(w, "This node is the master, cannot forward to self", http.StatusInternalServerError)
		return
	}

	originalBody, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading original request body for forwarding: %v", err)
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}

	r.Body = io.NopCloser(bytes.NewBuffer(originalBody))

	targetURL := state.CurrentMaster + r.URL.Path
	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}

	log.Printf("Forwarding request from slave (%s) to master (%s): %s %s", config.SelfURL, state.CurrentMaster, r.Method, targetURL)

	masterReq, err := http.NewRequest(r.Method, targetURL, bytes.NewBuffer(originalBody))
	if err != nil {
		log.Printf("Error creating request to master: %v", err)
		http.Error(w, "Failed to create request for master", http.StatusInternalServerError)
		return
	}

	for name, headers := range r.Header {
		for _, h := range headers {
			masterReq.Header.Add(name, h)
		}
	}

	if len(originalBody) > 0 && masterReq.Header.Get("Content-Type") == "" {
		originalContentType := r.Header.Get("Content-Type")
		if originalContentType != "" {
			masterReq.Header.Set("Content-Type", originalContentType)
		} else {
			masterReq.Header.Set("Content-Type", "application/json")
		}
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(masterReq)
	if err != nil {
		log.Printf("Error forwarding request to master %s: %v", targetURL, err)
		http.Error(w, fmt.Sprintf("Failed to forward request to master: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for name, headers := range resp.Header {
		for _, h := range headers {
			w.Header().Add(name, h)
		}
	}
	w.WriteHeader(resp.StatusCode)

	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("Error copying response body from master: %v", err)
	}
	log.Printf("Successfully forwarded request to master and relayed response (status: %d)", resp.StatusCode)
}
