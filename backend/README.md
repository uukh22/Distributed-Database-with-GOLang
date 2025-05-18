# Distributed Database Management System

This project is a **Golang-based distributed backend system** for managing MySQL clusters with support for **sharding**, **replication**, **automatic failover**, and **RESTful CRUD operations**.

---

## Highlights

* Master/Slave node architecture
* Automatic master election on failure
* Horizontal sharding support
* MySQL replication via binary logs and HTTP fallback
* Health checks and heartbeat system
* Foreign key support for table linkage
* RESTful API for database/table/record management
* Graceful shutdown and online recovery of nodes

---

## Technologies Used

* **Language:** Go (Golang)
* **Database:** MySQL
* **Libraries:**

  * [`gorilla/mux`](https://github.com/gorilla/mux)
  * [`go-sql-driver/mysql`](https://github.com/go-sql-driver/mysql)

---

## Directory Structure

```
.
├── main.go              # Application logic
├── config.json          # Node configuration file
```

---

## Configuration File

Create a `config.json` with the following:

```json
{
  "self_url": "http://localhost:8080",
  "master_url": "http://localhost:8080",
  "shard_count": 3,
  "mysql": {
    "user": "root",
    "password": "yourpassword",
    "host": "127.0.0.1",
    "port": "3306"
  },
  "replication": {
    "user": "replica",
    "password": "replica_password"
  }
}
```

---

## Getting Started

1. **Install dependencies:**

   ```bash
   cd backend

2. **Install dependencies:**

   ```bash
   go mod tidy
   ```
3. **Run node:**

   ```bash
   go run .
   ```
4. **Override config (optional):**

   ```bash
   go run main.go http://this-node:port http://master-node:port
   ```

---

## 📡 API Overview

| Method | Endpoint                 | Purpose                              |
| ------ | ------------------------ | ------------------------------------ |
| GET    | `/api/nodes`             | List all nodes in cluster            |
| POST   | `/api/register`          | Register this node                   |
| GET    | `/api/health`            | Health check                         |
| GET    | `/api/node-role`         | Get current node's role and shard ID |
| POST   | `/api/create-db`         | Create new database                  |
| POST   | `/api/drop-db`           | Drop a database                      |
| GET    | `/api/list-databases`    | List databases                       |
| POST   | `/api/create-table`      | Create a new table                   |
| POST   | `/api/drop-table`        | Drop a table                         |
| GET    | `/api/list-tables`       | Get table list with columns          |
| POST   | `/api/link-tables`       | Add foreign key constraints          |
| POST   | `/api/crud`              | Perform CRUD operations              |
| POST   | `/api/shutdown`          | Shutdown node                        |
| POST   | `/api/setup-replication` | Configure slave replication          |

---

## Node Roles

* **Master Node:**

  * Handles writes
  * Coordinates replication
  * Manages cluster state and elections

* **Slave Node:**

  * Performs read-only queries
  * Listens for replication from master

When a master node fails, the system promotes the oldest healthy slave to master.

---

## Sample API Usage

**Create Table Example:**

```json
POST /api/create-table
{
  "dbName": "school",
  "tableName": "students",
  "shardKey": "student_id",
  "columns": {
    "student_id": "INT",
    "name": "VARCHAR(255)",
    "age": "INT"
  }
}
```

---

## Graceful Shutdown

To shut down a node gracefully:

```bash
POST /api/shutdown
```

---


## Author

Crafted with care by Khaled Ibrahem, mohamed Abdelmawla. Contributions and feedback welcome!
