
# Distributed Database System 🐬🚀

This project implements a **MySQL cluster management system** using **Go**. It supports **Master-Slave replication**, automatic **failover**, **CRUD operations**, and **cluster monitoring** through RESTful APIs. This system is ideal for managing and replicating MySQL databases across multiple nodes in real-time.

## Features ✅

- 🚀 Master-Slave Replication Setup
- 🔄 Automatic Failover with Leader Election
- 🗄️ CRUD Operations (Create, Read, Update, Delete)
- 📊 Database & Table Management APIs
- 🔗 Foreign Key Linking
- 🔄 Replication Sync Between Nodes
- 🛠️ Node Registration & Health Checks
- 🖥️ Remote Node Shutdown Capability

## Requirements ⚙️

- **Go** 1.18+
- **MySQL** Server
- Linux/Windows/macOS (supports all for node control)

## Installation 📦

1. **Clone the repo**
   ```bash
   git clone https://github.com/yourusername/mysql-cluster-manager.git
   cd mysql-cluster-manager
   ```

2. **Build the project**
   ```bash
   go build -o cluster-manager
   ```

3. **Run a Master Node**
   ```bash
   ./cluster-manager http://<master-ip>:8080
   ```

4. **Run a Slave Node**
   ```bash
   ./cluster-manager http://<slave-ip>:8081 http://<master-ip>:8080
   ```

## API Endpoints 🌐

### 📦 Cluster Management
| Endpoint                     | Method  | Description                   |
| ----------------------------  | ------- | ----------------------------- |
| `/api/register`              | POST    | Register new node             |
| `/api/nodes`                 | GET     | List cluster nodes            |
| `/api/health`                | GET     | Health check                  |
| `/api/election`              | POST    | Start election                |
| `/api/new-master`            | POST    | Notify new master             |
| `/api/node-role`             | GET     | Get current node role         |
| `/api/shutdown-slave`        | POST    | Shutdown slave remotely       |
| `/api/shutdown`              | POST    | Shutdown the node (machine)   |

### 🗄️ Database Management
| Endpoint                      | Method  | Description                   |
| ----------------------------- | ------- | ----------------------------- |
| `/api/create-db`              | POST    | Create database               |
| `/api/list-databases`         | GET     | List databases                |
| `/api/drop-db`                | POST    | Drop database                 |

### 📑 Table Management
| Endpoint                      | Method  | Description                   |
| ----------------------------- | ------- | ----------------------------- |
| `/api/create-table`           | POST    | Create table                  |
| `/api/list-tables`            | GET     | List tables                   |
| `/api/drop-table`             | POST    | Drop table                    |
| `/api/link-tables`            | POST    | Add foreign key               |

### ✍️ CRUD Operations
| Endpoint                      | Method  | Description                   |
| ----------------------------- | ------- | ----------------------------- |
| `/api/crud`                   | POST    | Perform CRUD operations       |

### 🔄 Replication
| Endpoint                      | Method  | Description                   |
| ----------------------------- | ------- | ----------------------------- |
| `/api/replicate`              | POST    | Replicate operation           |
| `/api/setup-replication`      | POST    | Setup replication manually    |

---

## Running Example ⚡

### Start Master Node
```bash
./cluster-manager http://localhost:8080
```

### Start Slave Node
```bash
./cluster-manager http://localhost:8081 http://localhost:8080
```

---

## Database Config 🔐
Edit the following constants in your `main.go` if needed:

```go
const (
    MySQLUser     = "root"
    MySQLPassword = "yourpassword"
    MySQLHost     = "127.0.0.1"
    MySQLPort     = "3306"
)
```

---

## Health Check ❤️
You can check if a node is healthy by:
```bash
GET /api/health
```

---

## Leader Election 🗳️
If master node fails, slaves will automatically start an election and promote a healthy node to master.

---

## Shutdown a Slave Node 💻
Master node can remotely shutdown a slave node’s machine:
```bash
POST /api/shutdown-slave
{
  "slaveURL": "http://localhost:8081"
}
```

---

## Security Warning ⚠️

- Passwords are hardcoded → Make sure to secure this before production!
- Remote shutdown will execute OS-level shutdown → Use with caution!
- This tool is intended for private/local network usage only.

---

## License 📄
This project is licensed under MIT License. Feel free to use and modify!
