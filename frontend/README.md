# Distributed Database Management System

A comprehensive distributed database management system with master-slave replication, fault tolerance, and a modern web interface.

## Features

- **Database Management**: Create, list, and drop databases
- **Table Management**: Create, list, and drop tables with custom schemas
- **CRUD Operations**: Perform create, read, update, and delete operations on data
- **Replication**: Master-slave replication for high availability
- **Fault Tolerance**: Automatic master election and failover
- **Cluster Management**: Monitor and manage nodes in the distributed system
- **Modern UI**: Clean and responsive web interface

## System Architecture

The system consists of two main components:

1. **Backend**: A Go server that handles database operations, replication, and cluster management
2. **Frontend**: A Next.js web application that provides a user interface for interacting with the system

### Backend Architecture

The backend is built with Go and uses the following components:

- **HTTP Server**: Handles API requests from the frontend
- **MySQL Driver**: Connects to MySQL databases
- **Replication Manager**: Manages master-slave replication
- **Cluster Manager**: Handles node discovery, health checks, and master election

### Frontend Architecture

The frontend is built with Next.js and uses the following components:

- **React**: For building the user interface
- **Tailwind CSS**: For styling
- **shadcn/ui**: For UI components
- **SWR**: For data fetching and caching

## Deployment Guide

### Prerequisites

- Go 1.16 or higher
- MySQL 8.0 or higher
- Node.js 18 or higher
- npm or yarn

### Backend Deployment

1. Clone the repository:
   \`\`\`bash
   git clone https://github.com/yourusername/distributed-db.git
   cd distributed-db
   \`\`\`

2. Configure MySQL connection in `main.go`:
   ```go
   const (
       MySQLUser     = "your_mysql_user"
       MySQLPassword = "your_mysql_password"
       MySQLHost     = "localhost"
       MySQLPort     = "3306"
   )
   \`\`\`

3. Build and run the backend:
   \`\`\`bash
   cd backend
   go build
   ./backend http://localhost:8080 http://localhost:8080
   \`\`\`

   The first argument is the URL of the current node, and the second argument is the URL of the master node. For the first node, both should be the same.

4. To start additional nodes in the cluster:
   \`\`\`bash
   ./backend http://localhost:8081 http://localhost:8080
   \`\`\`
   
   This starts a slave node at port 8081 that connects to the master at port 8080.

### Frontend Deployment

1. Navigate to the frontend directory:
   \`\`\`bash
   cd frontend
   \`\`\`

2. Install dependencies:
   \`\`\`bash
   npm install
   # or
   yarn install
   \`\`\`

3. Configure the API URL in `.env.local`:
   \`\`\`
   NEXT_PUBLIC_API_URL=http://localhost:8080
   \`\`\`

4. Build and start the frontend:
   \`\`\`bash
   npm run build
   npm start
   # or
   yarn build
   yarn start
   \`\`\`

5. Access the web interface at `http://localhost:3000`

### Docker Deployment

You can also deploy the system using Docker:

1. Build the Docker images:
   \`\`\`bash
   docker-compose build
   \`\`\`

2. Start the containers:
   \`\`\`bash
   docker-compose up -d
   \`\`\`

3. Access the web interface at `http://localhost:3000`

## Fault Tolerance

The system includes several mechanisms for fault tolerance:

### Master Election

When the master node fails, the system automatically elects a new master:

1. Slave nodes detect that the master is unreachable
2. Nodes communicate to elect a new master
3. The elected node is promoted to master
4. Other nodes are notified of the new master

### Data Replication

All write operations are replicated to slave nodes:

1. Write operations are executed on the master
2. The master sends the operation to all slave nodes
3. Slave nodes execute the operation locally
4. The system ensures consistency across all nodes

### Error Handling

The system includes comprehensive error handling:

1. Connection errors are detected and reported
2. Failed operations are retried with exponential backoff
3. The UI provides clear error messages and recovery options

## Monitoring and Debugging

The system includes tools for monitoring and debugging:

1. Node status monitoring in the UI
2. API debugging tools
3. Connection diagnostics
4. Detailed logging

## Testing

The system includes comprehensive tests:

### Backend Tests

Run backend tests:
\`\`\`bash
cd backend
go test ./...
\`\`\`

### Frontend Tests

Run frontend tests:
\`\`\`bash
cd frontend
npm test
# or
yarn test
\`\`\`

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
