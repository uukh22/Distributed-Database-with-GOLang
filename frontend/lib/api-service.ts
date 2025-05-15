const API_BASE_URL = "http://localhost:8080/api"

export interface ApiResponse<T = any> {
  success: boolean
  message: string
  result?: T
}

export interface Node {
  id: string
  role: string
  url: string
  isHealthy: boolean
  lastSeen: string
  shardId: number
  createdAt: string
}

export interface Database {
  name: string
}

export interface Table {
  name: string
  columns: Column[]
}

export interface Column {
  name: string
  type: string
}

class ApiService {
  private async request<T>(endpoint: string, options?: RequestInit): Promise<ApiResponse<T>> {
    try {
      const response = await fetch(`${API_BASE_URL}${endpoint}`, {
        headers: {
          "Content-Type": "application/json",
        },
        ...options,
      })

      if (!response.ok) {
        throw new Error(`HTTP error! Status: ${response.status}`)
      }

      const data = await response.json()
      return data as ApiResponse<T>
    } catch (error) {
      console.error("API request failed:", error)
      return {
        success: false,
        message: error instanceof Error ? error.message : "Unknown error occurred",
      }
    }
  }

  // Health check
  async checkHealth(): Promise<ApiResponse<boolean>> {
    return this.request<boolean>("/health")
  }

  // Node management
  async getNodeRole(): Promise<ApiResponse<{ role: string; shardId: number; masterUrl?: string }>> {
    return this.request<{ role: string; shardId: number; masterUrl?: string }>("/node-role")
  }

  async getNodes(): Promise<ApiResponse<Node[]>> {
    return this.request<Node[]>("/nodes")
  }

  // Database operations
  async getDatabases(): Promise<ApiResponse<string[]>> {
    return this.request<string[]>("/list-databases")
  }

  async createDatabase(dbName: string): Promise<ApiResponse> {
    return this.request("/create-db", {
      method: "POST",
      body: JSON.stringify({ dbName }),
    })
  }

  async dropDatabase(dbName: string): Promise<ApiResponse> {
    return this.request("/drop-db", {
      method: "POST",
      body: JSON.stringify({ dbName }),
    })
  }

  // Table operations
  async getTables(dbName: string): Promise<ApiResponse<Table[]>> {
    return this.request<Table[]>(`/list-tables?db=${dbName}`)
  }

  async createTable(dbName: string, tableName: string, columns: Record<string, string>): Promise<ApiResponse> {
    return this.request("/create-table", {
      method: "POST",
      body: JSON.stringify({ dbName, tableName, columns }),
    })
  }

  async dropTable(dbName: string, tableName: string): Promise<ApiResponse> {
    return this.request("/drop-table", {
      method: "POST",
      body: JSON.stringify({ dbName, tableName }),
    })
  }

  async linkTables(
    dbName: string,
    table1: string,
    table2: string,
    column1: string,
    column2: string,
    constraintType: string,
  ): Promise<ApiResponse> {
    return this.request("/link-tables", {
      method: "POST",
      body: JSON.stringify({ dbName, table1, table2, column1, column2, constraintType }),
    })
  }

  // CRUD operations
  async createRecord(dbName: string, table: string, data: Record<string, any>): Promise<ApiResponse> {
    return this.request("/crud", {
      method: "POST",
      body: JSON.stringify({ dbName, operation: "create", table, data }),
    })
  }

  async readRecords(dbName: string, table: string, where?: Record<string, any>): Promise<ApiResponse> {
    return this.request("/crud", {
      method: "POST",
      body: JSON.stringify({ dbName, operation: "read", table, where }),
    })
  }

  async updateRecords(
    dbName: string,
    table: string,
    data: Record<string, any>,
    where: Record<string, any>,
  ): Promise<ApiResponse> {
    return this.request("/crud", {
      method: "POST",
      body: JSON.stringify({ dbName, operation: "update", table, data, where }),
    })
  }

  async deleteRecords(dbName: string, table: string, where: Record<string, any>): Promise<ApiResponse> {
    return this.request("/crud", {
      method: "POST",
      body: JSON.stringify({ dbName, operation: "delete", table, where }),
    })
  }

  // Replication
  async setupReplication(masterHost: string, masterPort: number): Promise<ApiResponse> {
    return this.request("/setup-replication", {
      method: "POST",
      body: JSON.stringify({ masterHost, masterPort }),
    })
  }

  // Cluster management
  async shutdownSlave(slaveURL: string): Promise<ApiResponse> {
    return this.request("/shutdown-slave", {
      method: "POST",
      body: JSON.stringify({ slaveURL }),
    })
  }

  async notifySlaveOnline(slaveURL: string): Promise<ApiResponse> {
    return this.request("/slave-online", {
      method: "POST",
      body: JSON.stringify({ slaveURL }),
    })
  }
}

export const apiService = new ApiService()
