"use client"

import { useState, useEffect } from "react"
import { useSearchParams } from "next/navigation"
import { Table2, Plus, Trash2, ArrowLeft, LinkIcon } from "lucide-react"

import { apiService, type Table } from "@/lib/api-service"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog"
import { toast } from "@/components/ui/use-toast"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

export default function TablesPage() {
  const searchParams = useSearchParams()
  const dbName = searchParams.get("db")

  const [tables, setTables] = useState<Table[]>([])
  const [loading, setLoading] = useState(true)
  const [createDialogOpen, setCreateDialogOpen] = useState(false)
  const [linkDialogOpen, setLinkDialogOpen] = useState(false)
  const [nodeRole, setNodeRole] = useState<string | null>(null)
  const [masterUrl, setMasterUrl] = useState<string | null>(null)

  // New table form state
  const [newTableName, setNewTableName] = useState("")
  const [columns, setColumns] = useState<{ name: string; type: string }[]>([
    { name: "id", type: "INT AUTO_INCREMENT PRIMARY KEY" },
  ])

  // Link tables form state
  const [sourceTable, setSourceTable] = useState("")
  const [targetTable, setTargetTable] = useState("")
  const [sourceColumn, setSourceColumn] = useState("")
  const [targetColumn, setTargetColumn] = useState("")
  const [constraintType, setConstraintType] = useState("RESTRICT")

  // Add a new state for selected shard ID
  const [selectedShardId, setSelectedShardId] = useState<number>(0)
  const [availableShards, setAvailableShards] = useState<number[]>([0, 1, 2]) // Default to 3 shards (0, 1, 2)

  const fetchTables = async () => {
    if (!dbName) return

    setLoading(true)
    try {
      const roleResponse = await apiService.getNodeRole()
      if (roleResponse.success && roleResponse.result) {
        setNodeRole(roleResponse.result.role)
        setMasterUrl(roleResponse.result.masterUrl || null)
      }

      const response = await apiService.getTables(dbName)
      if (response.success && response.result) {
        setTables(response.result)
      } else {
        toast({
          variant: "destructive",
          title: "Error",
          description: response.message || "Failed to fetch tables",
        })
      }
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "An error occurred while fetching tables",
      })
    } finally {
      setLoading(false)
    }
  }

  // Update the useEffect that fetches node information to also get shard count
  useEffect(() => {
    if (dbName) {
      fetchTables()

      // Get node information and available shards
      const fetchNodeInfo = async () => {
        try {
          const nodesResponse = await apiService.getNodes()
          if (nodesResponse.success && nodesResponse.result) {
            // Find the maximum shard ID to determine how many shards are available
            const maxShardId = Math.max(...nodesResponse.result.map((node) => node.shardId))
            const shards = Array.from({ length: maxShardId + 1 }, (_, i) => i)
            setAvailableShards(shards)
          }
        } catch (error) {
          console.error("Failed to fetch node information:", error)
        }
      }

      fetchNodeInfo()
    }
  }, [dbName])

  const addColumn = () => {
    setColumns([...columns, { name: "", type: "VARCHAR(255)" }])
  }

  const updateColumn = (index: number, field: "name" | "type", value: string) => {
    const newColumns = [...columns]
    newColumns[index][field] = value
    setColumns(newColumns)
  }

  const removeColumn = (index: number) => {
    if (columns.length > 1) {
      const newColumns = [...columns]
      newColumns.splice(index, 1)
      setColumns(newColumns)
    }
  }

  // Update the handleCreateTable function to use the selected shard ID
  const handleCreateTable = async () => {
    if (!dbName) return

    if (!newTableName.trim()) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "Table name cannot be empty",
      })
      return
    }

    // Validate columns
    for (const column of columns) {
      if (!column.name.trim() || !column.type.trim()) {
        toast({
          variant: "destructive",
          title: "Error",
          description: "Column name and type cannot be empty",
        })
        return
      }
    }

    // Convert columns to the format expected by the API
    const columnsObj: Record<string, string> = {}
    columns.forEach((col) => {
      columnsObj[col.name] = col.type
    })

    // Show a loading toast
    toast({
      title: "Creating table...",
      description: "Please wait while we create your table",
    })

    console.log("Creating table with data:", {
      dbName,
      tableName: newTableName,
      columns: columnsObj,
      shardId: selectedShardId,
    })

    try {
      // Use the selected shard ID instead of calculating it
      const response = await apiService.createTable(dbName, newTableName, columnsObj, selectedShardId)
      console.log("Create table response:", response)

      if (response.success) {
        toast({
          title: "Success",
          description: response.message || "Table created successfully",
        })
        setNewTableName("")
        setColumns([{ name: "id", type: "INT AUTO_INCREMENT PRIMARY KEY" }])
        setCreateDialogOpen(false)
        fetchTables()
      } else {
        toast({
          variant: "destructive",
          title: "Error",
          description: response.message || "Failed to create table. Check console for details.",
        })
      }
    } catch (error) {
      console.error("Error creating table:", error)
      toast({
        variant: "destructive",
        title: "Error",
        description: error instanceof Error ? error.message : "An unknown error occurred while creating the table",
      })
    }
  }

  const handleDeleteTable = async (tableName: string) => {
    if (!dbName) return

    try {
      const response = await apiService.dropTable(dbName, tableName)
      if (response.success) {
        toast({
          title: "Success",
          description: response.message || "Table deleted successfully",
        })
        fetchTables()
      } else {
        toast({
          variant: "destructive",
          title: "Error",
          description: response.message || "Failed to delete table",
        })
      }
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "An error occurred while deleting the table",
      })
    }
  }

  const handleLinkTables = async () => {
    if (!dbName || !sourceTable || !targetTable || !sourceColumn || !targetColumn) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "All fields are required",
      })
      return
    }

    try {
      const response = await apiService.linkTables(
        dbName,
        sourceTable,
        targetTable,
        sourceColumn,
        targetColumn,
        constraintType,
      )

      if (response.success) {
        toast({
          title: "Success",
          description: response.message || "Tables linked successfully",
        })
        setLinkDialogOpen(false)
        fetchTables()
      } else {
        toast({
          variant: "destructive",
          title: "Error",
          description: response.message || "Failed to link tables",
        })
      }
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "An error occurred while linking tables",
      })
    }
  }

  // Add the calculateShardId function to match the backend's algorithm
  const calculateShardId = (key: string, shardCount = 3): number => {
    let hash = 0
    for (let i = 0; i < key.length; i++) {
      hash = (hash * 31 + key.charCodeAt(i)) % shardCount
    }
    return (hash & 0x7fffffff) % shardCount
  }

  if (!dbName) {
    return (
      <div className="container mx-auto py-6">
        <div className="flex flex-col items-center justify-center py-12">
          <Table2 className="h-12 w-12 text-muted-foreground" />
          <h3 className="mt-4 text-xl font-medium">No Database Selected</h3>
          <p className="mt-2 text-center text-muted-foreground">Please select a database to view its tables.</p>
          <Button className="mt-4" asChild>
            <a href="/databases">Go to Databases</a>
          </Button>
        </div>
      </div>
    )
  }

  return (
    <div className="container mx-auto py-6">
      <div className="flex flex-col gap-6">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Button variant="outline" size="icon" asChild>
              <a href="/databases">
                <ArrowLeft className="h-4 w-4" />
              </a>
            </Button>
            <div>
              <h1 className="text-3xl font-bold tracking-tight">Tables</h1>
              <p className="text-muted-foreground">
                Manage tables in database <Badge variant="outline">{dbName}</Badge>
              </p>
            </div>
          </div>

          <div className="flex items-center gap-2">
            {nodeRole === "slave" && (
              <div className="flex items-center mr-4">
                <span className="text-sm text-muted-foreground mr-2">
                  Slave node: Table management is only available on master node.
                </span>
                {masterUrl && (
                  <Button variant="outline" size="sm" asChild>
                    <a href={`${masterUrl}/tables?db=${dbName}`} target="_blank" rel="noopener noreferrer">
                      Go to Master
                    </a>
                  </Button>
                )}
              </div>
            )}

            <div className="flex gap-2">
              <Dialog open={linkDialogOpen} onOpenChange={setLinkDialogOpen}>
                <DialogTrigger asChild>
                  <Button variant="outline" disabled={nodeRole === "slave"}>
                    <LinkIcon className="mr-2 h-4 w-4" />
                    Link Tables
                  </Button>
                </DialogTrigger>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Link Tables</DialogTitle>
                    <DialogDescription>Create a foreign key relationship between two tables.</DialogDescription>
                  </DialogHeader>
                  <div className="grid gap-4 py-4">
                    <div className="grid grid-cols-2 gap-4">
                      <div className="grid gap-2">
                        <Label htmlFor="sourceTable">Source Table</Label>
                        <Select value={sourceTable} onValueChange={setSourceTable}>
                          <SelectTrigger id="sourceTable">
                            <SelectValue placeholder="Select table" />
                          </SelectTrigger>
                          <SelectContent>
                            {tables.map((table) => (
                              <SelectItem key={table.name} value={table.name}>
                                {table.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                      <div className="grid gap-2">
                        <Label htmlFor="targetTable">Target Table</Label>
                        <Select value={targetTable} onValueChange={setTargetTable}>
                          <SelectTrigger id="targetTable">
                            <SelectValue placeholder="Select table" />
                          </SelectTrigger>
                          <SelectContent>
                            {tables.map((table) => (
                              <SelectItem key={table.name} value={table.name}>
                                {table.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                    </div>

                    <div className="grid grid-cols-2 gap-4">
                      <div className="grid gap-2">
                        <Label htmlFor="sourceColumn">Source Column</Label>
                        <Select value={sourceColumn} onValueChange={setSourceColumn}>
                          <SelectTrigger id="sourceColumn">
                            <SelectValue placeholder="Select column" />
                          </SelectTrigger>
                          <SelectContent>
                            {sourceTable &&
                              tables
                                .find((t) => t.name === sourceTable)
                                ?.columns.map((column) => (
                                  <SelectItem key={column.name} value={column.name}>
                                    {column.name}
                                  </SelectItem>
                                ))}
                          </SelectContent>
                        </Select>
                      </div>
                      <div className="grid gap-2">
                        <Label htmlFor="targetColumn">Target Column</Label>
                        <Select value={targetColumn} onValueChange={setTargetColumn}>
                          <SelectTrigger id="targetColumn">
                            <SelectValue placeholder="Select column" />
                          </SelectTrigger>
                          <SelectContent>
                            {targetTable &&
                              tables
                                .find((t) => t.name === targetTable)
                                ?.columns.map((column) => (
                                  <SelectItem key={column.name} value={column.name}>
                                    {column.name}
                                  </SelectItem>
                                ))}
                          </SelectContent>
                        </Select>
                      </div>
                    </div>

                    <div className="grid gap-2">
                      <Label htmlFor="constraintType">Constraint Type</Label>
                      <Select value={constraintType} onValueChange={setConstraintType}>
                        <SelectTrigger id="constraintType">
                          <SelectValue placeholder="Select constraint type" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="RESTRICT">RESTRICT</SelectItem>
                          <SelectItem value="CASCADE">CASCADE</SelectItem>
                          <SelectItem value="SET NULL">SET NULL</SelectItem>
                          <SelectItem value="NO ACTION">NO ACTION</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                  <DialogFooter>
                    <Button variant="outline" onClick={() => setLinkDialogOpen(false)}>
                      Cancel
                    </Button>
                    <Button onClick={handleLinkTables}>Link Tables</Button>
                  </DialogFooter>
                </DialogContent>
              </Dialog>

              <Dialog open={createDialogOpen} onOpenChange={setCreateDialogOpen}>
                <DialogTrigger asChild>
                  <Button disabled={nodeRole === "slave"}>
                    <Plus className="mr-2 h-4 w-4" />
                    Create Table
                  </Button>
                </DialogTrigger>
                <DialogContent className="max-w-2xl">
                  <DialogHeader>
                    <DialogTitle>Create New Table</DialogTitle>
                    <DialogDescription>Define the structure of your new table.</DialogDescription>
                  </DialogHeader>
                  <div className="grid gap-4 py-4">
                    <div className="grid gap-2">
                      <Label htmlFor="tableName">Table Name</Label>
                      <Input
                        id="tableName"
                        value={newTableName}
                        onChange={(e) => setNewTableName(e.target.value)}
                        placeholder="Enter table name"
                      />
                    </div>

                    <div className="grid gap-2">
                      <Label htmlFor="shardId">Shard ID</Label>
                      <Select
                        value={selectedShardId.toString()}
                        onValueChange={(value) => setSelectedShardId(Number.parseInt(value))}
                      >
                        <SelectTrigger id="shardId">
                          <SelectValue placeholder="Select shard ID" />
                        </SelectTrigger>
                        <SelectContent>
                          {availableShards.map((shardId) => (
                            <SelectItem key={shardId} value={shardId.toString()}>
                              Shard {shardId}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <p className="text-xs text-muted-foreground">
                        Select the shard where this table will be created. Tables in the same shard can be joined more
                        efficiently.
                      </p>
                    </div>

                    <div className="grid gap-2">
                      <div className="flex items-center justify-between">
                        <Label>Columns</Label>
                        <Button type="button" variant="outline" size="sm" onClick={addColumn}>
                          <Plus className="mr-2 h-3 w-3" />
                          Add Column
                        </Button>
                      </div>

                      <div className="space-y-2">
                        {columns.map((column, index) => (
                          <div key={index} className="flex items-center gap-2">
                            <Input
                              value={column.name}
                              onChange={(e) => updateColumn(index, "name", e.target.value)}
                              placeholder="Column name"
                              className="flex-1"
                            />
                            <Input
                              value={column.type}
                              onChange={(e) => updateColumn(index, "type", e.target.value)}
                              placeholder="Data type"
                              className="flex-1"
                            />
                            <Button
                              type="button"
                              variant="ghost"
                              size="icon"
                              onClick={() => removeColumn(index)}
                              disabled={columns.length <= 1}
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                  <DialogFooter>
                    <Button variant="outline" onClick={() => setCreateDialogOpen(false)}>
                      Cancel
                    </Button>
                    <Button onClick={handleCreateTable}>Create Table</Button>
                  </DialogFooter>
                </DialogContent>
              </Dialog>
            </div>
          </div>
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-12">
            <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent"></div>
          </div>
        ) : tables.length === 0 ? (
          <Card>
            <CardContent className="flex flex-col items-center justify-center py-12">
              <Table2 className="h-12 w-12 text-muted-foreground" />
              <h3 className="mt-4 text-xl font-medium">No Tables Found</h3>
              <p className="mt-2 text-center text-muted-foreground">
                This database doesn't have any tables yet. Click the "Create Table" button to get started.
              </p>
            </CardContent>
          </Card>
        ) : (
          <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
            {tables.map((table) => (
              <Card key={table.name}>
                <CardHeader className="pb-2">
                  <CardTitle className="flex items-center gap-2">
                    <Table2 className="h-5 w-5" />
                    <span>{table.name}</span>
                  </CardTitle>
                  <CardDescription>
                    {table.columns.length} columns • Shard {table.shardId}
                    {table.shardKey && ` • Shard Key: ${table.shardKey}`}
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <Tabs defaultValue="columns">
                    <TabsList className="grid w-full grid-cols-2">
                      <TabsTrigger value="columns">Columns</TabsTrigger>
                      <TabsTrigger value="actions">Actions</TabsTrigger>
                    </TabsList>
                    <TabsContent value="columns" className="mt-2">
                      <div className="max-h-[200px] overflow-y-auto rounded-md border">
                        <div className="divide-y">
                          {table.columns.map((column, index) => (
                            <div key={index} className="flex items-center justify-between p-2 text-sm">
                              <span className="font-medium">{column.name}</span>
                              <span className="text-muted-foreground">{column.type}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    </TabsContent>
                    <TabsContent value="actions" className="mt-2">
                      <div className="flex flex-col gap-2">
                        <Button variant="outline" asChild>
                          <a href={`/crud?db=${dbName}&table=${table.name}`}>Manage Data</a>
                        </Button>

                        <AlertDialog>
                          <AlertDialogTrigger asChild>
                            <Button variant="destructive" disabled={nodeRole === "slave"}>
                              <Trash2 className="mr-2 h-4 w-4" />
                              Delete Table
                            </Button>
                          </AlertDialogTrigger>
                          <AlertDialogContent>
                            <AlertDialogHeader>
                              <AlertDialogTitle>Are you sure?</AlertDialogTitle>
                              <AlertDialogDescription>
                                This will permanently delete the table "{table.name}" and all its data. This action
                                cannot be undone.
                              </AlertDialogDescription>
                            </AlertDialogHeader>
                            <AlertDialogFooter>
                              <AlertDialogCancel>Cancel</AlertDialogCancel>
                              <AlertDialogAction
                                onClick={() => handleDeleteTable(table.name)}
                                className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                              >
                                Delete
                              </AlertDialogAction>
                            </AlertDialogFooter>
                          </AlertDialogContent>
                        </AlertDialog>
                      </div>
                    </TabsContent>
                  </Tabs>
                </CardContent>
              </Card>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
