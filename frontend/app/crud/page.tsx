"use client"

import { useState, useEffect } from "react"
import { useSearchParams } from "next/navigation"
import { Plus, Trash2, Edit, Save, X } from "lucide-react"

import { apiService } from "@/lib/api-service"
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
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"

export default function CrudPage() {
  const searchParams = useSearchParams()
  const dbName = searchParams.get("db")
  const tableName = searchParams.get("table")

  const [databases, setDatabases] = useState<string[]>([])
  const [tables, setTables] = useState<any[]>([])
  const [selectedDb, setSelectedDb] = useState<string>(dbName || "")
  const [selectedTable, setSelectedTable] = useState<string>(tableName || "")
  const [columns, setColumns] = useState<any[]>([])
  const [records, setRecords] = useState<any[]>([])
  const [loading, setLoading] = useState(false)
  const [nodeRole, setNodeRole] = useState<string | null>(null)
  const [masterUrl, setMasterUrl] = useState<string | null>(null)
  const [nodeInfo, setNodeInfo] = useState<{ role: string; shardId: number; masterUrl?: string } | null>(null)

  // Form states
  const [createDialogOpen, setCreateDialogOpen] = useState(false)
  const [newRecord, setNewRecord] = useState<Record<string, any>>({})
  const [editingRecord, setEditingRecord] = useState<Record<string, any> | null>(null)
  const [editingId, setEditingId] = useState<string | null>(null)

  // Fetch node role
  useEffect(() => {
    const fetchNodeRole = async () => {
      try {
        const response = await apiService.getNodeRole()
        if (response.success && response.result) {
          setNodeRole(response.result.role)
          setMasterUrl(response.result.masterUrl || null)
          setNodeInfo(response.result) // Store the full node info
        }
      } catch (error) {
        console.error("Failed to fetch node role:", error)
      }
    }

    fetchNodeRole()
  }, [])

  // Fetch databases
  useEffect(() => {
    const fetchDatabases = async () => {
      try {
        const response = await apiService.getDatabases()
        if (response.success && response.result) {
          setDatabases(response.result)
        }
      } catch (error) {
        console.error("Failed to fetch databases:", error)
      }
    }

    fetchDatabases()
  }, [])

  // Fetch tables when database changes
  useEffect(() => {
    if (!selectedDb) return

    const fetchTables = async () => {
      try {
        const response = await apiService.getTables(selectedDb)
        if (response.success && response.result) {
          setTables(response.result)
        }
      } catch (error) {
        console.error("Failed to fetch tables:", error)
      }
    }

    fetchTables()
  }, [selectedDb])

  // Update columns when table changes
  useEffect(() => {
    if (!selectedDb || !selectedTable) {
      setColumns([])
      return
    }

    const table = tables.find((t) => t.name === selectedTable)
    if (table) {
      setColumns(table.columns)
      fetchRecords()
    }
  }, [selectedDb, selectedTable, tables])

  // Update the CRUD operations to include shardKeyValue when needed
  const handleCreateRecord = async () => {
    if (!selectedDb || !selectedTable) return

    try {
      // Find the table's shard key if it exists
      const table = tables.find((t) => t.name === selectedTable)
      let shardKeyValue = null

      if (table && table.shardKey && newRecord[table.shardKey]) {
        shardKeyValue = newRecord[table.shardKey]
      }

      const response = await apiService.createRecord(selectedDb, selectedTable, newRecord, shardKeyValue)
      if (response.success) {
        toast({
          title: "Success",
          description: "Record created successfully",
        })
        setCreateDialogOpen(false)
        setNewRecord({})
        fetchRecords()
      } else {
        toast({
          variant: "destructive",
          title: "Error",
          description: response.message || "Failed to create record",
        })
      }
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "An error occurred while creating the record",
      })
    }
  }

  const handleUpdateRecord = async () => {
    if (!selectedDb || !selectedTable || !editingRecord || !editingId) return

    try {
      // Find the table's shard key if it exists
      const table = tables.find((t) => t.name === selectedTable)
      let shardKeyValue = null

      if (table && table.shardKey && editingRecord[table.shardKey]) {
        shardKeyValue = editingRecord[table.shardKey]
      }

      // Find the primary key column (usually 'id')
      const idColumn = columns.find((col) => col.name.toLowerCase() === "id" || col.name.endsWith("_id"))?.name || "id"

      const where = { [idColumn]: editingId }
      const response = await apiService.updateRecords(selectedDb, selectedTable, editingRecord, where, shardKeyValue)

      if (response.success) {
        toast({
          title: "Success",
          description: "Record updated successfully",
        })
        setEditingRecord(null)
        setEditingId(null)
        fetchRecords()
      } else {
        toast({
          variant: "destructive",
          title: "Error",
          description: response.message || "Failed to update record",
        })
      }
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "An error occurred while updating the record",
      })
    }
  }

  const handleDeleteRecord = async (record: any) => {
    if (!selectedDb || !selectedTable) return

    try {
      // Find the table's shard key if it exists
      const table = tables.find((t) => t.name === selectedTable)
      let shardKeyValue = null

      if (table && table.shardKey && record[table.shardKey]) {
        shardKeyValue = record[table.shardKey]
      }

      // Find the primary key column (usually 'id')
      const idColumn = columns.find((col) => col.name.toLowerCase() === "id" || col.name.endsWith("_id"))?.name || "id"

      const where = { [idColumn]: record[idColumn] }
      const response = await apiService.deleteRecords(selectedDb, selectedTable, where, shardKeyValue)

      if (response.success) {
        toast({
          title: "Success",
          description: "Record deleted successfully",
        })
        fetchRecords()
      } else {
        toast({
          variant: "destructive",
          title: "Error",
          description: response.message || "Failed to delete record",
        })
      }
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "An error occurred while deleting the record",
      })
    }
  }

  const fetchRecords = async () => {
    if (!selectedDb || !selectedTable) return

    setLoading(true)
    try {
      let response

      // If this is a slave node, we need to specify the shard ID
      if (nodeRole === "slave" && nodeInfo && nodeInfo.shardId !== undefined) {
        // Use the slave node's shard ID to fetch records
        response = await apiService.fetchRecordsForShard(selectedDb, selectedTable, nodeInfo.shardId)
      } else {
        // Regular read for master node
        response = await apiService.readRecords(selectedDb, selectedTable)
      }

      if (response.success && response.result) {
        setRecords(response.result)
      } else {
        toast({
          variant: "destructive",
          title: "Error",
          description: response.message || "Failed to fetch records",
        })
      }
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "An error occurred while fetching records",
      })
    } finally {
      setLoading(false)
    }
  }

  const startEditing = (record: any) => {
    // Find the primary key column (usually 'id')
    const idColumn = columns.find((col) => col.name.toLowerCase() === "id" || col.name.endsWith("_id"))?.name || "id"

    setEditingRecord({ ...record })
    setEditingId(record[idColumn])
  }

  const cancelEditing = () => {
    setEditingRecord(null)
    setEditingId(null)
  }

  const updateEditingField = (field: string, value: any) => {
    setEditingRecord((prev) => ({ ...prev, [field]: value }))
  }

  const updateNewRecordField = (field: string, value: any) => {
    setNewRecord((prev) => ({ ...prev, [field]: value }))
  }

  return (
    <div className="container mx-auto py-6">
      <div className="flex flex-col gap-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">CRUD Operations</h1>
            <p className="text-muted-foreground">Manage data in your tables</p>
          </div>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Select Database and Table</CardTitle>
            <CardDescription>Choose a database and table to perform operations</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="grid gap-2">
                <Label htmlFor="database">Database</Label>
                <Select value={selectedDb} onValueChange={setSelectedDb}>
                  <SelectTrigger id="database">
                    <SelectValue placeholder="Select database" />
                  </SelectTrigger>
                  <SelectContent>
                    {databases.map((db) => (
                      <SelectItem key={db} value={db}>
                        {db}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="grid gap-2">
                <Label htmlFor="table">Table</Label>
                <Select
                  value={selectedTable}
                  onValueChange={setSelectedTable}
                  disabled={!selectedDb || tables.length === 0}
                >
                  <SelectTrigger id="table">
                    <SelectValue
                      placeholder={
                        !selectedDb
                          ? "Select database first"
                          : tables.length === 0
                            ? "No tables available"
                            : "Select table"
                      }
                    />
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
          </CardContent>
        </Card>

        {selectedDb && selectedTable && (
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <div>
                <CardTitle>
                  Table: <Badge variant="outline">{selectedTable}</Badge>
                </CardTitle>
                <CardDescription>Database: {selectedDb}</CardDescription>
              </div>

              <div className="flex items-center gap-2">
                {nodeRole === "slave" && masterUrl && (
                  <Button variant="outline" size="sm" asChild className="ml-auto">
                    <a
                      href={`${masterUrl}/crud?db=${selectedDb}&table=${selectedTable}`}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      Go to Master
                    </a>
                  </Button>
                )}

                <Dialog open={createDialogOpen} onOpenChange={setCreateDialogOpen}>
                  <DialogTrigger asChild>
                    <Button>
                      <Plus className="mr-2 h-4 w-4" />
                      Add Record
                    </Button>
                  </DialogTrigger>
                  <DialogContent>
                    <DialogHeader>
                      <DialogTitle>Create New Record</DialogTitle>
                      <DialogDescription>Add a new record to the {selectedTable} table.</DialogDescription>
                    </DialogHeader>
                    <div className="grid gap-4 py-4">
                      {columns.map((column) => {
                        const isShardKey =
                          selectedTable && tables.find((t) => t.name === selectedTable)?.shardKey === column.name

                        return (
                          <div key={column.name} className="grid gap-2">
                            <Label htmlFor={`new-${column.name}`}>
                              {column.name}
                              {isShardKey && (
                                <Badge
                                  variant="outline"
                                  className="ml-2 bg-amber-50 text-amber-700 dark:bg-amber-900/20 dark:text-amber-400"
                                >
                                  Shard Key
                                </Badge>
                              )}
                            </Label>
                            <Input
                              id={`new-${column.name}`}
                              value={newRecord[column.name] || ""}
                              onChange={(e) => updateNewRecordField(column.name, e.target.value)}
                              placeholder={column.type}
                              disabled={column.name.toLowerCase() === "id" || column.name.endsWith("_id")}
                              className={isShardKey ? "border-amber-300 focus-visible:ring-amber-300" : ""}
                            />
                          </div>
                        )
                      })}
                    </div>
                    <DialogFooter>
                      <Button variant="outline" onClick={() => setCreateDialogOpen(false)}>
                        Cancel
                      </Button>
                      <Button onClick={handleCreateRecord}>Create Record</Button>
                    </DialogFooter>
                  </DialogContent>
                </Dialog>
              </div>
            </CardHeader>
            <CardContent>
              {loading ? (
                <div className="flex items-center justify-center py-12">
                  <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent"></div>
                </div>
              ) : records.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-12">
                  <p className="text-center text-muted-foreground">No records found in this table.</p>
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        {columns.map((column) => (
                          <TableHead key={column.name}>{column.name}</TableHead>
                        ))}
                        <TableHead className="text-right">Actions</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {records.map((record, index) => (
                        <TableRow key={index}>
                          {columns.map((column) => (
                            <TableCell key={column.name}>
                              {editingId &&
                              editingRecord &&
                              record[
                                columns.find((col) => col.name.toLowerCase() === "id" || col.name.endsWith("_id"))
                                  ?.name || "id"
                              ] === editingId ? (
                                <Input
                                  value={editingRecord[column.name] || ""}
                                  onChange={(e) => updateEditingField(column.name, e.target.value)}
                                  disabled={column.name.toLowerCase() === "id" || column.name.endsWith("_id")}
                                />
                              ) : (
                                <span>{record[column.name] !== null ? record[column.name] : "-"}</span>
                              )}
                            </TableCell>
                          ))}

                          <TableCell className="text-right">
                            {editingId &&
                            editingRecord &&
                            record[
                              columns.find((col) => col.name.toLowerCase() === "id" || col.name.endsWith("_id"))
                                ?.name || "id"
                            ] === editingId ? (
                              <div className="flex justify-end gap-2">
                                <Button variant="outline" size="icon" onClick={handleUpdateRecord}>
                                  <Save className="h-4 w-4" />
                                </Button>
                                <Button variant="ghost" size="icon" onClick={cancelEditing}>
                                  <X className="h-4 w-4" />
                                </Button>
                              </div>
                            ) : (
                              <div className="flex justify-end gap-2">
                                <Button variant="outline" size="icon" onClick={() => startEditing(record)}>
                                  <Edit className="h-4 w-4" />
                                </Button>
                                <AlertDialog>
                                  <AlertDialogTrigger asChild>
                                    <Button variant="destructive" size="icon">
                                      <Trash2 className="h-4 w-4" />
                                    </Button>
                                  </AlertDialogTrigger>
                                  <AlertDialogContent>
                                    <AlertDialogHeader>
                                      <AlertDialogTitle>Are you sure?</AlertDialogTitle>
                                      <AlertDialogDescription>
                                        This will permanently delete this record. This action cannot be undone.
                                      </AlertDialogDescription>
                                    </AlertDialogHeader>
                                    <AlertDialogFooter>
                                      <AlertDialogCancel>Cancel</AlertDialogCancel>
                                      <AlertDialogAction
                                        onClick={() => handleDeleteRecord(record)}
                                        className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                                      >
                                        Delete
                                      </AlertDialogAction>
                                    </AlertDialogFooter>
                                  </AlertDialogContent>
                                </AlertDialog>
                              </div>
                            )}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  )
}
