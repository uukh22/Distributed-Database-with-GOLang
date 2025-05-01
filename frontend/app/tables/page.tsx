"use client"

import { useState, useEffect } from "react"
import { useSearchParams } from "next/navigation"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { useToast } from "@/components/ui/use-toast"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table2, Plus, Trash2, LinkIcon, RefreshCw, AlertTriangle } from "lucide-react"
import { Separator } from "@/components/ui/separator"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"

type Column = {
  name: string
  type: string
}

type TableInfo = {
  name: string
  columns: Column[]
}

export default function TablesPage() {
  const searchParams = useSearchParams()
  const dbName = searchParams.get("db") || "No database selected"
  const { toast } = useToast()

  const [tableName, setTableName] = useState("")
  const [columns, setColumns] = useState<Column[]>([{ name: "", type: "INT" }])
  const [tables, setTables] = useState<TableInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [isMaster, setIsMaster] = useState(true)
  const [masterUrl, setMasterUrl] = useState("")

  // For table linking
  const [table1, setTable1] = useState("")
  const [table2, setTable2] = useState("")
  const [column1, setColumn1] = useState("")
  const [column2, setColumn2] = useState("")
  const [availableTables, setAvailableTables] = useState<string[]>([])
  const [tableColumns, setTableColumns] = useState<Record<string, Column[]>>({})
  const [linkLoading, setLinkLoading] = useState(false)

  // For table deletion
  const [tableToDelete, setTableToDelete] = useState<string | null>(null)
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false)
  const [deleteLoading, setDeleteLoading] = useState(false)

  const columnTypes = ["INT", "VARCHAR(255)", "TEXT", "DATE", "DATETIME", "BOOLEAN", "FLOAT", "DECIMAL(10,2)"]

  // Check if current node is master
  const checkNodeRole = async () => {
    try {
      const response = await fetch("/api/node-role")
      const data = await response.json()

      if (data.success) {
        setIsMaster(data.result.role === "master")
        if (data.result.role !== "master") {
          setMasterUrl(data.result.masterUrl)
        }
      }
    } catch (error) {
      console.error("Failed to check node role:", error)
    }
  }

  // Fetch existing tables
  const fetchTables = async () => {
    if (!dbName || dbName === "No database selected") {
      return
    }

    setRefreshing(true)
    try {
      const response = await fetch(`/api/list-tables?db=${dbName}`)
      const data = await response.json()

      if (data.success) {
        const tableData = data.result || []
        setTables(tableData)

        // Extract table names and columns for dropdowns
        const tableNames = tableData.map((t: TableInfo) => t.name)
        setAvailableTables(tableNames)

        const columnsMap: Record<string, Column[]> = {}
        tableData.forEach((table: TableInfo) => {
          columnsMap[table.name] = table.columns
        })
        setTableColumns(columnsMap)
      } else {
        toast({
          title: "Error",
          description: data.message || "Failed to fetch tables",
          variant: "destructive",
        })
      }
    } catch (error) {
      toast({
        title: "Error",
        description: "Connection error. Please check if the backend is running.",
        variant: "destructive",
      })
    } finally {
      setRefreshing(false)
    }
  }

  useEffect(() => {
    checkNodeRole()
    fetchTables()
  }, [dbName])

  const addColumn = () => {
    setColumns([...columns, { name: "", type: "INT" }])
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

  const createTable = async () => {
    if (!tableName.trim()) {
      toast({
        title: "Error",
        description: "Please enter a table name",
        variant: "destructive",
      })
      return
    }

    const validColumns = columns.filter((col) => col.name.trim() !== "")
    if (validColumns.length === 0) {
      toast({
        title: "Error",
        description: "Please add at least one column",
        variant: "destructive",
      })
      return
    }

    setLoading(true)

    try {
      // Convert columns array to object for API
      const columnsObj: Record<string, string> = {}
      validColumns.forEach((col) => {
        columnsObj[col.name] = col.type
      })

      const response = await fetch("/api/create-table", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dbName,
          tableName,
          columns: columnsObj,
        }),
      })

      const data = await response.json()

      if (data.success) {
        setTables([...tables, { name: tableName, columns: validColumns }])
        setAvailableTables([...availableTables, tableName])
        setTableColumns({
          ...tableColumns,
          [tableName]: validColumns,
        })
        setTableName("")
        setColumns([{ name: "", type: "INT" }])
        toast({
          title: "Success",
          description: data.message || `Table "${tableName}" created successfully`,
        })
      } else {
        toast({
          title: "Error",
          description: data.message || "Failed to create table",
          variant: "destructive",
        })
      }
    } catch (error) {
      toast({
        title: "Error",
        description: "Connection error. Please check if the backend is running.",
        variant: "destructive",
      })
    } finally {
      setLoading(false)
    }
  }

  const linkTables = async () => {
    if (!table1 || !table2 || !column1 || !column2) {
      toast({
        title: "Error",
        description: "Please fill in all fields",
        variant: "destructive",
      })
      return
    }

    setLinkLoading(true)
    try {
      const response = await fetch("/api/link-tables", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dbName,
          table1,
          table2,
          column1,
          column2,
          constraintType: "primary",
        }),
      })

      const data = await response.json()

      if (data.success) {
        setTable1("")
        setTable2("")
        setColumn1("")
        setColumn2("")
        toast({
          title: "Success",
          description: "Tables linked successfully!",
          variant: "success",
        })

        // Refresh tables list to show the updated relationships
        fetchTables()
      } else {
        toast({
          title: "Error",
          description: data.message || "Failed to link tables",
          variant: "destructive",
        })
      }
    } catch (error) {
      toast({
        title: "Error",
        description: "Connection error. Please check if the backend is running.",
        variant: "destructive",
      })
    } finally {
      setLinkLoading(false)
    }
  }

  const deleteTable = async () => {
    if (!tableToDelete) return

    setDeleteLoading(true)
    try {
      const response = await fetch("/api/drop-table", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dbName,
          tableName: tableToDelete,
        }),
      })

      const data = await response.json()

      if (data.success) {
        setTables(tables.filter((table) => table.name !== tableToDelete))
        setAvailableTables(availableTables.filter((t) => t !== tableToDelete))

        // Remove from tableColumns
        const newTableColumns = { ...tableColumns }
        delete newTableColumns[tableToDelete]
        setTableColumns(newTableColumns)

        toast({
          title: "Success",
          description: `Table "${tableToDelete}" deleted successfully`,
        })
      } else {
        toast({
          title: "Error",
          description: data.message || "Failed to delete table",
          variant: "destructive",
        })
      }
    } catch (error) {
      toast({
        title: "Error",
        description: "Connection error. Please check if the backend is running.",
        variant: "destructive",
      })
    } finally {
      setDeleteLoading(false)
      setDeleteDialogOpen(false)
      setTableToDelete(null)
    }
  }

  const confirmDeleteTable = (name: string) => {
    setTableToDelete(name)
    setDeleteDialogOpen(true)
  }

  return (
    <div className="container mx-auto">
      <div className="flex flex-col gap-6">
        <div className="flex flex-col gap-2">
          <h1 className="text-3xl font-bold tracking-tight">Table Management</h1>
          <p className="text-muted-foreground">
            Database: <span className="font-medium">{dbName}</span>
          </p>
        </div>

        {!isMaster && (
          <Alert variant="warning">
            <AlertTriangle className="h-4 w-4" />
            <AlertTitle>Read-Only Mode</AlertTitle>
            <AlertDescription>
              This node is a slave. Table creation, linking, and deletion operations must be performed on the master
              node.
              {masterUrl && (
                <div className="mt-2">
                  <a href={`${masterUrl}/tables?db=${dbName}`} className="text-blue-500 hover:underline">
                    Go to Master Node
                  </a>
                </div>
              )}
            </AlertDescription>
          </Alert>
        )}

        <Tabs defaultValue="create">
          <TabsList className="grid w-full grid-cols-2">
            <TabsTrigger value="create">Create Table</TabsTrigger>
            <TabsTrigger value="link">Link Tables</TabsTrigger>
          </TabsList>

          <TabsContent value="create">
            <Card>
              <CardHeader>
                <CardTitle>Create Table</CardTitle>
                <CardDescription>Define your table schema with columns and data types</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-col gap-4">
                  <Input
                    placeholder="Table name"
                    value={tableName}
                    onChange={(e) => setTableName(e.target.value)}
                    disabled={!isMaster}
                  />

                  <div className="flex flex-col gap-2">
                    <div className="flex items-center justify-between">
                      <h3 className="text-sm font-medium">Columns</h3>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={addColumn}
                        className="flex items-center gap-1"
                        disabled={!isMaster}
                      >
                        <Plus className="h-3.5 w-3.5" />
                        Add Column
                      </Button>
                    </div>

                    {columns.map((column, index) => (
                      <div key={index} className="flex items-center gap-2">
                        <Input
                          placeholder="Column name"
                          value={column.name}
                          onChange={(e) => updateColumn(index, "name", e.target.value)}
                          className="flex-1"
                          disabled={!isMaster}
                        />
                        <Select
                          value={column.type}
                          onValueChange={(value) => updateColumn(index, "type", value)}
                          disabled={!isMaster}
                        >
                          <SelectTrigger className="w-[180px]">
                            <SelectValue placeholder="Data type" />
                          </SelectTrigger>
                          <SelectContent>
                            {columnTypes.map((type) => (
                              <SelectItem key={type} value={type}>
                                {type}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => removeColumn(index)}
                          disabled={columns.length <= 1 || !isMaster}
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                    ))}
                  </div>
                </div>
              </CardContent>
              <CardFooter>
                <Button onClick={createTable} disabled={loading || !isMaster} className="ml-auto">
                  {loading ? "Creating..." : "Create Table"}
                </Button>
              </CardFooter>
            </Card>
          </TabsContent>

          <TabsContent value="link">
            <Card>
              <CardHeader>
                <CardTitle>Link Tables</CardTitle>
                <CardDescription>Create relationships between tables</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-col gap-4">
                  <div className="grid grid-cols-2 gap-4">
                    <div className="flex flex-col gap-2">
                      <label className="text-sm font-medium">Primary Table</label>
                      <Select value={table1} onValueChange={setTable1} disabled={!isMaster}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select primary table" />
                        </SelectTrigger>
                        <SelectContent>
                          {availableTables.map((table) => (
                            <SelectItem key={table} value={table}>
                              {table}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="flex flex-col gap-2">
                      <label className="text-sm font-medium">Foreign Table</label>
                      <Select value={table2} onValueChange={setTable2} disabled={!isMaster}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select foreign table" />
                        </SelectTrigger>
                        <SelectContent>
                          {availableTables.map((table) => (
                            <SelectItem key={table} value={table}>
                              {table}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>

                  <div className="grid grid-cols-2 gap-4">
                    <div className="flex flex-col gap-2">
                      <label className="text-sm font-medium">Primary Key Column</label>
                      <Select value={column1} onValueChange={setColumn1} disabled={!table1 || !isMaster}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select primary key column" />
                        </SelectTrigger>
                        <SelectContent>
                          {table1 &&
                            tableColumns[table1]?.map((col) => (
                              <SelectItem key={col.name} value={col.name}>
                                {col.name} ({col.type})
                              </SelectItem>
                            ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="flex flex-col gap-2">
                      <label className="text-sm font-medium">Foreign Key Column</label>
                      <Select value={column2} onValueChange={setColumn2} disabled={!table2 || !isMaster}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select foreign key column" />
                        </SelectTrigger>
                        <SelectContent>
                          {table2 &&
                            tableColumns[table2]?.map((col) => (
                              <SelectItem key={col.name} value={col.name}>
                                {col.name} ({col.type})
                              </SelectItem>
                            ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                </div>
              </CardContent>
              <CardFooter>
                <Button onClick={linkTables} className="ml-auto" disabled={linkLoading || !isMaster}>
                  {linkLoading ? (
                    <>
                      <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                      Linking...
                    </>
                  ) : (
                    <>
                      <LinkIcon className="mr-2 h-4 w-4" />
                      Link Tables
                    </>
                  )}
                </Button>
              </CardFooter>
            </Card>
          </TabsContent>
        </Tabs>

        <Separator className="my-4" />

        <div className="flex flex-col gap-4">
          <div className="flex items-center justify-between">
            <h2 className="text-xl font-semibold">Your Tables</h2>
            <Button variant="outline" size="sm" onClick={fetchTables} disabled={refreshing}>
              {refreshing ? (
                <>
                  <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                  Refreshing...
                </>
              ) : (
                <>
                  <RefreshCw className="mr-2 h-4 w-4" />
                  Refresh
                </>
              )}
            </Button>
          </div>

          {tables.length === 0 ? (
            <Alert>
              <Table2 className="h-4 w-4" />
              <AlertTitle>No tables found</AlertTitle>
              <AlertDescription>Create your first table to get started.</AlertDescription>
            </Alert>
          ) : (
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
              {tables.map((table) => (
                <Card key={table.name}>
                  <CardHeader className="pb-2">
                    <CardTitle className="flex items-center gap-2 text-lg">
                      <Table2 className="h-4 w-4" />
                      {table.name}
                    </CardTitle>
                    <CardDescription>
                      {table.columns.length} column{table.columns.length !== 1 ? "s" : ""}
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="text-sm">
                    <div className="max-h-32 overflow-y-auto">
                      {table.columns.map((col, i) => (
                        <div key={i} className="flex justify-between py-1">
                          <span className="font-medium">{col.name}</span>
                          <span className="text-muted-foreground">{col.type}</span>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                  <CardFooter className="flex justify-between pt-2">
                    <Button
                      variant="outline"
                      onClick={() => (window.location.href = `/crud?db=${dbName}&table=${table.name}`)}
                    >
                      Manage Data
                    </Button>
                    {isMaster && (
                      <Button variant="destructive" size="icon" onClick={() => confirmDeleteTable(table.name)}>
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    )}
                  </CardFooter>
                </Card>
              ))}
            </div>
          )}
        </div>
      </div>

      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Are you absolutely sure?</AlertDialogTitle>
            <AlertDialogDescription>
              This action will permanently delete the table "{tableToDelete}" and all its data. This action cannot be
              undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={deleteTable} disabled={deleteLoading} className="bg-red-600 hover:bg-red-700">
              {deleteLoading ? "Deleting..." : "Delete Table"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
