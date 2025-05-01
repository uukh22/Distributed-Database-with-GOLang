"use client"

import { useState } from "react"
import { useSearchParams } from "next/navigation"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import { useToast } from "@/components/ui/use-toast"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { FileText, Plus, RefreshCw, Search, Trash2, Edit } from "lucide-react"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"

export default function CrudPage() {
  const searchParams = useSearchParams()
  const dbName = searchParams.get("db") || "No database selected"
  const tableName = searchParams.get("table") || ""
  const { toast } = useToast()

  const [createTableName, setCreateTableName] = useState(tableName)
  const [recordData, setRecordData] = useState("")
  const [readTableName, setReadTableName] = useState(tableName)
  const [records, setRecords] = useState<any[]>([])
  const [loading, setLoading] = useState(false)

  // For update/delete operations
  const [selectedRecord, setSelectedRecord] = useState<any>(null)
  const [updateData, setUpdateData] = useState("")
  const [whereData, setWhereData] = useState("")

  const createRecord = async () => {
    if (!createTableName.trim()) {
      toast({
        title: "Error",
        description: "Please enter a table name",
        variant: "destructive",
      })
      return
    }

    try {
      const data = JSON.parse(recordData)
      setLoading(true)

      const response = await fetch("/api/crud", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dbName,
          operation: "create",
          table: createTableName,
          data,
        }),
      })

      const result = await response.json()

      if (result.success) {
        setRecordData("")
        toast({
          title: "Success",
          description: result.message || "Record created successfully",
        })

        // Refresh records if we're on the same table
        if (createTableName === readTableName) {
          readRecords()
        }
      } else {
        toast({
          title: "Error",
          description: result.message || "Failed to create record",
          variant: "destructive",
        })
      }
    } catch (error: any) {
      toast({
        title: "Error",
        description: error.message || "Invalid JSON data",
        variant: "destructive",
      })
    } finally {
      setLoading(false)
    }
  }

  const readRecords = async () => {
    if (!readTableName.trim()) {
      toast({
        title: "Error",
        description: "Please enter a table name",
        variant: "destructive",
      })
      return
    }

    setLoading(true)

    try {
      const response = await fetch("/api/crud", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dbName,
          operation: "read",
          table: readTableName,
        }),
      })

      const result = await response.json()

      if (result.success) {
        setRecords(result.result || [])
        toast({
          title: "Success",
          description: `Retrieved ${result.result?.length || 0} records from ${readTableName}`,
        })
      } else {
        toast({
          title: "Error",
          description: result.message || "Failed to read records",
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

  const updateRecord = async () => {
    if (!readTableName.trim()) {
      toast({
        title: "Error",
        description: "Please enter a table name",
        variant: "destructive",
      })
      return
    }

    try {
      const data = JSON.parse(updateData)
      const where = JSON.parse(whereData)

      setLoading(true)

      const response = await fetch("/api/crud", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dbName,
          operation: "update",
          table: readTableName,
          data,
          where,
        }),
      })

      const result = await response.json()

      if (result.success) {
        setUpdateData("")
        setWhereData("")
        setSelectedRecord(null)
        toast({
          title: "Success",
          description: "Record updated successfully",
        })

        // Refresh the records
        readRecords()
      } else {
        toast({
          title: "Error",
          description: result.message || "Failed to update record",
          variant: "destructive",
        })
      }
    } catch (error: any) {
      toast({
        title: "Error",
        description: error.message || "Invalid JSON data",
        variant: "destructive",
      })
    } finally {
      setLoading(false)
    }
  }

  const deleteRecord = async (record: any) => {
    if (!readTableName.trim()) {
      toast({
        title: "Error",
        description: "Please enter a table name",
        variant: "destructive",
      })
      return
    }

    // Create a where condition based on the first column (usually id)
    const firstKey = Object.keys(record)[0]
    const where = { [firstKey]: record[firstKey] }

    setLoading(true)

    try {
      const response = await fetch("/api/crud", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dbName,
          operation: "delete",
          table: readTableName,
          where,
        }),
      })

      const result = await response.json()

      if (result.success) {
        toast({
          title: "Success",
          description: "Record deleted successfully",
        })

        // Refresh the records
        readRecords()
      } else {
        toast({
          title: "Error",
          description: result.message || "Failed to delete record",
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

  const prepareUpdate = (record: any) => {
    setSelectedRecord(record)
    setUpdateData(JSON.stringify(record, null, 2))

    // Create a where condition based on the first column (usually id)
    const firstKey = Object.keys(record)[0]
    setWhereData(JSON.stringify({ [firstKey]: record[firstKey] }, null, 2))
  }

  return (
    <div className="container mx-auto">
      <div className="flex flex-col gap-6">
        <div className="flex flex-col gap-2">
          <h1 className="text-3xl font-bold tracking-tight">CRUD Operations</h1>
          <p className="text-muted-foreground">
            Database: <span className="font-medium">{dbName}</span>
          </p>
        </div>

        <Tabs defaultValue="create">
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="create">Create Record</TabsTrigger>
            <TabsTrigger value="read">Read Records</TabsTrigger>
            <TabsTrigger value="update">Update Record</TabsTrigger>
          </TabsList>

          <TabsContent value="create">
            <Card>
              <CardHeader>
                <CardTitle>Create Record</CardTitle>
                <CardDescription>Add a new record to your table</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-col gap-4">
                  <Input
                    placeholder="Table name"
                    value={createTableName}
                    onChange={(e) => setCreateTableName(e.target.value)}
                  />

                  <div className="flex flex-col gap-2">
                    <label className="text-sm font-medium">Record Data (JSON)</label>
                    <Textarea
                      placeholder={'Enter JSON data, e.g., {"name": "Alice", "email": "alice@example.com"}'}
                      value={recordData}
                      onChange={(e) => setRecordData(e.target.value)}
                      rows={6}
                    />
                  </div>
                </div>
              </CardContent>
              <CardFooter>
                <Button onClick={createRecord} disabled={loading} className="ml-auto">
                  {loading ? (
                    <>
                      <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                      Creating...
                    </>
                  ) : (
                    <>
                      <Plus className="mr-2 h-4 w-4" />
                      Create Record
                    </>
                  )}
                </Button>
              </CardFooter>
            </Card>
          </TabsContent>

          <TabsContent value="read">
            <Card>
              <CardHeader>
                <CardTitle>Read Records</CardTitle>
                <CardDescription>View records from your table</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex gap-4">
                  <Input
                    placeholder="Table name"
                    value={readTableName}
                    onChange={(e) => setReadTableName(e.target.value)}
                    className="flex-1"
                  />
                  <Button onClick={readRecords} disabled={loading}>
                    {loading ? (
                      <>
                        <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                        Loading...
                      </>
                    ) : (
                      <>
                        <Search className="mr-2 h-4 w-4" />
                        Read Records
                      </>
                    )}
                  </Button>
                </div>

                <div className="mt-6">
                  {records.length > 0 ? (
                    <div className="rounded-md border">
                      <Table>
                        <TableHeader>
                          <TableRow>
                            {Object.keys(records[0]).map((key) => (
                              <TableHead key={key}>{key}</TableHead>
                            ))}
                            <TableHead>Actions</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {records.map((record, index) => (
                            <TableRow key={index}>
                              {Object.values(record).map((value: any, i) => (
                                <TableCell key={i}>{String(value)}</TableCell>
                              ))}
                              <TableCell>
                                <div className="flex gap-2">
                                  <Button variant="outline" size="icon" onClick={() => prepareUpdate(record)}>
                                    <Edit className="h-4 w-4" />
                                  </Button>
                                  <Button variant="destructive" size="icon" onClick={() => deleteRecord(record)}>
                                    <Trash2 className="h-4 w-4" />
                                  </Button>
                                </div>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  ) : (
                    <Alert>
                      <FileText className="h-4 w-4" />
                      <AlertTitle>No records found</AlertTitle>
                      <AlertDescription>Enter a table name and click "Read Records" to view data.</AlertDescription>
                    </Alert>
                  )}
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="update">
            <Card>
              <CardHeader>
                <CardTitle>Update Record</CardTitle>
                <CardDescription>Modify an existing record</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-col gap-4">
                  <div className="flex flex-col gap-2">
                    <label className="text-sm font-medium">New Data (JSON)</label>
                    <Textarea
                      placeholder={'Enter JSON data to update, e.g., {"name": "New Name"}'}
                      value={updateData}
                      onChange={(e) => setUpdateData(e.target.value)}
                      rows={6}
                    />
                  </div>

                  <div className="flex flex-col gap-2">
                    <label className="text-sm font-medium">Where Condition (JSON)</label>
                    <Textarea
                      placeholder={'Enter JSON condition, e.g., {"id": 1}'}
                      value={whereData}
                      onChange={(e) => setWhereData(e.target.value)}
                      rows={3}
                    />
                  </div>
                </div>
              </CardContent>
              <CardFooter>
                <Button onClick={updateRecord} disabled={loading} className="ml-auto">
                  {loading ? (
                    <>
                      <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                      Updating...
                    </>
                  ) : (
                    <>
                      <Edit className="mr-2 h-4 w-4" />
                      Update Record
                    </>
                  )}
                </Button>
              </CardFooter>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  )
}
