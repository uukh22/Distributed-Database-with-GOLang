"use client"

import { useState, useEffect } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { useToast } from "@/components/ui/use-toast"
import { Database, Trash2, RefreshCw, AlertTriangle } from "lucide-react"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Separator } from "@/components/ui/separator"
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

export default function DatabasesPage() {
  const { toast } = useToast()
  const [dbName, setDbName] = useState("")
  const [databases, setDatabases] = useState<string[]>([])
  const [loading, setLoading] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [isMaster, setIsMaster] = useState(true)
  const [masterUrl, setMasterUrl] = useState("")
  const [dbToDelete, setDbToDelete] = useState<string | null>(null)
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false)
  const [deleteLoading, setDeleteLoading] = useState(false)

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

  // Fetch existing databases
  const fetchDatabases = async () => {
    setRefreshing(true)
    try {
      const response = await fetch("/api/list-databases")
      const data = await response.json()

      if (data.success) {
        setDatabases(data.result || [])
      } else {
        toast({
          title: "Error",
          description: data.message || "Failed to fetch databases",
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
    fetchDatabases()
  }, [])

  const createDatabase = async () => {
    if (!dbName.trim()) {
      toast({
        title: "Error",
        description: "Please enter a database name",
        variant: "destructive",
      })
      return
    }

    setLoading(true)

    try {
      const response = await fetch("/api/create-db", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ dbName }),
      })

      const data = await response.json()

      if (data.success) {
        setDatabases([...databases, dbName])
        setDbName("")
        toast({
          title: "Success",
          description: data.message || `Database "${dbName}" created successfully`,
        })
      } else {
        toast({
          title: "Error",
          description: data.message || "Failed to create database",
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

  const deleteDatabase = async () => {
    if (!dbToDelete) return

    setDeleteLoading(true)
    try {
      const response = await fetch("/api/drop-db", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ dbName: dbToDelete }),
      })

      const data = await response.json()

      if (data.success) {
        setDatabases(databases.filter((db) => db !== dbToDelete))
        toast({
          title: "Success",
          description: `Database "${dbToDelete}" deleted successfully`,
        })
      } else {
        toast({
          title: "Error",
          description: data.message || "Failed to delete database",
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
      setDbToDelete(null)
    }
  }

  const confirmDelete = (name: string) => {
    setDbToDelete(name)
    setDeleteDialogOpen(true)
  }

  return (
    <div className="container mx-auto">
      <div className="flex flex-col gap-6">
        <div className="flex flex-col gap-2">
          <h1 className="text-3xl font-bold tracking-tight">Database Management</h1>
          <p className="text-muted-foreground">Create and manage your databases</p>
        </div>

        {!isMaster && (
          <Alert variant="warning">
            <AlertTriangle className="h-4 w-4" />
            <AlertTitle>Read-Only Mode</AlertTitle>
            <AlertDescription>
              This node is a slave. Database creation and deletion operations must be performed on the master node.
              {masterUrl && (
                <div className="mt-2">
                  <a href={masterUrl} className="text-blue-500 hover:underline">
                    Go to Master Node
                  </a>
                </div>
              )}
            </AlertDescription>
          </Alert>
        )}

        <Card>
          <CardHeader>
            <CardTitle>Create Database</CardTitle>
            <CardDescription>Enter a name for your new database</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex gap-4">
              <Input
                placeholder="Database name"
                value={dbName}
                onChange={(e) => setDbName(e.target.value)}
                disabled={!isMaster}
              />
              <Button onClick={createDatabase} disabled={loading || !isMaster}>
                {loading ? "Creating..." : "Create Database"}
              </Button>
            </div>
          </CardContent>
        </Card>

        <Separator className="my-4" />

        <div className="flex flex-col gap-4">
          <div className="flex items-center justify-between">
            <h2 className="text-xl font-semibold">Your Databases</h2>
            <Button variant="outline" size="sm" onClick={fetchDatabases} disabled={refreshing}>
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

          {databases.length === 0 ? (
            <Alert>
              <Database className="h-4 w-4" />
              <AlertTitle>No databases found</AlertTitle>
              <AlertDescription>Create your first database to get started.</AlertDescription>
            </Alert>
          ) : (
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
              {databases.map((db) => (
                <Card key={db}>
                  <CardHeader className="pb-2">
                    <CardTitle className="flex items-center gap-2 text-lg">
                      <Database className="h-4 w-4" />
                      {db}
                    </CardTitle>
                  </CardHeader>
                  <CardFooter className="flex justify-between pt-4">
                    <Button variant="outline" onClick={() => (window.location.href = `/tables?db=${db}`)}>
                      Manage Tables
                    </Button>
                    {isMaster && (
                      <Button variant="destructive" size="icon" onClick={() => confirmDelete(db)}>
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
              This action will permanently delete the database "{dbToDelete}" and all its tables and data. This action
              cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={deleteDatabase}
              disabled={deleteLoading}
              className="bg-red-600 hover:bg-red-700"
            >
              {deleteLoading ? "Deleting..." : "Delete Database"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
