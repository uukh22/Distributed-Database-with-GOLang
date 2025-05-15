"use client"

import { useState, useEffect } from "react"
import { Database, Plus, Trash2 } from "lucide-react"

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
import { Header } from "@/components/header"

export default function DatabasesPage() {
  const [databases, setDatabases] = useState<string[]>([])
  const [loading, setLoading] = useState(true)
  const [newDbName, setNewDbName] = useState("")
  const [createDialogOpen, setCreateDialogOpen] = useState(false)
  const [nodeRole, setNodeRole] = useState<string | null>(null)
  const [masterUrl, setMasterUrl] = useState<string | null>(null)

  const fetchDatabases = async () => {
    setLoading(true)
    try {
      const roleResponse = await apiService.getNodeRole()
      if (roleResponse.success && roleResponse.result) {
        setNodeRole(roleResponse.result.role)
        setMasterUrl(roleResponse.result.masterUrl || null)
      }

      const response = await apiService.getDatabases()
      if (response.success && response.result) {
        setDatabases(response.result)
      } else {
        toast({
          variant: "destructive",
          title: "Error",
          description: response.message || "Failed to fetch databases",
        })
      }
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "An error occurred while fetching databases",
      })
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchDatabases()
  }, [])

  const handleCreateDatabase = async () => {
    if (!newDbName.trim()) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "Database name cannot be empty",
      })
      return
    }

    try {
      const response = await apiService.createDatabase(newDbName)
      if (response.success) {
        toast({
          title: "Success",
          description: response.message || "Database created successfully",
        })
        setNewDbName("")
        setCreateDialogOpen(false)
        fetchDatabases()
      } else {
        toast({
          variant: "destructive",
          title: "Error",
          description: response.message || "Failed to create database",
        })
      }
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "An error occurred while creating the database",
      })
    }
  }

  const handleDeleteDatabase = async (dbName: string) => {
    try {
      const response = await apiService.dropDatabase(dbName)
      if (response.success) {
        toast({
          title: "Success",
          description: response.message || "Database deleted successfully",
        })
        fetchDatabases()
      } else {
        toast({
          variant: "destructive",
          title: "Error",
          description: response.message || "Failed to delete database",
        })
      }
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "An error occurred while deleting the database",
      })
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <Header />
      <div className="container mx-auto">
        <div className="flex flex-col gap-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold tracking-tight">Databases</h1>
              <p className="text-muted-foreground">Create and manage your databases</p>
            </div>

            {nodeRole === "slave" ? (
              <div className="flex items-center gap-2">
                <span className="text-sm text-muted-foreground">
                  This is a slave node. Write operations must be performed on the master.
                </span>
                {masterUrl && (
                  <Button variant="outline" size="sm" asChild>
                    <a href={masterUrl} target="_blank" rel="noopener noreferrer">
                      Go to Master
                    </a>
                  </Button>
                )}
              </div>
            ) : (
              <Dialog open={createDialogOpen} onOpenChange={setCreateDialogOpen}>
                <DialogTrigger asChild>
                  <Button>
                    <Plus className="mr-2 h-4 w-4" />
                    Create Database
                  </Button>
                </DialogTrigger>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Create New Database</DialogTitle>
                    <DialogDescription>Enter a name for your new database.</DialogDescription>
                  </DialogHeader>
                  <div className="grid gap-4 py-4">
                    <div className="grid gap-2">
                      <Label htmlFor="name">Database Name</Label>
                      <Input
                        id="name"
                        value={newDbName}
                        onChange={(e) => setNewDbName(e.target.value)}
                        placeholder="Enter database name"
                      />
                    </div>
                  </div>
                  <DialogFooter>
                    <Button variant="outline" onClick={() => setCreateDialogOpen(false)}>
                      Cancel
                    </Button>
                    <Button onClick={handleCreateDatabase}>Create Database</Button>
                  </DialogFooter>
                </DialogContent>
              </Dialog>
            )}
          </div>

          {loading ? (
            <div className="flex items-center justify-center py-12">
              <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent"></div>
            </div>
          ) : databases.length === 0 ? (
            <Card>
              <CardContent className="flex flex-col items-center justify-center py-12">
                <Database className="h-12 w-12 text-muted-foreground" />
                <h3 className="mt-4 text-xl font-medium">No Databases Found</h3>
                <p className="mt-2 text-center text-muted-foreground">
                  You haven't created any databases yet. Click the "Create Database" button to get started.
                </p>
              </CardContent>
            </Card>
          ) : (
            <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
              {databases.map((dbName) => (
                <Card key={dbName}>
                  <CardHeader className="pb-2">
                    <CardTitle className="flex items-center gap-2">
                      <Database className="h-5 w-5" />
                      <span>{dbName}</span>
                    </CardTitle>
                    <CardDescription>Database</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="flex justify-between">
                      <Button variant="outline" asChild>
                        <a href={`/tables?db=${dbName}`}>View Tables</a>
                      </Button>

                      {nodeRole !== "slave" && (
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
                                This will permanently delete the database "{dbName}" and all its tables. This action
                                cannot be undone.
                              </AlertDialogDescription>
                            </AlertDialogHeader>
                            <AlertDialogFooter>
                              <AlertDialogCancel>Cancel</AlertDialogCancel>
                              <AlertDialogAction
                                onClick={() => handleDeleteDatabase(dbName)}
                                className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                              >
                                Delete
                              </AlertDialogAction>
                            </AlertDialogFooter>
                          </AlertDialogContent>
                        </AlertDialog>
                      )}
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
