"use client"

import { useState, useEffect } from "react"
import { Network, Server, CheckCircle2, XCircle, RefreshCw, Database } from "lucide-react"

import { apiService, type Node } from "@/lib/api-service"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { toast } from "@/components/ui/use-toast"
import { Badge } from "@/components/ui/badge"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
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

export default function ClusterPage() {
  const [nodes, setNodes] = useState<Node[]>([])
  const [loading, setLoading] = useState(true)
  const [nodeRole, setNodeRole] = useState<string | null>(null)
  const [lastUpdated, setLastUpdated] = useState<Date>(new Date())

  const fetchNodes = async () => {
    setLoading(true)
    try {
      const roleResponse = await apiService.getNodeRole()
      if (roleResponse.success && roleResponse.result) {
        setNodeRole(roleResponse.result.role)
      }

      const response = await apiService.getNodes()
      if (response.success && response.result) {
        setNodes(response.result)
      } else {
        toast({
          variant: "destructive",
          title: "Error",
          description: response.message || "Failed to fetch nodes",
        })
      }
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "An error occurred while fetching nodes",
      })
    } finally {
      setLoading(false)
      setLastUpdated(new Date())
    }
  }

  useEffect(() => {
    fetchNodes()

    // Set up interval to refresh nodes every 10 seconds
    const interval = setInterval(fetchNodes, 10000)
    return () => clearInterval(interval)
  }, [])

  const handleShutdownSlave = async (slaveURL: string) => {
    try {
      const response = await apiService.shutdownSlave(slaveURL)
      if (response.success) {
        toast({
          title: "Success",
          description: response.message || "Slave shutdown command sent successfully",
        })
        fetchNodes()
      } else {
        toast({
          variant: "destructive",
          title: "Error",
          description: response.message || "Failed to shutdown slave",
        })
      }
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "An error occurred while shutting down the slave",
      })
    }
  }

  const formatDate = (dateString: string) => {
    const date = new Date(dateString)
    return date.toLocaleString()
  }

  const getTimeSince = (dateString: string) => {
    const date = new Date(dateString)
    const now = new Date()
    const diffMs = now.getTime() - date.getTime()

    const seconds = Math.floor(diffMs / 1000)
    if (seconds < 60) return `${seconds} seconds ago`

    const minutes = Math.floor(seconds / 60)
    if (minutes < 60) return `${minutes} minutes ago`

    const hours = Math.floor(minutes / 60)
    if (hours < 24) return `${hours} hours ago`

    const days = Math.floor(hours / 24)
    return `${days} days ago`
  }

  return (
    <div className="container mx-auto py-6">
      <div className="flex flex-col gap-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">Cluster Management</h1>
            <p className="text-muted-foreground">Monitor and manage nodes in your distributed database cluster</p>
          </div>

          <Button onClick={fetchNodes}>
            <RefreshCw className="mr-2 h-4 w-4" />
            Refresh
          </Button>
        </div>

        <div className="grid gap-6 md:grid-cols-3">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-xl">Cluster Overview</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 gap-4">
                <div className="flex flex-col items-center justify-center rounded-md bg-muted p-3">
                  <span className="text-2xl font-bold">{nodes.length}</span>
                  <span className="text-xs text-muted-foreground">Total Nodes</span>
                </div>
                <div className="flex flex-col items-center justify-center rounded-md bg-muted p-3">
                  <span className="text-2xl font-bold">{nodes.filter((node) => node.isHealthy).length}</span>
                  <span className="text-xs text-muted-foreground">Healthy Nodes</span>
                </div>
                <div className="flex flex-col items-center justify-center rounded-md bg-muted p-3">
                  <span className="text-2xl font-bold">{nodes.filter((node) => node.role === "master").length}</span>
                  <span className="text-xs text-muted-foreground">Master Nodes</span>
                </div>
                <div className="flex flex-col items-center justify-center rounded-md bg-muted p-3">
                  <span className="text-2xl font-bold">{nodes.filter((node) => node.role === "slave").length}</span>
                  <span className="text-xs text-muted-foreground">Slave Nodes</span>
                </div>
              </div>

              <div className="mt-4 text-xs text-muted-foreground text-right">
                Last updated: {lastUpdated.toLocaleTimeString()}
              </div>
            </CardContent>
          </Card>

          <Card className="md:col-span-2">
            <CardHeader className="pb-2">
              <CardTitle className="text-xl">Node Status</CardTitle>
              <CardDescription>Current status of all nodes in the cluster</CardDescription>
            </CardHeader>
            <CardContent>
              {loading ? (
                <div className="flex items-center justify-center py-12">
                  <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent"></div>
                </div>
              ) : nodes.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-12">
                  <Network className="h-12 w-12 text-muted-foreground" />
                  <h3 className="mt-4 text-xl font-medium">No Nodes Found</h3>
                  <p className="mt-2 text-center text-muted-foreground">
                    There are no nodes registered in the cluster.
                  </p>
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Node URL</TableHead>
                        <TableHead>Role</TableHead>
                        <TableHead>Shard ID</TableHead>
                        <TableHead>Status</TableHead>
                        <TableHead>Last Seen</TableHead>
                        {nodeRole === "master" && <TableHead className="text-right">Actions</TableHead>}
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {nodes.map((node) => (
                        <TableRow key={node.id}>
                          <TableCell className="font-medium">{node.url}</TableCell>
                          <TableCell>
                            <Badge variant={node.role === "master" ? "default" : "secondary"}>{node.role}</Badge>
                          </TableCell>
                          <TableCell>{node.shardId}</TableCell>
                          <TableCell>
                            <div className="flex items-center gap-2">
                              {node.isHealthy ? (
                                <>
                                  <CheckCircle2 className="h-4 w-4 text-green-500" />
                                  <span>Healthy</span>
                                </>
                              ) : (
                                <>
                                  <XCircle className="h-4 w-4 text-destructive" />
                                  <span>Unhealthy</span>
                                </>
                              )}
                            </div>
                          </TableCell>
                          <TableCell>
                            <span title={formatDate(node.lastSeen)}>{getTimeSince(node.lastSeen)}</span>
                          </TableCell>
                          {nodeRole === "master" && (
                            <TableCell className="text-right">
                              {node.role === "slave" && (
                                <AlertDialog>
                                  <AlertDialogTrigger asChild>
                                    <Button variant="destructive" size="sm">
                                      Shutdown
                                    </Button>
                                  </AlertDialogTrigger>
                                  <AlertDialogContent>
                                    <AlertDialogHeader>
                                      <AlertDialogTitle>Are you sure?</AlertDialogTitle>
                                      <AlertDialogDescription>
                                        This will shut down the slave node at {node.url}. The node will be unavailable
                                        until it is manually restarted.
                                      </AlertDialogDescription>
                                    </AlertDialogHeader>
                                    <AlertDialogFooter>
                                      <AlertDialogCancel>Cancel</AlertDialogCancel>
                                      <AlertDialogAction
                                        onClick={() => handleShutdownSlave(node.url)}
                                        className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                                      >
                                        Shutdown
                                      </AlertDialogAction>
                                    </AlertDialogFooter>
                                  </AlertDialogContent>
                                </AlertDialog>
                              )}
                            </TableCell>
                          )}
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Shards Distribution</CardTitle>
            <CardDescription>Distribution of nodes across different shards</CardDescription>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="flex items-center justify-center py-12">
                <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent"></div>
              </div>
            ) : (
              <div className="grid gap-6 md:grid-cols-3">
                {Array.from(new Set(nodes.map((node) => node.shardId)))
                  .sort((a, b) => a - b) // Sort numerically
                  .map((shardId) => {
                    const shardNodes = nodes.filter((node) => node.shardId === shardId)
                    const hasMaster = shardNodes.some((node) => node.role === "master")
                    const healthyNodes = shardNodes.filter((node) => node.isHealthy).length

                    return (
                      <Card key={shardId} className={hasMaster ? "border-primary" : ""}>
                        <CardHeader className="pb-2">
                          <CardTitle className="text-lg flex items-center justify-between">
                            <span>Shard {shardId}</span>
                            {hasMaster && <Database className="h-4 w-4 text-primary" />}
                          </CardTitle>
                          <CardDescription>
                            {shardNodes.length} nodes • {healthyNodes} healthy
                          </CardDescription>
                        </CardHeader>
                        <CardContent>
                          <div className="space-y-2">
                            {shardNodes.map((node) => (
                              <div
                                key={node.id}
                                className="flex items-center justify-between rounded-md border p-2 text-sm"
                              >
                                <div className="flex items-center gap-2">
                                  <Server className="h-4 w-4" />
                                  <span className="font-medium truncate max-w-[150px]">{node.url}</span>
                                </div>
                                <div className="flex items-center gap-2">
                                  <Badge variant={node.role === "master" ? "default" : "secondary"}>{node.role}</Badge>
                                  {node.isHealthy ? (
                                    <CheckCircle2 className="h-4 w-4 text-green-500" />
                                  ) : (
                                    <XCircle className="h-4 w-4 text-destructive" />
                                  )}
                                </div>
                              </div>
                            ))}
                          </div>
                        </CardContent>
                      </Card>
                    )
                  })}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
