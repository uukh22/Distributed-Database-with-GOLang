"use client"

import { useState, useEffect } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { useToast } from "@/components/ui/use-toast"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { RefreshCw, Server, CheckCircle2, XCircle, AlertTriangle, Network } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Separator } from "@/components/ui/separator"
import { Progress } from "@/components/ui/progress"

type Node = {
  id: string
  role: string
  url: string
  isHealthy: boolean
  lastSeen: string
}

export default function ClusterPage() {
  const { toast } = useToast()
  const [nodes, setNodes] = useState<Node[]>([])
  const [refreshing, setRefreshing] = useState(false)
  const [newNodeUrl, setNewNodeUrl] = useState("")
  const [addingNode, setAddingNode] = useState(false)
  const [currentRole, setCurrentRole] = useState<string>("unknown")
  const [masterUrl, setMasterUrl] = useState<string>("")
  const [clusterHealth, setClusterHealth] = useState<number>(0)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)

  // Fetch cluster nodes
  const fetchNodes = async () => {
    setRefreshing(true)
    try {
      const response = await fetch("/api/nodes", {
        cache: "no-store",
        headers: {
          "Cache-Control": "no-cache",
        },
      })

      if (response.ok) {
        const data = await response.json()

        if (data.success) {
          setNodes(data.result || [])

          // Calculate cluster health
          const totalNodes = data.result?.length || 0
          const healthyNodes = data.result?.filter((node: Node) => node.isHealthy).length || 0
          setClusterHealth(totalNodes > 0 ? (healthyNodes / totalNodes) * 100 : 0)
          setLastUpdated(new Date())
        } else {
          toast({
            title: "Error",
            description: data.message || "Failed to fetch nodes",
            variant: "destructive",
          })
        }
      } else {
        toast({
          title: "Error",
          description: `Server responded with status: ${response.status}`,
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

  // Check current node role
  const checkNodeRole = async () => {
    try {
      const response = await fetch("/api/node-role")
      const data = await response.json()

      if (data.success && data.result) {
        setCurrentRole(data.result.role)
        if (data.result.masterUrl) {
          setMasterUrl(data.result.masterUrl)
        }
      }
    } catch (error) {
      console.error("Failed to check node role:", error)
    }
  }

  useEffect(() => {
    fetchNodes()
    checkNodeRole()
    // Poll for node status every 10 seconds
    const interval = setInterval(fetchNodes, 10000)
    return () => clearInterval(interval)
  }, [])

  const addNode = async () => {
    if (!newNodeUrl) {
      toast({
        title: "Error",
        description: "Please enter a node URL",
        variant: "destructive",
      })
      return
    }

    setAddingNode(true)
    try {
      const response = await fetch("/api/add-node", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ nodeUrl: newNodeUrl }),
      })

      const data = await response.json()

      if (data.success) {
        setNewNodeUrl("")
        toast({
          title: "Success",
          description: "Node added successfully",
        })
        fetchNodes()
      } else {
        toast({
          title: "Error",
          description: data.message || "Failed to add node",
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
      setAddingNode(false)
    }
  }

  const formatDate = (dateString: string) => {
    if (!dateString) return "N/A"
    const date = new Date(dateString)
    return date.toLocaleString()
  }

  const getTimeSinceLastSeen = (lastSeenStr: string) => {
    if (!lastSeenStr) return "N/A"

    const lastSeen = new Date(lastSeenStr)
    const now = new Date()
    const diffMs = now.getTime() - lastSeen.getTime()
    const diffSec = Math.floor(diffMs / 1000)

    if (diffSec < 60) return `${diffSec}s ago`
    if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`
    return `${Math.floor(diffSec / 3600)}h ago`
  }

  const masterNode = nodes.find((node) => node.role === "master")
  const slaveNodes = nodes.filter((node) => node.role === "slave")

  return (
    <div className="container mx-auto">
      <div className="flex flex-col gap-6">
        <div className="flex flex-col gap-2">
          <h1 className="text-3xl font-bold tracking-tight">Cluster Management</h1>
          <p className="text-muted-foreground">Monitor and manage your database cluster nodes</p>
        </div>

        <div className="grid gap-6 md:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Network className="h-5 w-5" />
                Cluster Overview
              </CardTitle>
              <CardDescription>Current status of your database cluster</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex flex-col gap-4">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Badge
                      variant={clusterHealth > 80 ? "default" : clusterHealth > 50 ? "outline" : "destructive"}
                      className="px-3 py-1"
                    >
                      {clusterHealth > 80 ? (
                        <>
                          <CheckCircle2 className="mr-1 h-3 w-3" />
                          Healthy
                        </>
                      ) : clusterHealth > 50 ? (
                        <>
                          <AlertTriangle className="mr-1 h-3 w-3" />
                          Degraded
                        </>
                      ) : (
                        <>
                          <XCircle className="mr-1 h-3 w-3" />
                          Critical
                        </>
                      )}
                    </Badge>
                    <span className="text-sm text-muted-foreground">
                      {nodes.filter((n) => n.isHealthy).length} of {nodes.length} nodes online
                    </span>
                  </div>
                  {lastUpdated && (
                    <span className="text-xs text-muted-foreground">Updated {lastUpdated.toLocaleTimeString()}</span>
                  )}
                </div>

                <div className="space-y-2">
                  <div className="flex justify-between text-xs">
                    <span>Cluster Health</span>
                    <span>{clusterHealth.toFixed(0)}%</span>
                  </div>
                  <Progress
                    value={clusterHealth}
                    className="h-1"
                    indicatorClassName={
                      clusterHealth < 50 ? "bg-red-500" : clusterHealth < 80 ? "bg-yellow-500" : undefined
                    }
                  />
                </div>

                <div className="rounded-md bg-muted p-3">
                  <div className="flex flex-col gap-2">
                    <div className="flex items-center justify-between">
                      <span className="font-medium">Current Node Role:</span>
                      <Badge variant={currentRole === "master" ? "default" : "secondary"}>
                        {currentRole === "master" ? "Master" : currentRole === "slave" ? "Slave" : "Unknown"}
                      </Badge>
                    </div>
                    {currentRole === "slave" && masterUrl && (
                      <div className="flex items-center justify-between">
                        <span className="font-medium">Master URL:</span>
                        <span className="text-sm">{masterUrl}</span>
                      </div>
                    )}
                    <div className="flex items-center justify-between">
                      <span className="font-medium">Total Nodes:</span>
                      <span>{nodes.length}</span>
                    </div>
                  </div>
                </div>
              </div>
            </CardContent>
            <CardFooter>
              <Button variant="outline" size="sm" onClick={fetchNodes} disabled={refreshing} className="ml-auto">
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
            </CardFooter>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Add Node</CardTitle>
              <CardDescription>Add a new node to the cluster</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex flex-col gap-4">
                <div className="flex flex-col gap-2">
                  <Label htmlFor="nodeUrl">Node URL</Label>
                  <Input
                    id="nodeUrl"
                    placeholder="http://localhost:8081"
                    value={newNodeUrl}
                    onChange={(e) => setNewNodeUrl(e.target.value)}
                  />
                  <p className="text-xs text-muted-foreground">
                    Enter the URL of the node you want to add to the cluster
                  </p>
                </div>
              </div>
            </CardContent>
            <CardFooter>
              <Button onClick={addNode} disabled={addingNode || !newNodeUrl} className="ml-auto">
                {addingNode ? (
                  <>
                    <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                    Adding...
                  </>
                ) : (
                  "Add Node"
                )}
              </Button>
            </CardFooter>
          </Card>
        </div>

        <Separator />

        <Tabs defaultValue="all">
          <TabsList>
            <TabsTrigger value="all">All Nodes</TabsTrigger>
            <TabsTrigger value="master">Master Node</TabsTrigger>
            <TabsTrigger value="slaves">Slave Nodes</TabsTrigger>
          </TabsList>

          <TabsContent value="all">
            <Card>
              <CardHeader>
                <CardTitle>All Cluster Nodes</CardTitle>
                <CardDescription>View all nodes in the database cluster</CardDescription>
              </CardHeader>
              <CardContent>
                {nodes.length === 0 ? (
                  <Alert>
                    <Server className="h-4 w-4" />
                    <AlertTitle>No nodes found</AlertTitle>
                    <AlertDescription>No database nodes are currently registered in the cluster.</AlertDescription>
                  </Alert>
                ) : (
                  <div className="rounded-md border">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Node ID</TableHead>
                          <TableHead>Role</TableHead>
                          <TableHead>URL</TableHead>
                          <TableHead>Status</TableHead>
                          <TableHead>Last Seen</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {nodes.map((node) => (
                          <TableRow key={node.id}>
                            <TableCell>{node.id}</TableCell>
                            <TableCell>
                              <Badge variant={node.role === "master" ? "default" : "secondary"}>
                                {node.role === "master" ? "Master" : "Slave"}
                              </Badge>
                            </TableCell>
                            <TableCell>{node.url}</TableCell>
                            <TableCell>
                              {node.isHealthy ? (
                                <div className="flex items-center">
                                  <CheckCircle2 className="mr-2 h-4 w-4 text-green-500" />
                                  <span>Healthy</span>
                                </div>
                              ) : (
                                <div className="flex items-center">
                                  <XCircle className="mr-2 h-4 w-4 text-red-500" />
                                  <span>Unhealthy</span>
                                </div>
                              )}
                            </TableCell>
                            <TableCell title={formatDate(node.lastSeen)}>
                              {getTimeSinceLastSeen(node.lastSeen)}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="master">
            <Card>
              <CardHeader>
                <CardTitle>Master Node</CardTitle>
                <CardDescription>Details of the current master node</CardDescription>
              </CardHeader>
              <CardContent>
                {!masterNode ? (
                  <Alert>
                    <AlertTriangle className="h-4 w-4" />
                    <AlertTitle>No master node found</AlertTitle>
                    <AlertDescription>There is currently no master node in the cluster.</AlertDescription>
                  </Alert>
                ) : (
                  <div className="space-y-4">
                    <div className="rounded-md border p-4">
                      <div className="flex flex-col gap-3">
                        <div className="flex items-center justify-between">
                          <span className="font-medium">Node ID:</span>
                          <span>{masterNode.id}</span>
                        </div>
                        <div className="flex items-center justify-between">
                          <span className="font-medium">URL:</span>
                          <span>{masterNode.url}</span>
                        </div>
                        <div className="flex items-center justify-between">
                          <span className="font-medium">Status:</span>
                          {masterNode.isHealthy ? (
                            <div className="flex items-center">
                              <CheckCircle2 className="mr-2 h-4 w-4 text-green-500" />
                              <span>Healthy</span>
                            </div>
                          ) : (
                            <div className="flex items-center">
                              <XCircle className="mr-2 h-4 w-4 text-red-500" />
                              <span>Unhealthy</span>
                            </div>
                          )}
                        </div>
                        <div className="flex items-center justify-between">
                          <span className="font-medium">Last Seen:</span>
                          <span title={formatDate(masterNode.lastSeen)}>
                            {getTimeSinceLastSeen(masterNode.lastSeen)}
                          </span>
                        </div>
                      </div>
                    </div>

                    {!masterNode.isHealthy && (
                      <Alert variant="destructive">
                        <AlertTriangle className="h-4 w-4" />
                        <AlertTitle>Master Node is Unhealthy</AlertTitle>
                        <AlertDescription>
                          The master node is currently unreachable. A new master election may be in progress.
                        </AlertDescription>
                      </Alert>
                    )}
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="slaves">
            <Card>
              <CardHeader>
                <CardTitle>Slave Nodes</CardTitle>
                <CardDescription>Details of all slave nodes in the cluster</CardDescription>
              </CardHeader>
              <CardContent>
                {slaveNodes.length === 0 ? (
                  <Alert>
                    <Server className="h-4 w-4" />
                    <AlertTitle>No slave nodes found</AlertTitle>
                    <AlertDescription>There are currently no slave nodes in the cluster.</AlertDescription>
                  </Alert>
                ) : (
                  <div className="rounded-md border">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Node ID</TableHead>
                          <TableHead>URL</TableHead>
                          <TableHead>Status</TableHead>
                          <TableHead>Last Seen</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {slaveNodes.map((node) => (
                          <TableRow key={node.id}>
                            <TableCell>{node.id}</TableCell>
                            <TableCell>{node.url}</TableCell>
                            <TableCell>
                              {node.isHealthy ? (
                                <div className="flex items-center">
                                  <CheckCircle2 className="mr-2 h-4 w-4 text-green-500" />
                                  <span>Healthy</span>
                                </div>
                              ) : (
                                <div className="flex items-center">
                                  <XCircle className="mr-2 h-4 w-4 text-red-500" />
                                  <span>Unhealthy</span>
                                </div>
                              )}
                            </TableCell>
                            <TableCell title={formatDate(node.lastSeen)}>
                              {getTimeSinceLastSeen(node.lastSeen)}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  )
}
