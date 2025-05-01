"use client"

import { useState, useEffect } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { useToast } from "@/components/ui/use-toast"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { RefreshCw, Server, CheckCircle2, XCircle } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Switch } from "@/components/ui/switch"
import { Label } from "@/components/ui/label"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"

type Node = {
  id: string
  role: string
  url: string
  isHealthy: boolean
  lastSeen: string
}

export default function ReplicationPage() {
  const { toast } = useToast()
  const [masterHost, setMasterHost] = useState("")
  const [masterPort, setMasterPort] = useState("3306")
  const [replicaHost, setReplicaHost] = useState("")
  const [replicaPort, setReplicaPort] = useState("3306")
  const [loading, setLoading] = useState(false)
  const [replicationActive, setReplicationActive] = useState(false)
  const [replicationStatus, setReplicationStatus] = useState<null | {
    status: "running" | "stopped" | "error"
    lastSync: string
    lag: string
  }>(null)
  const [nodes, setNodes] = useState<Node[]>([])
  const [refreshingNodes, setRefreshingNodes] = useState(false)

  // Fetch cluster nodes
  const fetchNodes = async () => {
    setRefreshingNodes(true)
    try {
      const response = await fetch("/api/nodes")
      const data = await response.json()

      if (data.success) {
        setNodes(data.result || [])
      } else {
        toast({
          title: "Error",
          description: data.message || "Failed to fetch nodes",
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
      setRefreshingNodes(false)
    }
  }

  useEffect(() => {
    fetchNodes()
    // Poll for node status every 5 seconds
    const interval = setInterval(fetchNodes, 5000)
    return () => clearInterval(interval)
  }, [])

  const setupReplication = async () => {
    if (!masterHost || !masterPort || !replicaHost || !replicaPort) {
      toast({
        title: "Error",
        description: "Please fill in all fields",
        variant: "destructive",
      })
      return
    }

    setLoading(true)

    try {
      const response = await fetch("/api/setup-replication", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          masterHost,
          masterPort: Number.parseInt(masterPort),
        }),
      })

      const data = await response.json()

      if (data.success) {
        setReplicationActive(true)
        setReplicationStatus({
          status: "running",
          lastSync: new Date().toISOString(),
          lag: "0 seconds",
        })
        toast({
          title: "Success",
          description: data.message || "Replication setup successfully",
        })

        // Refresh nodes list
        fetchNodes()
      } else {
        toast({
          title: "Error",
          description: data.message || "Failed to setup replication",
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

  const toggleReplication = () => {
    if (replicationActive) {
      // Stop replication
      setReplicationStatus({
        ...replicationStatus!,
        status: "stopped",
      })
      setReplicationActive(false)

      toast({
        title: "Replication Stopped",
        description: "Database replication has been stopped",
      })
    } else {
      // Start replication
      setReplicationStatus({
        ...replicationStatus!,
        status: "running",
        lastSync: new Date().toISOString(),
      })
      setReplicationActive(true)

      toast({
        title: "Replication Started",
        description: "Database replication has been started",
      })
    }
  }

  return (
    <div className="container mx-auto">
      <div className="flex flex-col gap-6">
        <div className="flex flex-col gap-2">
          <h1 className="text-3xl font-bold tracking-tight">Replication Setup</h1>
          <p className="text-muted-foreground">Configure master-slave replication for high availability</p>
        </div>

        <Tabs defaultValue="setup">
          <TabsList className="grid w-full grid-cols-2">
            <TabsTrigger value="setup">Replication Setup</TabsTrigger>
            <TabsTrigger value="nodes">Cluster Nodes</TabsTrigger>
          </TabsList>

          <TabsContent value="setup">
            <div className="grid gap-6 md:grid-cols-2">
              <Card>
                <CardHeader>
                  <CardTitle>Master Database</CardTitle>
                  <CardDescription>Configure the source database server</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="flex flex-col gap-4">
                    <div className="flex flex-col gap-2">
                      <Label htmlFor="masterHost">Host IP Address</Label>
                      <Input
                        id="masterHost"
                        placeholder="Master IP (e.g., 192.168.1.100)"
                        value={masterHost}
                        onChange={(e) => setMasterHost(e.target.value)}
                      />
                    </div>
                    <div className="flex flex-col gap-2">
                      <Label htmlFor="masterPort">Port</Label>
                      <Input
                        id="masterPort"
                        placeholder="Master Port (default: 3306)"
                        value={masterPort}
                        onChange={(e) => setMasterPort(e.target.value)}
                        type="number"
                      />
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Replica Database</CardTitle>
                  <CardDescription>Configure the target database server</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="flex flex-col gap-4">
                    <div className="flex flex-col gap-2">
                      <Label htmlFor="replicaHost">Host IP Address</Label>
                      <Input
                        id="replicaHost"
                        placeholder="Replica IP (e.g., 192.168.1.101)"
                        value={replicaHost}
                        onChange={(e) => setReplicaHost(e.target.value)}
                      />
                    </div>
                    <div className="flex flex-col gap-2">
                      <Label htmlFor="replicaPort">Port</Label>
                      <Input
                        id="replicaPort"
                        placeholder="Replica Port (default: 3306)"
                        value={replicaPort}
                        onChange={(e) => setReplicaPort(e.target.value)}
                        type="number"
                      />
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>

            <Card className="mt-6">
              <CardHeader>
                <CardTitle>Replication Control</CardTitle>
                <CardDescription>Start or stop the replication process</CardDescription>
              </CardHeader>
              <CardContent>
                {replicationStatus ? (
                  <div className="flex flex-col gap-6">
                    <div className="flex items-center justify-between">
                      <div className="flex flex-col gap-1">
                        <h3 className="text-sm font-medium">Replication Status</h3>
                        <div className="flex items-center gap-2">
                          <Badge variant={replicationStatus.status === "running" ? "default" : "destructive"}>
                            {replicationStatus.status === "running" ? "Running" : "Stopped"}
                          </Badge>
                          {replicationStatus.status === "running" && (
                            <span className="text-xs text-muted-foreground">Lag: {replicationStatus.lag}</span>
                          )}
                        </div>
                      </div>
                      <div className="flex items-center space-x-2">
                        <Switch
                          id="replication-toggle"
                          checked={replicationActive}
                          onCheckedChange={toggleReplication}
                        />
                        <Label htmlFor="replication-toggle">{replicationActive ? "Active" : "Inactive"}</Label>
                      </div>
                    </div>

                    <div className="rounded-md bg-muted p-4">
                      <div className="flex flex-col gap-2 text-sm">
                        <div className="flex justify-between">
                          <span className="font-medium">Master:</span>
                          <span>
                            {masterHost}:{masterPort}
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="font-medium">Replica:</span>
                          <span>
                            {replicaHost}:{replicaPort}
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="font-medium">Last Synchronized:</span>
                          <span>{new Date(replicationStatus.lastSync).toLocaleString()}</span>
                        </div>
                      </div>
                    </div>
                  </div>
                ) : (
                  <Alert>
                    <Server className="h-4 w-4" />
                    <AlertTitle>No replication configured</AlertTitle>
                    <AlertDescription>Configure and start replication to see status information.</AlertDescription>
                  </Alert>
                )}
              </CardContent>
              <CardFooter>
                {!replicationStatus && (
                  <Button onClick={setupReplication} disabled={loading} className="ml-auto">
                    {loading ? (
                      <>
                        <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                        Setting up...
                      </>
                    ) : (
                      "Setup Replication"
                    )}
                  </Button>
                )}
              </CardFooter>
            </Card>
          </TabsContent>

          <TabsContent value="nodes">
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle>Cluster Nodes</CardTitle>
                    <CardDescription>View all nodes in the database cluster</CardDescription>
                  </div>
                  <Button variant="outline" size="sm" onClick={fetchNodes} disabled={refreshingNodes}>
                    {refreshingNodes ? (
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
                            <TableCell>{node.lastSeen ? new Date(node.lastSeen).toLocaleString() : "N/A"}</TableCell>
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
