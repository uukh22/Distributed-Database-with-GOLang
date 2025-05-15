"use client"

import { useState, useEffect } from "react"
import { Activity, RefreshCw } from "lucide-react"

import { apiService } from "@/lib/api-service"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { toast } from "@/components/ui/use-toast"
import { Badge } from "@/components/ui/badge"

export default function ReplicationPage() {
  const [nodeRole, setNodeRole] = useState<string | null>(null)
  const [masterUrl, setMasterUrl] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  // Replication setup form
  const [masterHost, setMasterHost] = useState("")
  const [masterPort, setMasterPort] = useState("3306")
  const [setupLoading, setSetupLoading] = useState(false)

  const fetchNodeInfo = async () => {
    setLoading(true)
    try {
      const response = await apiService.getNodeRole()
      if (response.success && response.result) {
        setNodeRole(response.result.role)
        setMasterUrl(response.result.masterUrl || null)
      } else {
        toast({
          variant: "destructive",
          title: "Error",
          description: response.message || "Failed to fetch node information",
        })
      }
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "An error occurred while fetching node information",
      })
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchNodeInfo()
  }, [])

  const handleSetupReplication = async () => {
    if (!masterHost || !masterPort) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "Master host and port are required",
      })
      return
    }

    setSetupLoading(true)
    try {
      const response = await apiService.setupReplication(masterHost, Number.parseInt(masterPort))
      if (response.success) {
        toast({
          title: "Success",
          description: response.message || "Replication setup successfully",
        })
        fetchNodeInfo()
      } else {
        toast({
          variant: "destructive",
          title: "Error",
          description: response.message || "Failed to setup replication",
        })
      }
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "An error occurred while setting up replication",
      })
    } finally {
      setSetupLoading(false)
    }
  }

  return (
    <div className="container mx-auto py-6">
      <div className="flex flex-col gap-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">Replication</h1>
            <p className="text-muted-foreground">Configure and manage database replication</p>
          </div>

          <Button onClick={fetchNodeInfo}>
            <RefreshCw className="mr-2 h-4 w-4" />
            Refresh
          </Button>
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-12">
            <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent"></div>
          </div>
        ) : (
          <>
            <Card>
              <CardHeader>
                <CardTitle>Node Status</CardTitle>
                <CardDescription>Current replication status of this node</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex items-center gap-4">
                  <div className="flex h-16 w-16 items-center justify-center rounded-full bg-muted">
                    <Activity className="h-8 w-8 text-primary" />
                  </div>
                  <div>
                    <h3 className="text-lg font-medium">
                      This node is a <Badge variant={nodeRole === "master" ? "default" : "secondary"}>{nodeRole}</Badge>
                    </h3>
                    {nodeRole === "slave" && masterUrl && (
                      <p className="text-muted-foreground">Connected to master: {masterUrl}</p>
                    )}
                  </div>
                </div>
              </CardContent>
            </Card>

            {nodeRole === "slave" ? (
              <Card>
                <CardHeader>
                  <CardTitle>Replication Configuration</CardTitle>
                  <CardDescription>Configure replication settings for this slave node</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="grid gap-6 md:grid-cols-2">
                    <div className="grid gap-2">
                      <Label htmlFor="masterHost">Master Host</Label>
                      <Input
                        id="masterHost"
                        value={masterHost}
                        onChange={(e) => setMasterHost(e.target.value)}
                        placeholder="Enter master host (e.g., 192.168.1.100)"
                      />
                    </div>
                    <div className="grid gap-2">
                      <Label htmlFor="masterPort">Master Port</Label>
                      <Input
                        id="masterPort"
                        value={masterPort}
                        onChange={(e) => setMasterPort(e.target.value)}
                        placeholder="Enter master port (default: 3306)"
                        type="number"
                      />
                    </div>
                  </div>

                  <Button className="mt-6" onClick={handleSetupReplication} disabled={setupLoading}>
                    {setupLoading && (
                      <div className="mr-2 h-4 w-4 animate-spin rounded-full border-2 border-background border-t-transparent"></div>
                    )}
                    Setup Replication
                  </Button>
                </CardContent>
              </Card>
            ) : (
              <Card>
                <CardHeader>
                  <CardTitle>Master Node Information</CardTitle>
                  <CardDescription>This node is configured as a master node</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="rounded-md bg-muted p-4">
                    <h3 className="font-medium">Replication Instructions</h3>
                    <p className="mt-2 text-sm text-muted-foreground">To connect slave nodes to this master:</p>
                    <ol className="mt-2 space-y-2 text-sm text-muted-foreground">
                      <li>1. On the slave node, go to the Replication page</li>
                      <li>2. Enter this node's host address in the Master Host field</li>
                      <li>3. Enter the MySQL port (default: 3306) in the Master Port field</li>
                      <li>4. Click "Setup Replication" to establish the connection</li>
                    </ol>
                  </div>
                </CardContent>
              </Card>
            )}
          </>
        )}
      </div>
    </div>
  )
}
