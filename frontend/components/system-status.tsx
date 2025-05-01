"use client"

import { useState, useEffect } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { RefreshCw, CheckCircle, XCircle, AlertTriangle, Server, ShieldAlert } from "lucide-react"
import { Progress } from "@/components/ui/progress"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"

type NodeStatus = {
  id: string
  role: string
  url: string
  isHealthy: boolean
  lastSeen: string
}

export function SystemStatus() {
  const [nodes, setNodes] = useState<NodeStatus[]>([])
  const [loading, setLoading] = useState(false)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const [error, setError] = useState<string | null>(null)

  const fetchNodeStatus = async () => {
    setLoading(true)
    setError(null)
    try {
      const response = await fetch("/api/nodes", {
        cache: "no-store",
        headers: {
          "Cache-Control": "no-cache",
        },
        signal: AbortSignal.timeout(5000), // 5 second timeout
      })

      if (response.ok) {
        const data = await response.json()
        if (data.success && data.result) {
          setNodes(data.result)
        } else {
          setError(data.message || "Failed to fetch node data")
        }
      } else {
        setError(`Server responded with status: ${response.status}`)
      }
    } catch (error: any) {
      if (error.name === "AbortError") {
        setError("Connection timed out. Server might be down or unreachable.")
      } else {
        setError(error.message || "Failed to fetch node status")
      }
      console.error("Failed to fetch node status:", error)
    } finally {
      setLoading(false)
      setLastUpdated(new Date())
    }
  }

  useEffect(() => {
    fetchNodeStatus()
    const interval = setInterval(fetchNodeStatus, 30000) // Update every 30 seconds
    return () => clearInterval(interval)
  }, [])

  const healthyNodes = nodes.filter((node) => node.isHealthy).length
  const totalNodes = nodes.length
  const healthPercentage = totalNodes > 0 ? (healthyNodes / totalNodes) * 100 : 0

  const masterNode = nodes.find((node) => node.role === "master")
  const systemHealthy = masterNode?.isHealthy ?? false

  // Calculate time since last seen for each node
  const getTimeSinceLastSeen = (lastSeenStr: string) => {
    const lastSeen = new Date(lastSeenStr)
    const now = new Date()
    const diffMs = now.getTime() - lastSeen.getTime()
    const diffSec = Math.floor(diffMs / 1000)

    if (diffSec < 60) return `${diffSec}s ago`
    if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`
    return `${Math.floor(diffSec / 3600)}h ago`
  }

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-lg flex items-center gap-2">
          <Server className="h-5 w-5" />
          Cluster Status
        </CardTitle>
        <CardDescription>Current status of your database cluster</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col gap-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Badge variant={systemHealthy ? "default" : "destructive"} className="px-3 py-1">
                {systemHealthy ? (
                  <>
                    <CheckCircle className="mr-1 h-3 w-3" />
                    Healthy
                  </>
                ) : (
                  <>
                    <AlertTriangle className="mr-1 h-3 w-3" />
                    Issues Detected
                  </>
                )}
              </Badge>
              <span className="text-sm text-muted-foreground">
                {healthyNodes} of {totalNodes} nodes online
              </span>
            </div>
            {lastUpdated && (
              <span className="text-xs text-muted-foreground">Updated {lastUpdated.toLocaleTimeString()}</span>
            )}
          </div>

          <div className="space-y-2">
            <div className="flex justify-between text-xs">
              <span>Cluster Health</span>
              <span>{healthPercentage.toFixed(0)}%</span>
            </div>
            <Progress
              value={healthPercentage}
              className="h-1"
              indicatorClassName={
                healthPercentage < 50 ? "bg-red-500" : healthPercentage < 80 ? "bg-yellow-500" : undefined
              }
            />
          </div>

          {error && (
            <Alert variant="destructive" className="mt-2">
              <ShieldAlert className="h-4 w-4" />
              <AlertTitle>Connection Error</AlertTitle>
              <AlertDescription className="text-sm">{error}</AlertDescription>
            </Alert>
          )}

          {masterNode && (
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <div className="rounded-md bg-muted p-3 cursor-help">
                    <div className="flex items-center gap-2">
                      <Server className="h-4 w-4" />
                      <span className="font-medium">Master Node:</span>
                      <span className="text-sm">{masterNode.url}</span>
                      {masterNode.isHealthy ? (
                        <CheckCircle className="h-3 w-3 text-green-500 ml-auto" />
                      ) : (
                        <XCircle className="h-3 w-3 text-red-500 ml-auto" />
                      )}
                    </div>
                  </div>
                </TooltipTrigger>
                <TooltipContent>
                  <p>Last seen: {masterNode.lastSeen ? getTimeSinceLastSeen(masterNode.lastSeen) : "Unknown"}</p>
                  <p>Node ID: {masterNode.id}</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          )}

          {nodes.length > 0 && (
            <div className="text-xs text-muted-foreground mt-2">
              <span>{nodes.filter((n) => n.role === "slave").length} slave nodes</span>
              {nodes.filter((n) => !n.isHealthy).length > 0 && (
                <span className="text-red-500 ml-2">({nodes.filter((n) => !n.isHealthy).length} offline)</span>
              )}
            </div>
          )}

          {nodes.filter((n) => n.role === "slave").length > 0 && (
            <div className="mt-2 space-y-2">
              <h4 className="text-sm font-medium">Slave Nodes</h4>
              <div className="space-y-1">
                {nodes
                  .filter((n) => n.role === "slave")
                  .map((node) => (
                    <TooltipProvider key={node.id}>
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <div className="flex items-center justify-between text-xs p-2 rounded-md bg-muted/50 cursor-help">
                            <div className="flex items-center gap-2">
                              <div
                                className={`w-2 h-2 rounded-full ${node.isHealthy ? "bg-green-500" : "bg-red-500"}`}
                              ></div>
                              <span>{node.url}</span>
                            </div>
                            <span className="text-muted-foreground">
                              {node.lastSeen ? getTimeSinceLastSeen(node.lastSeen) : "Unknown"}
                            </span>
                          </div>
                        </TooltipTrigger>
                        <TooltipContent>
                          <p>Node ID: {node.id}</p>
                          <p>Status: {node.isHealthy ? "Healthy" : "Unhealthy"}</p>
                        </TooltipContent>
                      </Tooltip>
                    </TooltipProvider>
                  ))}
              </div>
            </div>
          )}
        </div>
      </CardContent>
      <CardFooter>
        <Button variant="outline" size="sm" onClick={fetchNodeStatus} disabled={loading} className="ml-auto">
          {loading ? (
            <>
              <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
              Updating...
            </>
          ) : (
            <>
              <RefreshCw className="mr-2 h-4 w-4" />
              Refresh Status
            </>
          )}
        </Button>
      </CardFooter>
    </Card>
  )
}
