"use client"

import { useEffect, useState } from "react"
import { AlertCircle, CheckCircle2, Server } from "lucide-react"

import { apiService, type Node } from "@/lib/api-service"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"

export function SystemStatus() {
  const [nodes, setNodes] = useState<Node[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [lastUpdated, setLastUpdated] = useState<Date>(new Date())
  const [shardCount, setShardCount] = useState<number>(3) // Default to 3 shards

  const fetchNodes = async () => {
    setLoading(true)
    try {
      const response = await apiService.getNodes()
      if (response.success && response.result) {
        setNodes(response.result)

        // Determine the shard count from the nodes
        if (response.result.length > 0) {
          const maxShardId = Math.max(...response.result.map((node) => node.shardId))
          setShardCount(maxShardId + 1) // Assuming shardIds are 0-based
        }

        setError(null)
      } else {
        setError(response.message || "Failed to fetch nodes")
      }
    } catch (error) {
      setError("An error occurred while fetching nodes")
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

  // Count nodes by role and health
  const masterCount = nodes.filter((node) => node.role === "master").length
  const healthySlaves = nodes.filter((node) => node.role === "slave" && node.isHealthy).length
  const unhealthyNodes = nodes.filter((node) => !node.isHealthy).length

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-xl">System Status</CardTitle>
        <CardDescription>Current status of the distributed database system</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col gap-4">
          {loading ? (
            <div className="flex items-center justify-center py-4">
              <div className="h-6 w-6 animate-spin rounded-full border-2 border-primary border-t-transparent" />
            </div>
          ) : error ? (
            <div className="flex items-center gap-2 text-destructive">
              <AlertCircle className="h-5 w-5" />
              <span>{error}</span>
            </div>
          ) : (
            <>
              <div className="grid grid-cols-4 gap-2">
                <div className="flex flex-col items-center justify-center rounded-md bg-muted p-3">
                  <span className="text-2xl font-bold">{nodes.length}</span>
                  <span className="text-xs text-muted-foreground">Total Nodes</span>
                </div>
                <div className="flex flex-col items-center justify-center rounded-md bg-muted p-3">
                  <span className="text-2xl font-bold">{masterCount}</span>
                  <span className="text-xs text-muted-foreground">Master Nodes</span>
                </div>
                <div className="flex flex-col items-center justify-center rounded-md bg-muted p-3">
                  <span className="text-2xl font-bold">{healthySlaves}</span>
                  <span className="text-xs text-muted-foreground">Healthy Slaves</span>
                </div>
                <div className="flex flex-col items-center justify-center rounded-md bg-muted p-3">
                  <span className="text-2xl font-bold">{shardCount}</span>
                  <span className="text-xs text-muted-foreground">Shards</span>
                </div>
              </div>

              <div className="space-y-2">
                <h4 className="text-sm font-medium">Node Health</h4>
                <div className="flex items-center gap-2">
                  {unhealthyNodes > 0 ? (
                    <Badge variant="destructive">{unhealthyNodes} unhealthy nodes</Badge>
                  ) : (
                    <Badge
                      variant="outline"
                      className="bg-green-50 text-green-700 dark:bg-green-900/20 dark:text-green-400"
                    >
                      All nodes healthy
                    </Badge>
                  )}
                </div>
              </div>

              <div className="space-y-2">
                <h4 className="text-sm font-medium">Cluster Nodes</h4>
                <div className="max-h-[150px] overflow-y-auto rounded-md border">
                  {nodes.length === 0 ? (
                    <div className="p-3 text-center text-sm text-muted-foreground">No nodes found</div>
                  ) : (
                    <div className="divide-y">
                      {nodes.map((node) => (
                        <div key={node.id} className="flex items-center justify-between p-2 text-sm">
                          <div className="flex items-center gap-2">
                            <Server className="h-4 w-4" />
                            <span className="font-medium">{node.url}</span>
                          </div>
                          <div className="flex items-center gap-2">
                            <Badge variant={node.role === "master" ? "default" : "secondary"}>{node.role}</Badge>
                            <Badge variant="outline">Shard {node.shardId}</Badge>
                            {node.isHealthy ? (
                              <CheckCircle2 className="h-4 w-4 text-green-500" />
                            ) : (
                              <AlertCircle className="h-4 w-4 text-destructive" />
                            )}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            </>
          )}

          <div className="text-right text-xs text-muted-foreground">
            Last updated: {lastUpdated.toLocaleTimeString()}
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
