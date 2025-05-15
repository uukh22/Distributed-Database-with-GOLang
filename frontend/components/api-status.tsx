"use client"

import { useEffect, useState } from "react"
import { AlertCircle, CheckCircle2 } from "lucide-react"

import { apiService } from "@/lib/api-service"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"

export function ApiStatus() {
  const [status, setStatus] = useState<"loading" | "connected" | "disconnected">("loading")
  const [nodeInfo, setNodeInfo] = useState<{ role: string; shardId: number; masterUrl?: string } | null>(null)
  const [lastChecked, setLastChecked] = useState<Date>(new Date())

  const checkConnection = async () => {
    setStatus("loading")
    try {
      const healthResponse = await apiService.checkHealth()
      if (healthResponse.success) {
        setStatus("connected")

        // Get node role information
        const roleResponse = await apiService.getNodeRole()
        if (roleResponse.success && roleResponse.result) {
          setNodeInfo(roleResponse.result)
        }
      } else {
        setStatus("disconnected")
      }
    } catch (error) {
      setStatus("disconnected")
    }
    setLastChecked(new Date())
  }

  useEffect(() => {
    checkConnection()

    // Set up interval to check connection every 30 seconds
    const interval = setInterval(checkConnection, 30000)
    return () => clearInterval(interval)
  }, [])

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-xl">Backend Connection Status</CardTitle>
        <CardDescription>Check the connection to the Go backend server</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col gap-4">
          <div className="flex items-center gap-2">
            {status === "loading" ? (
              <div className="h-4 w-4 animate-spin rounded-full border-2 border-primary border-t-transparent" />
            ) : status === "connected" ? (
              <CheckCircle2 className="h-5 w-5 text-green-500" />
            ) : (
              <AlertCircle className="h-5 w-5 text-destructive" />
            )}
            <span className="font-medium">
              {status === "loading"
                ? "Checking connection..."
                : status === "connected"
                  ? "Connected to backend"
                  : "Disconnected from backend"}
            </span>
          </div>

          {nodeInfo && status === "connected" && (
            <div className="rounded-md bg-muted p-3">
              <div className="grid grid-cols-2 gap-2 text-sm">
                <div className="font-medium">Node Role:</div>
                <div className="capitalize">{nodeInfo.role}</div>

                <div className="font-medium">Shard ID:</div>
                <div>{nodeInfo.shardId}</div>

                {nodeInfo.role === "slave" && nodeInfo.masterUrl && (
                  <>
                    <div className="font-medium">Master URL:</div>
                    <div className="truncate">{nodeInfo.masterUrl}</div>
                  </>
                )}
              </div>
            </div>
          )}

          <div className="flex items-center justify-between">
            <span className="text-xs text-muted-foreground">Last checked: {lastChecked.toLocaleTimeString()}</span>
            <Button size="sm" onClick={checkConnection}>
              Check Connection
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
