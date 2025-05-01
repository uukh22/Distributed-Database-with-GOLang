"use client"

import { Badge } from "@/components/ui/badge"
import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import { useToast } from "@/components/ui/use-toast"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { RefreshCw, Bug, Code, Send, Server, CheckCircle, XCircle, AlertTriangle } from "lucide-react"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

export default function DebugPage() {
  const { toast } = useToast()
  const [endpoint, setEndpoint] = useState("/api/health")
  const [method, setMethod] = useState("GET")
  const [requestBody, setRequestBody] = useState("")
  const [response, setResponse] = useState<any>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [responseTime, setResponseTime] = useState<number | null>(null)

  // Connection checker state
  const [connectionStatus, setConnectionStatus] = useState<"idle" | "checking" | "connected" | "error">("idle")
  const [connectionError, setConnectionError] = useState("")
  const [checkingConnection, setCheckingConnection] = useState(false)
  const [connectionResponse, setConnectionResponse] = useState<any>(null)

  const checkConnection = async () => {
    setCheckingConnection(true)
    setConnectionStatus("checking")
    setConnectionResponse(null)
    setConnectionError("")

    try {
      const response = await fetch("/api/health", {
        signal: AbortSignal.timeout(5000), // 5 second timeout
        cache: "no-store",
        headers: {
          "Cache-Control": "no-cache",
        },
      })

      const contentType = response.headers.get("content-type")
      let data: any = null

      try {
        if (contentType && contentType.includes("application/json")) {
          data = await response.json()
        } else {
          data = await response.text()
        }
        setConnectionResponse(data)
      } catch (e) {
        setConnectionResponse("Failed to parse response")
      }

      if (response.ok) {
        setConnectionStatus("connected")
      } else {
        setConnectionStatus("error")
        setConnectionError(`Server responded with status: ${response.status}`)
      }
    } catch (error: any) {
      setConnectionStatus("error")
      if (error.name === "AbortError") {
        setConnectionError("Connection timed out. Server might be down or unreachable.")
      } else if (error.message?.includes("Failed to fetch")) {
        setConnectionError("Cannot connect to the server. Make sure the backend is running on port 8080.")
      } else {
        setConnectionError(error.message || "Unknown error occurred")
      }
    } finally {
      setCheckingConnection(false)
    }
  }

  const sendRequest = async () => {
    setLoading(true)
    setError(null)
    setResponse(null)
    setResponseTime(null)

    const startTime = performance.now()

    try {
      const options: RequestInit = {
        method,
        headers: {
          "Content-Type": "application/json",
          "Cache-Control": "no-cache",
        },
        cache: "no-store",
      }

      if (method !== "GET" && requestBody.trim()) {
        try {
          // Validate JSON
          JSON.parse(requestBody)
          options.body = requestBody
        } catch (e) {
          setError("Invalid JSON in request body")
          setLoading(false)
          return
        }
      }

      const response = await fetch(endpoint, options)
      const endTime = performance.now()
      setResponseTime(endTime - startTime)

      try {
        const data = await response.json()
        setResponse({
          status: response.status,
          statusText: response.statusText,
          headers: Object.fromEntries(response.headers.entries()),
          data,
        })
      } catch (e) {
        // If not JSON, get text
        const text = await response.text()
        setResponse({
          status: response.status,
          statusText: response.statusText,
          headers: Object.fromEntries(response.headers.entries()),
          data: text,
        })
      }
    } catch (e: any) {
      setError(e.message || "An unknown error occurred")
    } finally {
      setLoading(false)
    }
  }

  const commonEndpoints = [
    { value: "/api/health", label: "Health Check" },
    { value: "/api/list-databases", label: "List Databases" },
    { value: "/api/nodes", label: "List Cluster Nodes" },
    { value: "/api/list-tables?db=test", label: "List Tables (db=test)" },
  ]

  return (
    <div className="container mx-auto">
      <div className="flex flex-col gap-6">
        <div className="flex flex-col gap-2">
          <h1 className="text-3xl font-bold tracking-tight">API Debug Tool</h1>
          <p className="text-muted-foreground">Test and debug backend API endpoints</p>
        </div>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Server className="h-5 w-5" />
              <span>Connection Diagnostic</span>
            </CardTitle>
            <CardDescription>Test connection to the backend server</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex flex-col gap-4">
              <div className="flex gap-2">
                <Button onClick={checkConnection} disabled={checkingConnection} className="w-full">
                  {checkingConnection ? (
                    <>
                      <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                      Checking Connection...
                    </>
                  ) : (
                    "Test Backend Connection"
                  )}
                </Button>
              </div>

              {connectionStatus !== "idle" && (
                <div className="flex items-center gap-2">
                  <Badge
                    variant={
                      connectionStatus === "connected"
                        ? "default"
                        : connectionStatus === "checking"
                          ? "outline"
                          : "destructive"
                    }
                    className="px-3 py-1"
                  >
                    {connectionStatus === "connected" && (
                      <>
                        <CheckCircle className="mr-1 h-3 w-3" />
                        Connected
                      </>
                    )}
                    {connectionStatus === "checking" && (
                      <>
                        <RefreshCw className="mr-1 h-3 w-3 animate-spin" />
                        Checking...
                      </>
                    )}
                    {connectionStatus === "error" && (
                      <>
                        <XCircle className="mr-1 h-3 w-3" />
                        Error
                      </>
                    )}
                  </Badge>

                  {connectionStatus === "connected" && (
                    <span className="text-sm text-muted-foreground">Backend server is accessible</span>
                  )}
                </div>
              )}

              {connectionStatus === "error" && (
                <Alert variant="destructive">
                  <AlertTriangle className="h-4 w-4" />
                  <AlertTitle>Connection Error</AlertTitle>
                  <AlertDescription className="text-sm">{connectionError}</AlertDescription>
                </Alert>
              )}

              {connectionResponse && (
                <div className="mt-4">
                  <h3 className="text-sm font-medium mb-2">Response:</h3>
                  <pre className="bg-muted p-3 rounded-md overflow-auto text-xs max-h-40">
                    {typeof connectionResponse === "string"
                      ? connectionResponse
                      : JSON.stringify(connectionResponse, null, 2)}
                  </pre>
                </div>
              )}
            </div>
          </CardContent>
          <CardFooter>
            <div className="text-xs text-muted-foreground">
              <p>Common issues:</p>
              <ul className="list-disc pl-5 mt-1">
                <li>Make sure the Go server is running on port 8080</li>
                <li>Check if CORS is properly configured on the backend</li>
                <li>Verify MySQL connection settings in main.go</li>
              </ul>
            </div>
          </CardFooter>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Bug className="h-5 w-5" />
              <span>API Request Tester</span>
            </CardTitle>
            <CardDescription>Send requests to the backend API and view responses</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex flex-col gap-4">
              <div className="flex flex-col gap-2">
                <label className="text-sm font-medium">Endpoint</label>
                <div className="flex gap-2">
                  <Select value={endpoint} onValueChange={setEndpoint}>
                    <SelectTrigger className="flex-1">
                      <SelectValue placeholder="Select endpoint" />
                    </SelectTrigger>
                    <SelectContent>
                      {commonEndpoints.map((ep) => (
                        <SelectItem key={ep.value} value={ep.value}>
                          {ep.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <Input
                    placeholder="Custom endpoint (e.g., /api/health)"
                    value={endpoint}
                    onChange={(e) => setEndpoint(e.target.value)}
                    className="flex-1"
                  />
                </div>
              </div>

              <div className="flex flex-col gap-2">
                <label className="text-sm font-medium">Method</label>
                <Select value={method} onValueChange={setMethod}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="GET">GET</SelectItem>
                    <SelectItem value="POST">POST</SelectItem>
                    <SelectItem value="PUT">PUT</SelectItem>
                    <SelectItem value="DELETE">DELETE</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {method !== "GET" && (
                <div className="flex flex-col gap-2">
                  <label className="text-sm font-medium">Request Body (JSON)</label>
                  <Textarea
                    placeholder={'{"key": "value"}'}
                    value={requestBody}
                    onChange={(e) => setRequestBody(e.target.value)}
                    rows={5}
                  />
                </div>
              )}
            </div>
          </CardContent>
          <CardFooter>
            <Button onClick={sendRequest} disabled={loading} className="ml-auto">
              {loading ? (
                <>
                  <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                  Sending...
                </>
              ) : (
                <>
                  <Send className="mr-2 h-4 w-4" />
                  Send Request
                </>
              )}
            </Button>
          </CardFooter>
        </Card>

        {error && (
          <Alert variant="destructive">
            <AlertTitle>Error</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        {response && (
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Code className="h-5 w-5" />
                <span>Response</span>
                {responseTime !== null && (
                  <Badge variant="outline" className="ml-2">
                    {responseTime.toFixed(0)}ms
                  </Badge>
                )}
              </CardTitle>
              <CardDescription>
                Status: {response.status} {response.statusText}
              </CardDescription>
            </CardHeader>
            <CardContent>
              <Tabs defaultValue="body">
                <TabsList>
                  <TabsTrigger value="body">Body</TabsTrigger>
                  <TabsTrigger value="headers">Headers</TabsTrigger>
                </TabsList>
                <TabsContent value="body">
                  <pre className="bg-muted p-4 rounded-md overflow-auto max-h-96">
                    {typeof response.data === "string" ? response.data : JSON.stringify(response.data, null, 2)}
                  </pre>
                </TabsContent>
                <TabsContent value="headers">
                  <pre className="bg-muted p-4 rounded-md overflow-auto max-h-96">
                    {JSON.stringify(response.headers, null, 2)}
                  </pre>
                </TabsContent>
              </Tabs>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  )
}
