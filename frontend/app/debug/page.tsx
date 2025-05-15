"use client"

import { Badge } from "@/components/ui/badge"

import { useState } from "react"
import { Bug, Send, CheckCircle2, XCircle } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { toast } from "@/components/ui/use-toast"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"

export default function DebugPage() {
  const [apiUrl, setApiUrl] = useState("http://localhost:8080/api/health")
  const [method, setMethod] = useState("GET")
  const [requestBody, setRequestBody] = useState("")
  const [response, setResponse] = useState<any>(null)
  const [loading, setLoading] = useState(false)
  const [status, setStatus] = useState<"idle" | "success" | "error">("idle")
  const [statusCode, setStatusCode] = useState<number | null>(null)
  const [responseTime, setResponseTime] = useState<number | null>(null)

  const handleSendRequest = async () => {
    setLoading(true)
    setStatus("idle")
    setResponse(null)
    setStatusCode(null)
    setResponseTime(null)

    try {
      const startTime = performance.now()

      const options: RequestInit = {
        method,
        headers: {
          "Content-Type": "application/json",
        },
      }

      if (method !== "GET" && requestBody) {
        try {
          // Validate JSON
          JSON.parse(requestBody)
          options.body = requestBody
        } catch (e) {
          toast({
            variant: "destructive",
            title: "Invalid JSON",
            description: "The request body is not valid JSON",
          })
          setLoading(false)
          return
        }
      }

      const response = await fetch(apiUrl, options)
      const endTime = performance.now()

      setResponseTime(Math.round(endTime - startTime))
      setStatusCode(response.status)

      const data = await response.json()
      setResponse(data)
      setStatus(response.ok ? "success" : "error")
    } catch (error) {
      setStatus("error")
      setResponse({
        error: error instanceof Error ? error.message : "Unknown error occurred",
      })
    } finally {
      setLoading(false)
    }
  }

  const formatJson = (json: any) => {
    try {
      return JSON.stringify(json, null, 2)
    } catch (e) {
      return String(json)
    }
  }

  return (
    <div className="container mx-auto py-6">
      <div className="flex flex-col gap-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">Debug Tools</h1>
            <p className="text-muted-foreground">Test and troubleshoot API endpoints</p>
          </div>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>API Request Tester</CardTitle>
            <CardDescription>Send requests to API endpoints and view responses</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid gap-6">
              <div className="grid gap-4 md:grid-cols-4">
                <div className="md:col-span-3">
                  <Label htmlFor="apiUrl">API URL</Label>
                  <Input
                    id="apiUrl"
                    value={apiUrl}
                    onChange={(e) => setApiUrl(e.target.value)}
                    placeholder="Enter API URL"
                  />
                </div>
                <div>
                  <Label htmlFor="method">Method</Label>
                  <select
                    id="method"
                    value={method}
                    onChange={(e) => setMethod(e.target.value)}
                    className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    <option value="GET">GET</option>
                    <option value="POST">POST</option>
                    <option value="PUT">PUT</option>
                    <option value="DELETE">DELETE</option>
                  </select>
                </div>
              </div>

              {method !== "GET" && (
                <div className="grid gap-2">
                  <Label htmlFor="requestBody">Request Body (JSON)</Label>
                  <Textarea
                    id="requestBody"
                    value={requestBody}
                    onChange={(e) => setRequestBody(e.target.value)}
                    placeholder="Enter JSON request body"
                    className="min-h-[150px] font-mono"
                  />
                </div>
              )}

              <Button onClick={handleSendRequest} disabled={loading} className="w-full md:w-auto">
                {loading ? (
                  <div className="mr-2 h-4 w-4 animate-spin rounded-full border-2 border-background border-t-transparent"></div>
                ) : (
                  <Send className="mr-2 h-4 w-4" />
                )}
                Send Request
              </Button>

              {(response || loading) && (
                <div className="mt-4 grid gap-4">
                  <div className="flex items-center justify-between">
                    <h3 className="text-lg font-medium">Response</h3>
                    {statusCode && (
                      <div className="flex items-center gap-2">
                        <span className="text-sm font-medium">Status:</span>
                        <Badge variant={statusCode >= 200 && statusCode < 300 ? "outline" : "destructive"}>
                          {statusCode}
                        </Badge>
                        {responseTime && <span className="text-sm text-muted-foreground">{responseTime}ms</span>}
                      </div>
                    )}
                  </div>

                  {loading ? (
                    <div className="flex items-center justify-center py-12">
                      <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent"></div>
                    </div>
                  ) : (
                    <Tabs defaultValue="formatted">
                      <TabsList className="grid w-full grid-cols-2">
                        <TabsTrigger value="formatted">Formatted</TabsTrigger>
                        <TabsTrigger value="raw">Raw</TabsTrigger>
                      </TabsList>
                      <TabsContent value="formatted" className="mt-2">
                        <div className="rounded-md bg-muted p-4">
                          <div className="flex items-center gap-2 mb-2">
                            {status === "success" ? (
                              <CheckCircle2 className="h-5 w-5 text-green-500" />
                            ) : status === "error" ? (
                              <XCircle className="h-5 w-5 text-destructive" />
                            ) : (
                              <Bug className="h-5 w-5" />
                            )}
                            <span className="font-medium">
                              {status === "success" ? "Success" : status === "error" ? "Error" : "Response"}
                            </span>
                          </div>
                          <pre className="overflow-x-auto text-sm">{formatJson(response)}</pre>
                        </div>
                      </TabsContent>
                      <TabsContent value="raw" className="mt-2">
                        <Textarea readOnly value={formatJson(response)} className="min-h-[300px] font-mono" />
                      </TabsContent>
                    </Tabs>
                  )}
                </div>
              )}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Common API Endpoints</CardTitle>
            <CardDescription>Quick access to frequently used API endpoints</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
              <Button
                variant="outline"
                className="justify-start"
                onClick={() => {
                  setApiUrl("http://localhost:8080/api/health")
                  setMethod("GET")
                  setRequestBody("")
                }}
              >
                <Bug className="mr-2 h-4 w-4" />
                Health Check
              </Button>

              <Button
                variant="outline"
                className="justify-start"
                onClick={() => {
                  setApiUrl("http://localhost:8080/api/node-role")
                  setMethod("GET")
                  setRequestBody("")
                }}
              >
                <Bug className="mr-2 h-4 w-4" />
                Node Role
              </Button>

              <Button
                variant="outline"
                className="justify-start"
                onClick={() => {
                  setApiUrl("http://localhost:8080/api/nodes")
                  setMethod("GET")
                  setRequestBody("")
                }}
              >
                <Bug className="mr-2 h-4 w-4" />
                List Nodes
              </Button>

              <Button
                variant="outline"
                className="justify-start"
                onClick={() => {
                  setApiUrl("http://localhost:8080/api/list-databases")
                  setMethod("GET")
                  setRequestBody("")
                }}
              >
                <Bug className="mr-2 h-4 w-4" />
                List Databases
              </Button>

              <Button
                variant="outline"
                className="justify-start"
                onClick={() => {
                  setApiUrl("http://localhost:8080/api/create-db")
                  setMethod("POST")
                  setRequestBody(JSON.stringify({ dbName: "test_db" }, null, 2))
                }}
              >
                <Bug className="mr-2 h-4 w-4" />
                Create Database
              </Button>

              <Button
                variant="outline"
                className="justify-start"
                onClick={() => {
                  setApiUrl("http://localhost:8080/api/list-tables?db=test_db")
                  setMethod("GET")
                  setRequestBody("")
                }}
              >
                <Bug className="mr-2 h-4 w-4" />
                List Tables
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
