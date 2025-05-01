"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { RefreshCw, CheckCircle, XCircle, AlertTriangle } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"

export function ConnectionChecker() {
  const [url, setUrl] = useState("http://localhost:8080/api/health")
  const [status, setStatus] = useState<"idle" | "checking" | "connected" | "error">("idle")
  const [errorMessage, setErrorMessage] = useState("")
  const [responseData, setResponseData] = useState<any>(null)
  const [checking, setChecking] = useState(false)

  const checkConnection = async () => {
    setChecking(true)
    setStatus("checking")
    setResponseData(null)

    try {
      const response = await fetch(url, {
        signal: AbortSignal.timeout(5000), // 5 second timeout
        cache: "no-store",
        headers: {
          "Cache-Control": "no-cache",
        },
      })

      const contentType = response.headers.get("content-type")
      let data: any = null

      if (contentType && contentType.includes("application/json")) {
        data = await response.json()
      } else {
        data = await response.text()
      }

      setResponseData(data)

      if (response.ok) {
        setStatus("connected")
        setErrorMessage("")
      } else {
        setStatus("error")
        setErrorMessage(`Server responded with status: ${response.status}`)
      }
    } catch (error: any) {
      setStatus("error")
      if (error.name === "AbortError") {
        setErrorMessage("Connection timed out. Server might be down or unreachable.")
      } else if (error.message?.includes("Failed to fetch")) {
        setErrorMessage("Cannot connect to the server. Make sure the backend is running and accessible.")
      } else {
        setErrorMessage(error.message || "Unknown error occurred")
      }
    } finally {
      setChecking(false)
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg">Connection Diagnostic Tool</CardTitle>
        <CardDescription>Test connection to the backend server</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col gap-4">
          <div className="flex gap-2">
            <Input placeholder="Backend URL" value={url} onChange={(e) => setUrl(e.target.value)} className="flex-1" />
            <Button onClick={checkConnection} disabled={checking}>
              {checking ? (
                <>
                  <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                  Checking...
                </>
              ) : (
                "Test Connection"
              )}
            </Button>
          </div>

          {status !== "idle" && (
            <div className="flex items-center gap-2">
              <Badge
                variant={status === "connected" ? "default" : status === "checking" ? "outline" : "destructive"}
                className="px-3 py-1"
              >
                {status === "connected" && (
                  <>
                    <CheckCircle className="mr-1 h-3 w-3" />
                    Connected
                  </>
                )}
                {status === "checking" && (
                  <>
                    <RefreshCw className="mr-1 h-3 w-3 animate-spin" />
                    Checking...
                  </>
                )}
                {status === "error" && (
                  <>
                    <XCircle className="mr-1 h-3 w-3" />
                    Error
                  </>
                )}
              </Badge>

              {status === "connected" && (
                <span className="text-sm text-muted-foreground">Backend server is accessible</span>
              )}
            </div>
          )}

          {status === "error" && (
            <Alert variant="destructive">
              <AlertTriangle className="h-4 w-4" />
              <AlertTitle>Connection Error</AlertTitle>
              <AlertDescription className="text-sm">{errorMessage}</AlertDescription>
            </Alert>
          )}

          {responseData && (
            <div className="mt-4">
              <h3 className="text-sm font-medium mb-2">Response:</h3>
              <pre className="bg-muted p-3 rounded-md overflow-auto text-xs max-h-40">
                {typeof responseData === "string" ? responseData : JSON.stringify(responseData, null, 2)}
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
  )
}
