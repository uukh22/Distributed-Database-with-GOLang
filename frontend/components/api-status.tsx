"use client"

import { useState, useEffect } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { RefreshCw, CheckCircle, XCircle, AlertTriangle } from "lucide-react"

export function ApiStatus() {
  const [status, setStatus] = useState<"checking" | "connected" | "error">("checking")
  const [errorMessage, setErrorMessage] = useState("")
  const [checking, setChecking] = useState(false)

  const checkApiStatus = async () => {
    setChecking(true)
    setStatus("checking")

    try {
      // زيادة مهلة الاتصال إلى 5 ثوانٍ
      const response = await fetch("/api/health", {
        signal: AbortSignal.timeout(5000), // 5 second timeout
        // إضافة معلمات إضافية لتجنب مشاكل التخزين المؤقت
        cache: "no-store",
        headers: {
          "Cache-Control": "no-cache",
        },
      })

      if (response.ok) {
        setStatus("connected")
        setErrorMessage("")
      } else {
        setStatus("error")
        const errorText = await response.text()
        setErrorMessage(`Server responded with status: ${response.status}. Details: ${errorText}`)
      }
    } catch (error: any) {
      setStatus("error")
      if (error.name === "AbortError") {
        setErrorMessage("Connection timed out. Server might be down or unreachable.")
      } else if (error.message?.includes("Failed to fetch")) {
        setErrorMessage("Cannot connect to the server. Make sure the backend is running on port 8080.")
      } else {
        setErrorMessage(error.message || "Unknown error occurred")
      }
    } finally {
      setChecking(false)
    }
  }

  useEffect(() => {
    checkApiStatus()
    // Check status every 30 seconds
    const interval = setInterval(checkApiStatus, 30000)
    return () => clearInterval(interval)
  }, [])

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-lg">Backend Connection Status</CardTitle>
        <CardDescription>Check the connection to the Go backend server</CardDescription>
      </CardHeader>
      <CardContent>
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
            <span className="text-sm text-muted-foreground">Backend server is running properly</span>
          )}
        </div>

        {status === "error" && (
          <Alert variant="destructive" className="mt-4">
            <AlertTriangle className="h-4 w-4" />
            <AlertTitle>Connection Error</AlertTitle>
            <AlertDescription className="text-sm">
              {errorMessage}
              <div className="mt-2">
                <strong>Troubleshooting:</strong>
                <ul className="list-disc pl-5 mt-1">
                  <li>Make sure the Go server is running</li>
                  <li>Check if the server is running on the correct port (default: 8080)</li>
                  <li>Verify there are no CORS issues</li>
                  <li>Check MySQL connection in the backend</li>
                </ul>
              </div>
            </AlertDescription>
          </Alert>
        )}
      </CardContent>
      <CardFooter>
        <Button variant="outline" size="sm" onClick={checkApiStatus} disabled={checking} className="ml-auto">
          {checking ? (
            <>
              <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
              Checking...
            </>
          ) : (
            <>
              <RefreshCw className="mr-2 h-4 w-4" />
              Check Connection
            </>
          )}
        </Button>
      </CardFooter>
    </Card>
  )
}
