import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { ArrowRight, Database, Server, Table2, Bug, Network, Activity } from "lucide-react"
import Link from "next/link"
import { ApiStatus } from "@/components/api-status"
import { SystemStatus } from "@/components/system-status"

export default function Home() {
  return (
    <div className="container mx-auto">
      <div className="flex flex-col gap-6">
        <div className="flex flex-col gap-2">
          <h1 className="text-3xl font-bold tracking-tight">Distributed Database Management System</h1>
          <p className="text-muted-foreground">Manage your distributed database cluster with ease</p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <ApiStatus />
          <SystemStatus />
        </div>

        <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2">
                <Database className="h-5 w-5" />
                <span>Databases</span>
              </CardTitle>
              <CardDescription>Create and manage databases</CardDescription>
            </CardHeader>
            <CardContent>
              <p className="text-sm">Create new databases, view existing ones, and manage their settings.</p>
            </CardContent>
            <CardFooter>
              <Link href="/databases" className="w-full">
                <Button className="w-full">
                  Manage Databases
                  <ArrowRight className="ml-2 h-4 w-4" />
                </Button>
              </Link>
            </CardFooter>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2">
                <Table2 className="h-5 w-5" />
                <span>Tables</span>
              </CardTitle>
              <CardDescription>Create and manage tables</CardDescription>
            </CardHeader>
            <CardContent>
              <p className="text-sm">Design table schemas, add columns, and create relationships between tables.</p>
            </CardContent>
            <CardFooter>
              <Link href="/tables" className="w-full">
                <Button className="w-full">
                  Manage Tables
                  <ArrowRight className="ml-2 h-4 w-4" />
                </Button>
              </Link>
            </CardFooter>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2">
                <Network className="h-5 w-5" />
                <span>Cluster</span>
              </CardTitle>
              <CardDescription>Manage cluster nodes</CardDescription>
            </CardHeader>
            <CardContent>
              <p className="text-sm">Monitor and manage nodes in your distributed database cluster.</p>
            </CardContent>
            <CardFooter>
              <Link href="/cluster" className="w-full">
                <Button className="w-full">
                  Manage Cluster
                  <ArrowRight className="ml-2 h-4 w-4" />
                </Button>
              </Link>
            </CardFooter>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2">
                <Activity className="h-5 w-5" />
                <span>Replication</span>
              </CardTitle>
              <CardDescription>Configure database replication</CardDescription>
            </CardHeader>
            <CardContent>
              <p className="text-sm">Set up master-slave replication for high availability and data redundancy.</p>
            </CardContent>
            <CardFooter>
              <Link href="/replication" className="w-full">
                <Button className="w-full">
                  Setup Replication
                  <ArrowRight className="ml-2 h-4 w-4" />
                </Button>
              </Link>
            </CardFooter>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2">
                <Server className="h-5 w-5" />
                <span>CRUD Operations</span>
              </CardTitle>
              <CardDescription>Manage data in tables</CardDescription>
            </CardHeader>
            <CardContent>
              <p className="text-sm">Perform create, read, update, and delete operations on your data.</p>
            </CardContent>
            <CardFooter>
              <Link href="/crud" className="w-full">
                <Button className="w-full">
                  Manage Data
                  <ArrowRight className="ml-2 h-4 w-4" />
                </Button>
              </Link>
            </CardFooter>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2">
                <Bug className="h-5 w-5" />
                <span>Debug Tools</span>
              </CardTitle>
              <CardDescription>Test and troubleshoot API endpoints</CardDescription>
            </CardHeader>
            <CardContent>
              <p className="text-sm">Use the debug tools to test API endpoints and diagnose connection issues.</p>
            </CardContent>
            <CardFooter>
              <Link href="/debug" className="w-full">
                <Button className="w-full">
                  Open Debug Tools
                  <ArrowRight className="ml-2 h-4 w-4" />
                </Button>
              </Link>
            </CardFooter>
          </Card>
        </div>
      </div>
    </div>
  )
}
