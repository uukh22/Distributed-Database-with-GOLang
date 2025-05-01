"use client"

import { Database, Home, Server, Settings, Table2, Network, Activity } from "lucide-react"
import Link from "next/link"
import { usePathname } from "next/navigation"
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarSeparator,
  SidebarTrigger,
} from "@/components/ui/sidebar"
import { ModeToggle } from "./mode-toggle"

export function AppSidebar() {
  const pathname = usePathname()

  const menuItems = [
    {
      title: "Dashboard",
      icon: Home,
      href: "/",
    },
    {
      title: "Databases",
      icon: Database,
      href: "/databases",
    },
    {
      title: "Tables",
      icon: Table2,
      href: "/tables",
    },
    {
      title: "CRUD Operations",
      icon: Server,
      href: "/crud",
    },
    {
      title: "Cluster Management",
      icon: Network,
      href: "/cluster",
    },
    {
      title: "Replication",
      icon: Activity,
      href: "/replication",
    },
  ]

  return (
    <Sidebar>
      <SidebarHeader className="flex items-center justify-between px-4 py-2">
        <div className="flex items-center gap-2">
          <Database className="h-6 w-6" />
          <span className="font-semibold">Distributed DB</span>
        </div>
        <SidebarTrigger />
      </SidebarHeader>
      <SidebarSeparator />
      <SidebarContent>
        <SidebarMenu>
          {menuItems.map((item) => (
            <SidebarMenuItem key={item.href}>
              <SidebarMenuButton asChild isActive={pathname === item.href} tooltip={item.title}>
                <Link href={item.href}>
                  <item.icon className="h-4 w-4" />
                  <span>{item.title}</span>
                </Link>
              </SidebarMenuButton>
            </SidebarMenuItem>
          ))}
        </SidebarMenu>
      </SidebarContent>
      <SidebarSeparator />
      <SidebarFooter className="p-4">
        <div className="flex items-center justify-between">
          <Link href="/settings">
            <Settings className="h-5 w-5" />
          </Link>
          <ModeToggle />
        </div>
      </SidebarFooter>
    </Sidebar>
  )
}
