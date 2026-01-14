import { headers } from "next/headers";
import { auth } from "@/lib/auth";

export async function getServerSession() {
  const hdrs = await headers();
  return auth.api.getSession({
    headers: hdrs,
  });
}
