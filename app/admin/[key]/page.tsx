import { notFound } from "next/navigation";
import AdminClient from "./AdminClient";

export default async function Page({
  params,
}: {
  params: Promise<{ key: string }>;
}) {
  const { key } = await params;

  if (key !== process.env.ADMIN_ROUTE_KEY) {
    notFound();
  }

  return <AdminClient />;
}
