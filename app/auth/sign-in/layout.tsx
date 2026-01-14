import Background from "@/components/background";

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="relative min-h-screen overflow-x-hidden">
        <Background />
        {children}
      </body>
    </html>
  );
}
