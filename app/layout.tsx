import "./globals.css";
import type { Metadata } from "next";
import { Inter } from "next/font/google";
import { headers } from "next/headers";
import { type ReactNode } from "react";
import { cookieToInitialState } from "wagmi";

import { getConfig } from '@/lib/wagmi'
import { Providers } from '@/app/providers'
import { NeuralNetworkBackground } from '@/components/NeuralNetworkBackground'

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Airdrop App",
  description: "Connect your wallet to participate in airdrops",
};

export default async function RootLayout(props: { children: ReactNode }) {
  const initialState = cookieToInitialState(
    getConfig(),
    (await headers()).get("cookie")
  );
  return (
    <html lang="en" className="dark">
      <body className={`${inter.className} antialiased bg-black`}>
        <Providers initialState={initialState}>
          <div className="relative min-h-screen">
            {/* Global Background Layers */}
            <div className="fixed inset-0 pointer-events-none overflow-hidden">
              {/* Layer 0: Technical Grid */}
              <div className="absolute inset-0 bg-[url('data:image/svg+xml,%3Csvg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 32 32%22 width=%2232%22 height=%2232%22 fill=%22none%22 stroke=%22white%22 stroke-opacity=%220.03%22%3E%3Cpath d=%22M0 .5H31.5V32%22/%3E%3C/svg%3E')] antialiased z-0" />

              {/* Layer 1: Ambient Glows */}
              <div className="absolute top-[-10%] left-[-10%] w-[60%] h-[60%] bg-primary/10 rounded-full blur-[120px] opacity-20 animate-pulse z-2" />
              <div className="absolute bottom-[-10%] right-[-10%] w-[60%] h-[60%] bg-blue-600/10 rounded-full blur-[120px] opacity-20 animate-pulse z-2" style={{ animationDelay: '2s' }} />
            </div>

            <NeuralNetworkBackground />

            <div className="relative z-10">
              {props.children}
            </div>
          </div>
        </Providers>
      </body>
    </html>
  );
}
