'use client'

import React, { useState, useEffect } from 'react'
import Link from 'next/link'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { ArrowRight, Box, ShieldCheck, Sparkles, Zap, Terminal } from 'lucide-react'

export default function LandingPage() {
  const [mounted, setMounted] = useState(false)

  const socials = [
    { name: 'LinkedIn', link: 'https://www.linkedin.com/company/jadavpur-university-acm-student-chapter' },
    { name: 'Instagram', link: 'https://www.instagram.com/acm.ju' },
    { name: 'Website', link: 'https://acm-ju.vercel.app/' }
  ]

  useEffect(() => {
    setMounted(true)
  }, [])

  if (!mounted) return null

  return (
    <div className="min-h-screen text-white selection:bg-primary/30 relative overflow-x-hidden font-sans">
      <div className="min-h-screen flex flex-col">
        {/* Navigation - Clean JU ACM Branding */}
        <nav className="relative z-20 flex justify-between items-center px-8 py-6 max-w-7xl mx-auto w-full shrink-0">
          <div className="flex items-center gap-4 group cursor-pointer">
            <div className="w-10 h-10 bg-white text-black flex items-center justify-center rounded-xl font-black group-hover:rotate-12 transition-all shadow-[0_0_30px_rgba(255,255,255,0.2)]">
              JU
            </div>
            <div className="flex flex-col">
              <span className="text-lg font-black tracking-tighter italic leading-none text-white">ACM Portal</span>
              <span className="text-[10px] font-black text-neutral-500 tracking-[0.3em] uppercase mt-1">Synchronicity_2026</span>
            </div>
          </div>
        </nav>

        {/* Main Hero Content - Centered in VH */}
        <main className="relative z-10 flex-1 flex flex-col items-center justify-center text-center px-6 max-w-7xl mx-auto w-full py-4">
          {/* Status Badge */}
          <div className="mb-8 flex flex-col items-center gap-4 animate-in fade-in slide-in-from-bottom-5 duration-700">
            <Badge variant="outline" className="px-5 py-2.5 border-white/5 bg-white/5 text-neutral-500 backdrop-blur-md rounded-full text-[10px] font-black tracking-[0.4em] uppercase group cursor-default">
              <Sparkles className="w-3.5 h-3.5 mr-3 text-primary animate-pulse" />
              JADAVPUR UNIVERSITY ACM STUDENT BRANCH PRESENTS
            </Badge>

            <div className="relative group/sync">
              <h2 className="text-4xl md:text-6xl font-black tracking-[0.2em] uppercase italic bg-linear-to-r from-primary via-blue-400 to-primary bg-size-[200%_auto] animate-gradient bg-clip-text text-transparent relative z-10 filter drop-shadow-[0_0_30px_rgba(var(--primary),0.2)] py-2">
                SYNCHRONICITY
              </h2>
              <div className="absolute -bottom-2 left-0 w-full h-px bg-linear-to-r from-transparent via-white/20 to-transparent" />
              <div className="absolute -bottom-[14px] left-1/2 -translate-x-1/2 flex items-center gap-3">
                <div className="h-px w-10 bg-linear-to-r from-transparent to-primary/50" />
                <div className="w-2 h-2 bg-white rotate-45 shadow-[0_0_15px_#fff] animate-pulse" />
                <div className="h-px w-10 bg-linear-to-l from-transparent to-primary/50" />
              </div>
            </div>
          </div>

          {/* Hero Title */}
          <div className="space-y-4 max-w-5xl mb-10 animate-in fade-in slide-in-from-bottom-10 duration-1000">
            <h1 className="text-6xl md:text-[8rem] font-black tracking-tighter leading-[0.8] italic uppercase">
              Sync_Your <br />
              <span className="bg-linear-to-r from-primary via-blue-400 to-primary bg-size-[200%_auto] animate-gradient bg-clip-text text-transparent inline-block p-4">
                Logic
              </span>
            </h1>
            <p className="text-base md:text-lg text-neutral-500 max-w-xl mx-auto font-bold italic tracking-tight leading-relaxed uppercase">
              36 Hours of Parallel Innovation. <br />
              Where Vision Synchronizes with the Jadavpur University Legacy.
            </p>
          </div>

          {/* Action Buttons */}
          <div className="flex flex-col sm:flex-row items-center justify-center gap-6 animate-in fade-in slide-in-from-bottom-12 duration-1200">
            <Link href="/airdrop">
              <Button className="h-16 px-16 text-base font-black bg-white text-black hover:bg-neutral-200 border-none rounded-2xl shadow-[0_0_50px_rgba(255,255,255,0.15)] flex items-center gap-4 group transition-all">
                INITIALIZE IDENTITY
                <ArrowRight className="w-5 h-5 group-hover:translate-x-2 transition-transform" />
              </Button>
            </Link>
          </div>
        </main>
      </div>

      {/* Feature Dashboard - Scrollable below the fold */}
      <section className="relative z-10 py-32 px-6 max-w-7xl mx-auto flex flex-col items-center">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 w-full max-w-5xl">
          {[
            {
              icon: <ShieldCheck className="w-6 h-6 text-emerald-400" />,
              title: "Verified Nodes",
              description: "Integrated with JU-ACM registration protocols to ensure authentic participation."
            },
            {
              icon: <Zap className="w-6 h-6 text-amber-400" />,
              title: "Binary Harmony",
              description: "Real-time synchronization of your team data with the official hackathon ledger."
            },
            {
              icon: <Box className="w-6 h-6 text-blue-400" />,
              title: "Digital Artifacts",
              description: "Claim your unique identity hash and unlock the Synchronicity resource pool."
            }
          ].map((feature, i) => (
            <div key={i} className="group p-8 rounded-[2.5rem] bg-neutral-900/40 border border-white/5 hover:border-white/10 transition-all text-left space-y-4 backdrop-blur-md shadow-2xl">
              <div className="w-14 h-14 bg-black flex items-center justify-center rounded-2xl border border-white/5 group-hover:border-primary/50 transition-all group-hover:shadow-[0_0_20px_rgba(var(--primary),0.1)]">
                {feature.icon}
              </div>
              <h3 className="text-xl font-black uppercase italic tracking-tighter">{feature.title}</h3>
              <p className="text-neutral-500 font-bold text-xs leading-relaxed italic">
                {feature.description}
              </p>
            </div>
          ))}
        </div>
      </section>

      {/* Terminal Footer */}
      <footer className="relative z-20 border-t border-white/5 py-12 px-8 max-w-7xl mx-auto flex flex-col md:flex-row justify-between items-center gap-10">
        <p className="text-neutral-600 text-[10px] font-black uppercase tracking-widest italic text-center md:text-left">
          © 2026 JU-ACM Chapter. All rights reserved.
        </p>
        <div className="flex gap-10">
          {socials.map((e) => (
            <a key={e.name} href={e.link} className="text-[10px] font-black text-neutral-600 hover:text-white uppercase tracking-widest transition-colors">
              {e.name}
            </a>
          ))}
        </div>
      </footer>

      {/* Cool Technical Animation Effects */}
      <style jsx global>{`
        @keyframes gradient {
          0% { background-position: 0% center; }
          100% { background-position: 200% center; }
        }
        .animate-gradient {
          animation: gradient 4s linear infinite;
        }
      `}</style>
    </div>
  )
}
