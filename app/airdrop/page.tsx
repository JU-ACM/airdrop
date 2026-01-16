"use client";

import React, { useState, useEffect } from "react";
import { useAccount, useConnect, useDisconnect } from "wagmi";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import Link from "next/link";
import {
  Wallet,
  CheckCircle2,
  AlertCircle,
  Loader2,
  Mail,
  Phone,
  Image as ImageIcon,
  KeyRound,
  ArrowRight,
  Unplug,
  Users,
  Upload,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { signIn, authClient, emailOtp } from "@/lib/auth-client";
import axios from "axios";
import { useRouter } from "next/navigation";

type Step = "WALLET" | "FORM" | "OTP" | "SUCCESS" | "REJECTED";

export default function AirdropPage() {
  const [mounted, setMounted] = useState(false);
  const { address, isConnected, isConnecting } = useAccount();
  const {
    connect,
    status: connectStatus,
    error: connectError,
    connectors,
  } = useConnect();
  const { disconnect } = useDisconnect();
  const router = useRouter();

  const [step, setStep] = useState<Step>("WALLET");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Form State
  const [formData, setFormData] = useState({
    email: "",
    phone: "",
    teamName: "",
    favoriteImageName: "",
  });
  const [imageFile, setImageFile] = useState<File | null>(null);
  const [imagePreview, setImagePreview] = useState<string | null>(null);
  const [otp, setOtp] = useState("");
  const [regHash, setRegHash] = useState<string | null>(null);

  useEffect(() => {
    setMounted(true);
  }, []);

  useEffect(() => {
    if (!mounted) return;
    if (isConnected && step === "WALLET") {
      setStep("FORM");
    }
    if (
      !isConnected &&
      step !== "SUCCESS" &&
      step !== "REJECTED" &&
      step !== "WALLET"
    ) {
      setStep("WALLET");
    }
  }, [isConnected, step, mounted]);

  const handleFormSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!imageFile) {
      setError("PROTOCOL_ERROR: Identity_Artifact (Logo) is required.");
      return;
    }

    setLoading(true);
    setError(null);

    try {
      // 1. Check Eligibility First
      try {
        await axios.post("/api/check-eligibility", { email: formData.email });
      } catch (checkErr: any) {
        // Forward the specific API error message
        throw new Error(
          checkErr.response?.data?.error || "Eligibility check failed."
        );
      }

      // 2. If eligible, Send OTP via better-auth
      const res = await emailOtp.sendVerificationOtp({
        email: formData.email,
        type: "sign-in",
      });

      if (res?.error) {
        throw new Error(res.error.message);
      }

      setStep("OTP");
    } catch (err: any) {
      console.error(err);
      setError(err.message || "Verification failed. Please check email.");
    } finally {
      setLoading(false);
    }
  };

  const handleOtpSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);

    try {
      // 1. Verify OTP
      const res = await emailOtp.checkVerificationOtp({
        email: formData.email,
        otp: otp,
        type: "sign-in",
      });
      if (res?.error) {
        throw new Error(res.error.message);
      }

      // 2. Submit Claim to Backend
      const data = new FormData();
      data.append("phone", formData.phone);
      data.append("walletAddress", address as string);
      data.append("image", imageFile!);

      const claimRes = await axios.post("/api/claim", data);

      if (claimRes.data.success) {
        setRegHash(claimRes.data.txHash || "PENDING_MINT"); // Backend might allow immediate mint or queue
        setStep("SUCCESS");
      } else {
        throw new Error("Claim failed");
      }
    } catch (err: any) {
      console.error(err);
      const msg =
        err.response?.data?.error ||
        err.message ||
        "Invalid OTP or Registration Failed.";
      if (
        msg.includes("No team registration") ||
        msg.includes("already minted")
      ) {
        setStep("REJECTED");
        setError(msg);
      } else {
        setError(msg);
      }
    } finally {
      setLoading(false);
    }
  };

  if (!mounted)
    return (
      <div className="min-h-screen flex items-center justify-center bg-black">
        <Loader2 className="w-8 h-8 animate-spin text-primary" />
      </div>
    );

  return (
    <div className="min-h-screen text-white selection:bg-primary/30 pt-16 pb-12 px-4 relative overflow-hidden font-sans">
      <div className="max-w-xl mx-auto relative z-10 space-y-8">
        {/* Header */}
        <div className="flex justify-between items-center bg-white/5 backdrop-blur-xl p-4 rounded-3xl border border-white/10 shadow-2xl">
          <Link href="/" className="flex items-center gap-2 group">
            <div className="w-9 h-9 bg-white text-black flex items-center justify-center rounded-xl font-black text-sm group-hover:rotate-12 transition-all">
              JU
            </div>
            <span className="text-xs font-bold tracking-tight uppercase text-neutral-300 group-hover:text-white transition-colors">
              Return Home
            </span>
          </Link>
          {isConnected && (
            <div className="flex items-center gap-3">
              <div className="flex flex-col items-end">
                <span className="text-[10px] font-black text-emerald-400 uppercase tracking-widest leading-none mb-1">
                  Authenticated
                </span>
                <span className="text-[10px] font-mono text-neutral-400">
                  {address?.slice(0, 6)}...{address?.slice(-4)}
                </span>
              </div>
              <Button
                variant="ghost"
                size="icon"
                className="h-9 w-9 rounded-xl hover:bg-red-500/10 hover:text-red-500 group transition-all"
                onClick={() => disconnect()}
              >
                <Unplug className="w-4 h-4 text-neutral-400 group-hover:text-red-500" />
              </Button>
            </div>
          )}
        </div>

        <div className="text-center space-y-4">
          <Badge
            variant="outline"
            className="px-5 py-2 border-primary/30 bg-primary/10 text-white backdrop-blur-md rounded-full text-[10px] font-black tracking-widest uppercase"
          >
            JU ACM // SYNCHRONICITY 2026
          </Badge>
          <h1 className="text-6xl font-black tracking-tighter bg-gradient-to-b from-white via-white to-neutral-500 bg-clip-text text-transparent italic leading-[0.9]">
            INITIALIZE <br /> YOUR <br /> IDENTITY
          </h1>
        </div>

        {/* Step Indicator */}
        <div className="grid grid-cols-4 gap-3 px-2">
          {["Connect", "Register", "Authorize", "Secure"].map((label, i) => {
            const steps: Step[] = ["WALLET", "FORM", "OTP", "SUCCESS"];
            const isActive = steps.indexOf(step) >= i;
            return (
              <div key={label} className="flex flex-col gap-2.5">
                <div
                  className={cn(
                    "h-1.5 rounded-full transition-all duration-700",
                    isActive ? "bg-white" : "bg-neutral-800"
                  )}
                />
                <span
                  className={cn(
                    "text-[10px] font-black uppercase tracking-widest text-center transition-colors",
                    isActive ? "text-white" : "text-neutral-600"
                  )}
                >
                  {label}
                </span>
              </div>
            );
          })}
        </div>

        <Card className="border-white/10 bg-neutral-900/60 backdrop-blur-2xl shadow-2xl relative overflow-hidden group rounded-[2.5rem]">
          <div className="absolute top-0 left-0 w-full h-1.5 bg-gradient-to-r from-transparent via-primary/40 to-transparent" />

          <CardHeader className="pt-10 px-8 text-center sm:text-left">
            <CardTitle className="flex items-center gap-4 text-3xl font-black italic tracking-tight text-white">
              {step === "WALLET" && (
                <>
                  <Wallet className="w-7 h-7" /> INITIALIZE WALLET
                </>
              )}
              {step === "FORM" && (
                <>
                  <ImageIcon className="w-7 h-7" /> TEAM REGISTRATION
                </>
              )}
              {step === "OTP" && (
                <>
                  <KeyRound className="w-7 h-7" /> SECURITY PASS
                </>
              )}
              {step === "SUCCESS" && (
                <>
                  <CheckCircle2 className="w-7 h-7 text-emerald-400" /> IDENTITY
                  SECURED
                </>
              )}
              {step === "REJECTED" && (
                <>
                  <AlertCircle className="w-7 h-7 text-red-500" /> ACCESS DENIED
                </>
              )}
            </CardTitle>
          </CardHeader>

          <CardContent className="pb-10 px-8">
            {error && (
              <div className="mb-6">
                <Alert
                  variant="destructive"
                  className="bg-red-500/10 border-red-500/20 text-red-500 rounded-2xl p-4"
                >
                  <AlertCircle className="h-5 w-5" />
                  <div className="ml-2">
                    <AlertTitle className="text-xs font-black uppercase tracking-widest underline decoration-2 underline-offset-4">
                      Error_Log
                    </AlertTitle>
                    <AlertDescription className="text-[11px] font-bold opacity-90 mt-1">
                      {error}
                    </AlertDescription>
                  </div>
                </Alert>
              </div>
            )}

            {step === "WALLET" && (
              <div className="space-y-4">
                <Button
                  onClick={() => {
                    const connector = connectors[0];
                    if (connector) connect({ connector });
                    else alert("No wallet found. Please install MetaMask!");
                  }}
                  disabled={isConnecting}
                  variant="default"
                  size="lg"
                  className="w-full h-20 text-xl tracking-[0.2em] flex items-center justify-center gap-4 group rounded-3xl font-black bg-white text-black hover:bg-neutral-200"
                >
                  {isConnecting ? (
                    <Loader2 className="h-7 w-7 animate-spin" />
                  ) : (
                    <>
                      ESTABLISH CONNECTION
                      <ArrowRight className="w-6 h-6 group-hover:translate-x-2 transition-transform" />
                    </>
                  )}
                </Button>
              </div>
            )}

            {step === "FORM" && (
              <form onSubmit={handleFormSubmit} className="space-y-8">
                <div className="space-y-5">
                  <div className="group space-y-2.5">
                    <Label className="text-[11px] font-bold uppercase tracking-[0.2em] text-neutral-400">
                      <Users className="w-3.5 h-3.5 inline mr-2" /> Team Name
                    </Label>
                    <Input
                      type="text"
                      placeholder="Enter your team name"
                      required
                      className="h-16 text-lg bg-neutral-950/80 border-white/10 text-white rounded-2xl"
                      value={formData.teamName}
                      onChange={(e) =>
                        setFormData({ ...formData, teamName: e.target.value })
                      }
                    />
                  </div>

                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-5">
                    <div className="group space-y-2.5">
                      <Label className="text-[11px] font-bold uppercase tracking-[0.2em] text-neutral-400">
                        <Mail className="w-3.5 h-3.5 inline mr-2" /> Registered
                        Email
                      </Label>
                      <Input
                        type="email"
                        placeholder="team@example.com"
                        required
                        className="h-16 text-lg bg-neutral-950/80 border-white/10 text-white rounded-2xl"
                        value={formData.email}
                        onChange={(e) =>
                          setFormData({ ...formData, email: e.target.value })
                        }
                      />
                    </div>

                    <div className="group space-y-2.5">
                      <Label className="text-[11px] font-bold uppercase tracking-[0.2em] text-neutral-400">
                        <Phone className="w-3.5 h-3.5 inline mr-2" />{" "}
                        Verification Phone
                      </Label>
                      <Input
                        type="tel"
                        placeholder="+91 00000 00000"
                        required
                        className="h-16 text-lg bg-neutral-950/80 border-white/10 text-white rounded-2xl"
                        value={formData.phone}
                        onChange={(e) =>
                          setFormData({ ...formData, phone: e.target.value.replace(/[^0-9+]/g, "") })
                        }
                      />
                    </div>
                  </div>

                  <div className="group space-y-2.5">
                    <Label className="text-[11px] font-bold uppercase tracking-[0.2em] text-neutral-400 flex justify-between">
                      <span>
                        <ImageIcon className="w-3.5 h-3.5 inline mr-2" /> Team
                        Logo Artifact
                      </span>
                      <span className="text-primary text-[8px]">REQUIRED</span>
                    </Label>
                    <div className="relative group/upload">
                      <input
                        type="file"
                        id="image-upload"
                        className="hidden"
                        accept="image/*"
                        onChange={(e) => {
                          const file = e.target.files?.[0];
                          if (file) {
                            setImageFile(file);
                            setFormData({
                              ...formData,
                              favoriteImageName: file.name,
                            });
                            const reader = new FileReader();
                            reader.onloadend = () => {
                              setImagePreview(reader.result as string);
                            };
                            reader.readAsDataURL(file);
                          }
                        }}
                      />
                      <label
                        htmlFor="image-upload"
                        className="flex flex-col items-center justify-center w-full h-32 border-2 border-dashed border-white/10 rounded-2xl bg-neutral-950/50 hover:bg-neutral-900/50 hover:border-primary/50 cursor-pointer transition-all"
                      >
                        {imagePreview ? (
                          <img
                            src={imagePreview}
                            className="h-full object-contain rounded-xl"
                            alt="Preview"
                          />
                        ) : (
                          <div className="flex flex-col items-center gap-2 text-center px-4">
                            <Upload className="w-6 h-6 text-neutral-600" />
                            <span className="text-[10px] font-black text-neutral-600 uppercase">
                              Select Image Asset
                            </span>
                          </div>
                        )}
                      </label>
                    </div>
                  </div>
                </div>

                <Button
                  type="submit"
                  disabled={loading}
                  className="w-full h-18 rounded-2xl font-black bg-white text-black hover:bg-neutral-200 text-base tracking-widest"
                  size="lg"
                >
                  {loading ? (
                    <Loader2 className="h-6 w-6 animate-spin" />
                  ) : (
                    "SEND SECURITY CODE"
                  )}
                </Button>
              </form>
            )}

            {step === "OTP" && (
              <form
                onSubmit={handleOtpSubmit}
                className="space-y-10 text-center"
              >
                <div className="space-y-5">
                  <div className="inline-block px-4 py-1.5 bg-primary/10 border border-primary/20 rounded-full">
                    <p className="text-[10px] font-black text-neutral-400 uppercase tracking-[0.2em]">
                      Authenticating: {formData.email}
                    </p>
                  </div>
                  <Input
                    placeholder="000 000"
                    maxLength={6}
                    required
                    className="text-center text-5xl h-24 tracking-[0.4em] font-black bg-neutral-950/90 border-white/10 focus:border-primary/50 rounded-3xl shadow-2xl transition-all"
                    value={otp}
                    onChange={(e) => setOtp(e.target.value.replace(/[^0-9]/g, ""))}
                  />
                </div>
                <Button
                  type="submit"
                  disabled={loading}
                  size="lg"
                  className="w-full h-18 rounded-2xl font-black bg-white text-black hover:bg-neutral-200 text-base tracking-widest"
                >
                  {loading ? (
                    <span className="flex items-center gap-2">
                      <Loader2 className="h-6 w-6 animate-spin" />
                      MINTING NFT... DO NOT CLOSE WINDOW
                    </span>
                  ) : (
                    "CONFIRM VERIFICATION"
                  )}
                </Button>
              </form>
            )}

            {step === "SUCCESS" && (
              <div className="text-center space-y-8 py-4">
                <div className="relative mx-auto w-full max-w-[340px] aspect-[3/4] bg-neutral-950 rounded-[2.5rem] border border-white/10 overflow-hidden shadow-2xl">
                  <div className="relative h-full flex flex-col p-8 z-10">
                    <div className="flex justify-between items-start mb-8">
                      <div className="text-left">
                        <p className="text-[10px] font-black text-primary uppercase tracking-widest leading-tight">
                          Official
                        </p>
                        <p className="text-[10px] font-black text-white uppercase tracking-widest leading-tight">
                          Digital Pass
                        </p>
                      </div>
                      <div className="w-10 h-10 bg-white text-black flex items-center justify-center rounded-lg font-black text-xs">
                        JU
                      </div>
                    </div>

                    <div className="flex-1 flex flex-col items-center justify-center gap-6">
                      <div className="w-32 h-32 rounded-3xl overflow-hidden border border-white/20 shadow-2xl relative bg-neutral-900">
                        {imagePreview && (
                          <img
                            src={imagePreview}
                            className="w-full h-full object-cover"
                            alt="Team Logo"
                          />
                        )}
                      </div>
                      <div className="text-center space-y-1">
                        <p className="text-[11px] font-black text-neutral-500 uppercase tracking-[0.3em]">
                          HACKATHON TEAM
                        </p>
                        <h3 className="text-2xl font-black italic tracking-tighter text-white uppercase">
                          {formData.teamName}
                        </h3>
                      </div>
                    </div>
                  </div>
                </div>

                <div className="space-y-4 px-4">
                  <h3 className="text-2xl font-black italic tracking-tighter uppercase text-emerald-400 leading-tight">
                    IDENTITY MATERIALIZED
                  </h3>
                  <p className="text-neutral-400 text-xs font-bold leading-relaxed italic max-w-xs mx-auto">
                    Registration successful. Your NFT has been minted.
                  </p>
                  {regHash && (
                    <a
                      href={`https://sepolia.etherscan.io/tx/${regHash}`}
                      target="_blank"
                      rel="noreferrer"
                      className="block text-[10px] text-neutral-500 hover:text-white underline underline-offset-4 decoration-neutral-700 transition-colors cursor-pointer"
                    >
                      TX: {regHash.slice(0, 10)}...{regHash.slice(-10)}
                    </a>
                  )}
                </div>

                <Link href="/" className="w-full block">
                  <Button className="w-full h-16 rounded-2xl font-black bg-white text-black hover:bg-neutral-200 shadow-xl text-sm tracking-widest uppercase">
                    RETURN TO HOME
                  </Button>
                </Link>
              </div>
            )}

            {step === "REJECTED" && (
              <div className="text-center space-y-10 py-8">
                <div className="w-28 h-28 bg-red-500/10 rounded-[2rem] flex items-center justify-center mx-auto border border-red-500/30">
                  <AlertCircle className="w-14 h-14 text-red-500" />
                </div>
                <div className="space-y-4">
                  <h3 className="text-4xl font-black italic tracking-tighter uppercase text-red-500 leading-[0.85]">
                    ACCESS <br /> DENIED
                  </h3>
                  <p className="text-neutral-400 text-sm font-bold leading-relaxed max-w-xs mx-auto italic">
                    {error || "Identity mismatch detected."}
                  </p>
                </div>
                <Button
                  className="w-full h-16 rounded-2xl bg-white text-black hover:bg-neutral-200 font-black uppercase tracking-widest text-sm"
                  onClick={() => setStep("FORM")}
                >
                  RE-TRY REGISTRATION
                </Button>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
