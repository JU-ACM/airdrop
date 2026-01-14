"use client";

import * as React from "react";
import { signIn } from "@/lib/auth-client";
import { Loader2 } from "lucide-react";

export default function SignIn() {
  const [email, setEmail] = React.useState("");
  const [password, setPassword] = React.useState("");
  const [error, setError] = React.useState("");
  const [loading, setLoading] = React.useState(false);

  return (
    <div className="min-h-screen flex items-center justify-center">
      <div className="relative z-10 w-full max-w-sm rounded-3xl shadow-2xl p-8 flex flex-col items-center border border-white/20">
        <div className="flex items-center justify-center w-12 h-12 rounded-full  mb-6 shadow-lg border border-white/20">
          <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRy6ob7-Syw1CeNDSzy6u2EzQ9HLk0eCd6_aQ&s" />
        </div>
        <h2 className="text-2xl font-semibold  mb-6 text-center">
          SYNCHRONICITY
        </h2>
        <div className="flex flex-col w-full gap-4">
          <div className="w-full flex flex-col gap-3">
            <input
              placeholder="Email"
              type="email"
              value={email}
              className="w-full px-5 py-3 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-gray-400 border border-gray-300"
              onChange={(e) => setEmail(e.target.value)}
            />
            <input
              placeholder="Password"
              type="password"
              value={password}
              className="w-full px-5 py-3 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-gray-400 border border-gray-300"
              onChange={(e) => setPassword(e.target.value)}
            />
            {error && <div className="text-sm text-left">{error}</div>}
          </div>
          <hr className="opacity-10" />
          <div>
            <button
              className="w-full font-medium px-5 py-3 rounded-full shadow transition mb-3 text-sm border border-gray-300"
              onClick={async () => {
                await signIn.email(
                  {
                    email,
                    password,
                    callbackURL: "/admin",
                  },
                  {
                    onRequest: (ctx) => {
                      setLoading(true);
                    },
                    onResponse: (ctx) => {
                      setLoading(false);
                    },
                  }
                );
              }}
            >
              {loading ? (
                <Loader2 size={16} className="animate-spin" />
              ) : (
                <p> Login </p>
              )}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
