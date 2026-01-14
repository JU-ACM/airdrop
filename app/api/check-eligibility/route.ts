import { NextRequest, NextResponse } from 'next/server';
import prisma from '@/lib/db';

export async function POST(req: NextRequest) {
  try {
    const { email } = await req.json();

    if (!email) {
      return NextResponse.json({ error: "Email is required" }, { status: 400 });
    }

    const team = await prisma.team.findFirst({
      where: {
        teamLeaderEmail: email
      }
    });

    if (!team) {
      return NextResponse.json({ error: "Register on Unstop first or if you already registered please be patient and try after sometime" }, { status: 400 });
    }

    if (team.nftMinted) {
       return NextResponse.json({ error: "You already minted your NFT" }, { status: 400 });
    }

    return NextResponse.json({ success: true });
  } catch (error: any) {
    console.error("Eligibility check error:", error);
    return NextResponse.json({ error: "Internal Server Error" }, { status: 500 });
  }
}
