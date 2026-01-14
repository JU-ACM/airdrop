import { NextRequest, NextResponse } from "next/server";
import { auth } from "@/lib/auth"; // server auth
import { uploadToPinata, uploadToPinataJSON } from "@/lib/pinata";
import prisma from "@/lib/db";
import { mintQueue } from "@/lib/queue";
import { headers } from "next/headers";

export async function POST(req: NextRequest) {
  try {
    const session = await auth.api.getSession({
        headers: await headers()
    });

    if (!session) {
        return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const formData = await req.formData();
    const phone = formData.get("phone") as string;
    const walletAddress = formData.get("walletAddress") as string;
    const file = formData.get("image") as File;

    if (!phone || !walletAddress || !file) {
      return NextResponse.json({ error: "Missing fields" }, { status: 400 });
    }

    // 1. Check if user is in Team table (using session email)
    const team = await prisma.team.findFirst({
        where: {
            teamLeaderEmail: session.user.email
        }
    });

    if (!team) {
        return NextResponse.json({ error: "No team registration found for this email" }, { status: 403 });
    }

    if (team.nftMinted) {
        return NextResponse.json({ error: "NFT already minted" }, { status: 400 });
    }

    // 2. Upload Image
    const imageUri = await uploadToPinata(file);

    // 3. Create & Upload Metadata
    const metadata = {
        name: `JU Airdrop Team ${team.teamId}`,
        description: "JU ACM Synchronicity 2026 Airdrop",
        image: imageUri,
        attributes: [
            { trait_type: "Team ID", value: team.teamId },
            { trait_type: "Event", value: "Synchronicity 2026" }
        ]
    };

    const metadataUri = await uploadToPinataJSON(metadata);

    // 4. Update Team
    await prisma.team.update({
        where: { id: team.id },
        data: {
            teamLeaderPhone: phone,
            walletAddress: walletAddress
        }
    });

    // 5. Add to Queue
    await mintQueue.add("mint", {
        teamId: team.teamId,
        walletAddress,
        imageUri: metadataUri // Passing Metadata URI as the main URI for minting
    });

    return NextResponse.json({ success: true, message: "Claim submitted, minting in progress" });

  } catch (error) {
    console.error("Claim error:", error);
    return NextResponse.json({ error: "Internal Server Error" }, { status: 500 });
  }
}
