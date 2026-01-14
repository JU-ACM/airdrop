import { Worker } from "bullmq";
import IORedis from "ioredis";
import { createWalletClient, http, getContract } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { sepolia } from "viem/chains";
import "dotenv/config"; // Load env vars
import prisma from "@/lib/db";
import { ABI } from "@/lib/abi";

const connection = new IORedis(process.env.REDIS_URL || "redis://localhost:6379", {
  maxRetriesPerRequest: null,
});

const pKey = process.env.PRIVATE_KEY?.startsWith("0x") 
  ? process.env.PRIVATE_KEY 
  : `0x${process.env.PRIVATE_KEY}`;

const account = privateKeyToAccount(pKey as `0x${string}`);

const client = createWalletClient({
  account,
  chain: sepolia,
  transport: http(process.env.RPC_URL),
});

const contract = getContract({
  address: process.env.CONTRACT_ADDRESS as `0x${string}`,
  abi: ABI,
  client,
});

export const worker = new Worker(
  "mint-queue",
  async (job) => {
    // The 'imageUri' field from the queue now contains the Metadata URI
    const { teamId, walletAddress, imageUri: tokenUri } = job.data;
    console.log(`Processing mint for team ${teamId} to ${walletAddress} with URI ${tokenUri}`);

    try {
      // Call batchMint with single item array
      const hash = await contract.write.batchMint([
        [walletAddress],
        [tokenUri],
      ]);

      console.log(`Transaction sent: ${hash}`);

      // Update DB
      await prisma.team.update({
        where: { teamId },
        data: {
          nftMinted: true,
        },
      });

      console.log(`Team ${teamId} updated`);
    } catch (error) {
      console.error(`Mint failed for ${teamId}:`, error);
      throw error;
    }
  },
  { connection }
);
