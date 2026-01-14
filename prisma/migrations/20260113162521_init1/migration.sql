-- AlterTable
ALTER TABLE "team" ADD COLUMN     "nftMinted" BOOLEAN NOT NULL DEFAULT false,
ADD COLUMN     "walletAddress" TEXT;
