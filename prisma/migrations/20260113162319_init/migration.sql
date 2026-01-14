-- CreateTable
CREATE TABLE "team" (
    "id" TEXT NOT NULL,
    "teamId" TEXT NOT NULL,
    "teamLeaderEmail" TEXT NOT NULL,
    "teamLeaderPhone" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "team_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "team_teamId_key" ON "team"("teamId");
