import { NextRequest, NextResponse } from "next/server";
import prisma from "@/lib/db";
import Papa from "papaparse";
import { groupBy } from "lodash";

type CsvRow = {
  "Team ID": string;
  "Candidate role": string;
  "Candidate's Email": string;
  "Candidate's Mobile"?: string;
};

export async function POST(req: NextRequest) {
  try {
    const formData = await req.formData();
    const file = formData.get("csvFile") as File | null;

    if (!file) {
      return NextResponse.json(
        { error: "No CSV file uploaded" },
        { status: 400 }
      );
    }
    const csvText = await file.text();

    const { data, errors } = Papa.parse<CsvRow>(csvText, {
      header: true,
      skipEmptyLines: true,
      transformHeader: (h) => h.trim(),
    });

    if (errors.length) {
      return NextResponse.json(
        { error: "CSV parsing failed", details: errors },
        { status: 400 }
      );
    }
    const grouped = groupBy(data, "Team ID");
    const teamsToCreate = Object.entries(grouped)
      .map(([teamId, members]) => {
        const leader = members.find(
          (m) => m["Candidate role"]?.toLowerCase() === "team leader"
        );

        if (!leader) return null;

        return {
          teamId: teamId.trim(),
          teamLeaderEmail: leader["Candidate's Email"]?.trim(),
          teamLeaderPhone: leader["Candidate's Mobile"]?.trim() ?? null,
        };
      })
      .filter(
        (
          t
        ): t is {
          teamId: string;
          teamLeaderEmail: string;
          teamLeaderPhone: string | null;
        } => Boolean(t?.teamId && t?.teamLeaderEmail)
      );

    if (!teamsToCreate.length) {
      return NextResponse.json(
        { error: "No valid team leaders found in CSV" },
        { status: 400 }
      );
    }
    const result = await prisma.team.createMany({
      data: teamsToCreate,
      skipDuplicates: true,
    });

    return NextResponse.json({
      message: "CSV processed successfully",
      addedCount: result.count,
      totalTeamsDetected: Object.keys(grouped).length,
    });
  } catch (err) {
    console.error("CSV upload error:", err);
    return NextResponse.json(
      { error: "Failed to process CSV" },
      { status: 500 }
    );
  }
}
