
import { PrismaClient } from '@prisma/client';
import { PrismaPg } from '@prisma/adapter-pg';
import { Pool } from 'pg';
import "dotenv/config";

const connectionString = process.env.DATABASE_URL;

async function main() {
  console.log("Initializing PrismaClient with PG adapter...");
  
  const pool = new Pool({ connectionString });
  const adapter = new PrismaPg(pool);
  const prisma = new PrismaClient({ adapter });

  console.log("Prisma Client initialized.");

  try {
      // Test Connection
      console.log("Testing connection via $queryRaw...");
      const now = await prisma.$queryRaw`SELECT NOW()`;
      console.log("Connection Success:", now);
  } catch(e:any) {
      console.error("Connection Failed:", e.message);
  }

  if (prisma.verification) {
    try {
        console.log("Attempting delete check...");
        await prisma.verification.delete({
            where: {
                identifier: "check-pg-adapter"
            }
        });
    } catch (e: any) {
        if (e.code === 'P2025') {
            console.log("SUCCESS: Record not found (as expected).");
        } else {
             console.log("Delete error:", e.message);
        }
    }
  }
  
  await prisma.$disconnect();
  await pool.end();
}

main()
  .catch(e => {
    console.error(e);
  });
