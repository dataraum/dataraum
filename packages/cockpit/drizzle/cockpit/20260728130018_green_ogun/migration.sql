ALTER TABLE "reports" ADD COLUMN "sql_params" jsonb;--> statement-breakpoint
ALTER TABLE "reports" ALTER COLUMN "confidence" DROP NOT NULL;