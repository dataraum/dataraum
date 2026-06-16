--> DAT-528: `kind` is NOT NULL, but pre-typed conversations exist in dev. Add
--> nullable, backfill them to `analyse` (the general/ungated kind — the safest
--> default for an existing free-form transcript), then enforce NOT NULL. New rows
--> always supply `kind` (createConversation), so the default is transitional only.
ALTER TABLE "conversations" ADD COLUMN "kind" varchar;--> statement-breakpoint
UPDATE "conversations" SET "kind" = 'analyse' WHERE "kind" IS NULL;--> statement-breakpoint
ALTER TABLE "conversations" ALTER COLUMN "kind" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "conversations" ADD COLUMN "title" varchar;--> statement-breakpoint
ALTER TABLE "conversations" ADD COLUMN "last_active_at" timestamp DEFAULT now() NOT NULL;--> statement-breakpoint
ALTER TABLE "session_runs" ADD COLUMN "conversation_id" varchar;--> statement-breakpoint
CREATE INDEX "session_runs_conversation_idx" ON "session_runs" ("conversation_id");--> statement-breakpoint
ALTER TABLE "session_runs" ADD CONSTRAINT "session_runs_conversation_id_conversations_id_fkey" FOREIGN KEY ("conversation_id") REFERENCES "conversations"("id");
