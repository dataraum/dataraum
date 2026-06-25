CREATE TABLE "reports" (
	"id" varchar PRIMARY KEY,
	"workspace_id" varchar NOT NULL,
	"conversation_id" varchar,
	"message_id" varchar,
	"parent_id" varchar,
	"title" varchar NOT NULL,
	"summary" text NOT NULL,
	"summary_fingerprint" varchar,
	"sql" text NOT NULL,
	"chart_config" jsonb,
	"confidence" jsonb NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"deleted_at" timestamp
);
--> statement-breakpoint
CREATE INDEX "reports_workspace_idx" ON "reports" ("workspace_id","created_at");--> statement-breakpoint
ALTER TABLE "reports" ADD CONSTRAINT "reports_workspace_id_workspaces_id_fkey" FOREIGN KEY ("workspace_id") REFERENCES "workspaces"("id");--> statement-breakpoint
ALTER TABLE "reports" ADD CONSTRAINT "reports_conversation_id_conversations_id_fkey" FOREIGN KEY ("conversation_id") REFERENCES "conversations"("id");--> statement-breakpoint
ALTER TABLE "reports" ADD CONSTRAINT "reports_parent_id_reports_id_fkey" FOREIGN KEY ("parent_id") REFERENCES "reports"("id");