CREATE TABLE "runs" (
	"id" varchar PRIMARY KEY,
	"workspace_id" varchar NOT NULL,
	"kind" varchar NOT NULL,
	"stage" varchar NOT NULL,
	"workflow_id" varchar NOT NULL,
	"run_id" varchar NOT NULL,
	"conversation_id" varchar,
	"status" varchar DEFAULT 'running' NOT NULL,
	"started_at" timestamp DEFAULT now() NOT NULL,
	"completion_narrated_at" timestamp,
	"awaiting_note" text
);
--> statement-breakpoint
ALTER TABLE "session_runs" DROP CONSTRAINT "session_runs_session_id_sessions_id_fkey";--> statement-breakpoint
DROP TABLE "session_runs";--> statement-breakpoint
DROP TABLE "sessions";--> statement-breakpoint
CREATE UNIQUE INDEX "runs_workflow_run_uq" ON "runs" ("workflow_id","run_id");--> statement-breakpoint
CREATE INDEX "runs_workspace_idx" ON "runs" ("workspace_id");--> statement-breakpoint
CREATE INDEX "runs_conversation_idx" ON "runs" ("conversation_id");--> statement-breakpoint
ALTER TABLE "runs" ADD CONSTRAINT "runs_workspace_id_workspaces_id_fkey" FOREIGN KEY ("workspace_id") REFERENCES "workspaces"("id");--> statement-breakpoint
ALTER TABLE "runs" ADD CONSTRAINT "runs_conversation_id_conversations_id_fkey" FOREIGN KEY ("conversation_id") REFERENCES "conversations"("id");