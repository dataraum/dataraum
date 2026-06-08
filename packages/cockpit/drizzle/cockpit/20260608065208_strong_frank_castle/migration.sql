CREATE TABLE "actors" (
	"id" varchar PRIMARY KEY,
	"display_name" varchar NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "session_runs" (
	"id" varchar PRIMARY KEY,
	"session_id" varchar NOT NULL,
	"stage" varchar NOT NULL,
	"workflow_id" varchar NOT NULL,
	"run_id" varchar NOT NULL,
	"status" varchar DEFAULT 'running' NOT NULL,
	"started_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "sessions" (
	"id" varchar PRIMARY KEY,
	"workspace_id" varchar NOT NULL,
	"engine_session_id" varchar NOT NULL,
	"kind" varchar NOT NULL,
	"status" varchar DEFAULT 'active' NOT NULL,
	"created_by" varchar NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"ended_at" timestamp
);
--> statement-breakpoint
CREATE TABLE "workspaces" (
	"id" varchar PRIMARY KEY,
	"name" varchar NOT NULL,
	"engine_schema" varchar NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"archived_at" timestamp
);
--> statement-breakpoint
CREATE UNIQUE INDEX "session_runs_workflow_run_uq" ON "session_runs" ("workflow_id","run_id");--> statement-breakpoint
CREATE INDEX "session_runs_session_idx" ON "session_runs" ("session_id");--> statement-breakpoint
CREATE UNIQUE INDEX "sessions_engine_session_uq" ON "sessions" ("engine_session_id");--> statement-breakpoint
CREATE INDEX "sessions_workspace_idx" ON "sessions" ("workspace_id");--> statement-breakpoint
ALTER TABLE "session_runs" ADD CONSTRAINT "session_runs_session_id_sessions_id_fkey" FOREIGN KEY ("session_id") REFERENCES "sessions"("id");--> statement-breakpoint
ALTER TABLE "sessions" ADD CONSTRAINT "sessions_workspace_id_workspaces_id_fkey" FOREIGN KEY ("workspace_id") REFERENCES "workspaces"("id");--> statement-breakpoint
ALTER TABLE "sessions" ADD CONSTRAINT "sessions_created_by_actors_id_fkey" FOREIGN KEY ("created_by") REFERENCES "actors"("id");