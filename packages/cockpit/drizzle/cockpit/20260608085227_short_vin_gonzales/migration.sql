CREATE TABLE "conversation_messages" (
	"id" varchar PRIMARY KEY,
	"conversation_id" varchar NOT NULL,
	"seq" integer NOT NULL,
	"role" varchar NOT NULL,
	"message" jsonb NOT NULL,
	"model_only" boolean DEFAULT false NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "conversations" (
	"id" varchar PRIMARY KEY,
	"workspace_id" varchar NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "ui_state" (
	"conversation_id" varchar PRIMARY KEY,
	"pinned_call_id" varchar,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE INDEX "conversation_messages_conversation_idx" ON "conversation_messages" ("conversation_id","seq");--> statement-breakpoint
CREATE INDEX "conversations_workspace_idx" ON "conversations" ("workspace_id");--> statement-breakpoint
ALTER TABLE "conversation_messages" ADD CONSTRAINT "conversation_messages_conversation_id_conversations_id_fkey" FOREIGN KEY ("conversation_id") REFERENCES "conversations"("id");--> statement-breakpoint
ALTER TABLE "conversations" ADD CONSTRAINT "conversations_workspace_id_workspaces_id_fkey" FOREIGN KEY ("workspace_id") REFERENCES "workspaces"("id");--> statement-breakpoint
ALTER TABLE "ui_state" ADD CONSTRAINT "ui_state_conversation_id_conversations_id_fkey" FOREIGN KEY ("conversation_id") REFERENCES "conversations"("id");