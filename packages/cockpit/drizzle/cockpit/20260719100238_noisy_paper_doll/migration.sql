CREATE TABLE "memberships" (
	"user_id" varchar,
	"workspace_id" varchar,
	"role" varchar DEFAULT 'member' NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "memberships_pkey" PRIMARY KEY("user_id","workspace_id")
);
--> statement-breakpoint
CREATE TABLE "users" (
	"id" varchar PRIMARY KEY,
	"display_name" varchar NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
DROP TABLE "actors";--> statement-breakpoint
ALTER TABLE "workspaces" ADD COLUMN "state" varchar DEFAULT 'ready' NOT NULL;--> statement-breakpoint
ALTER TABLE "workspaces" ADD COLUMN "subdomain" varchar;--> statement-breakpoint
ALTER TABLE "workspaces" ADD COLUMN "reader_role" varchar;--> statement-breakpoint
ALTER TABLE "workspaces" ADD COLUMN "writer_role" varchar;--> statement-breakpoint
ALTER TABLE "workspaces" ADD COLUMN "catalog_schema" varchar;--> statement-breakpoint
ALTER TABLE "workspaces" DROP COLUMN "archived_at";--> statement-breakpoint
ALTER TABLE "memberships" ADD CONSTRAINT "memberships_user_id_users_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users"("id");--> statement-breakpoint
ALTER TABLE "memberships" ADD CONSTRAINT "memberships_workspace_id_workspaces_id_fkey" FOREIGN KEY ("workspace_id") REFERENCES "workspaces"("id");