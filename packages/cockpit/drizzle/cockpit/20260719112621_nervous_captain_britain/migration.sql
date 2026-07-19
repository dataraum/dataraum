-- DAT-819 clean cut: better-auth owns identity from here. The DAT-817
-- placeholder rows (the seeded `default` user and its memberships) retire —
-- real users are minted by better-auth sign-up or the dev seed (registry.ts).
-- Deleted BEFORE the NOT NULL adds below so the cut applies on a warm dev DB.
DELETE FROM "memberships";--> statement-breakpoint
DELETE FROM "users";--> statement-breakpoint
CREATE TABLE "accounts" (
	"id" varchar PRIMARY KEY,
	"account_id" varchar NOT NULL,
	"provider_id" varchar NOT NULL,
	"user_id" varchar NOT NULL,
	"access_token" varchar,
	"refresh_token" varchar,
	"id_token" varchar,
	"access_token_expires_at" timestamp,
	"refresh_token_expires_at" timestamp,
	"scope" varchar,
	"password" varchar,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "sessions" (
	"id" varchar PRIMARY KEY,
	"expires_at" timestamp NOT NULL,
	"token" varchar NOT NULL UNIQUE,
	"ip_address" varchar,
	"user_agent" varchar,
	"user_id" varchar NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "verifications" (
	"id" varchar PRIMARY KEY,
	"identifier" varchar NOT NULL,
	"value" varchar NOT NULL,
	"expires_at" timestamp NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "users" ADD COLUMN "name" varchar NOT NULL;--> statement-breakpoint
ALTER TABLE "users" ADD COLUMN "email" varchar NOT NULL;--> statement-breakpoint
ALTER TABLE "users" ADD COLUMN "email_verified" boolean DEFAULT false NOT NULL;--> statement-breakpoint
ALTER TABLE "users" ADD COLUMN "image" varchar;--> statement-breakpoint
ALTER TABLE "users" ADD COLUMN "updated_at" timestamp DEFAULT now() NOT NULL;--> statement-breakpoint
ALTER TABLE "users" DROP COLUMN "display_name";--> statement-breakpoint
ALTER TABLE "users" ADD CONSTRAINT "users_email_key" UNIQUE("email");--> statement-breakpoint
CREATE INDEX "accounts_user_idx" ON "accounts" ("user_id");--> statement-breakpoint
CREATE INDEX "sessions_user_idx" ON "sessions" ("user_id");--> statement-breakpoint
CREATE INDEX "verifications_identifier_idx" ON "verifications" ("identifier");--> statement-breakpoint
ALTER TABLE "accounts" ADD CONSTRAINT "accounts_user_id_users_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE;--> statement-breakpoint
ALTER TABLE "sessions" ADD CONSTRAINT "sessions_user_id_users_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE;