ALTER TABLE "conversation_messages" DROP CONSTRAINT "conversation_messages_pkey";--> statement-breakpoint
ALTER TABLE "conversation_messages" ADD PRIMARY KEY ("conversation_id","id");