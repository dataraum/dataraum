import Anthropic from '@anthropic-ai/sdk'
import { createFileRoute } from '@tanstack/react-router'

const MODEL = 'claude-sonnet-4-6'
const MAX_TOKENS = 1024

const ENGINE_URL =
  process.env.ENGINE_API_URL ?? process.env.VITE_ENGINE_API_URL ?? 'http://localhost:8000'

const tools: Array<Anthropic.Messages.Tool> = [
  {
    name: 'list_sources',
    description:
      'List all data sources registered in the DataRaum workspace. Returns name, type, status, and path for each source. No arguments.',
    input_schema: { type: 'object', properties: {}, required: [] },
  },
]

async function runTool(name: string, _input: unknown): Promise<unknown> {
  if (name === 'list_sources') {
    const res = await fetch(`${ENGINE_URL}/api/sources`)
    if (!res.ok) {
      return { error: `engine returned ${res.status} ${res.statusText}` }
    }
    return await res.json()
  }
  return { error: `unknown tool: ${name}` }
}

type ChatRequest = {
  messages: Array<{ role: 'user' | 'assistant'; content: string }>
}

export const Route = createFileRoute('/api/chat')({
  server: {
    handlers: {
      POST: async ({ request }: { request: Request }) => {
    const apiKey = process.env.ANTHROPIC_API_KEY
    if (!apiKey) {
      return new Response(JSON.stringify({ error: 'ANTHROPIC_API_KEY not set' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    const body = (await request.json()) as ChatRequest
    if (!body.messages?.length) {
      return new Response(JSON.stringify({ error: 'messages required' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    const anthropic = new Anthropic({ apiKey })

    const encoder = new TextEncoder()
    const stream = new ReadableStream({
      async start(controller) {
        const send = (event: string, data: unknown) => {
          controller.enqueue(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`))
        }

        const conversation: Array<Anthropic.Messages.MessageParam> = body.messages.map((m) => ({
          role: m.role,
          content: m.content,
        }))

        try {
          // Agentic loop: stream → if tool_use, run → feed back → repeat.
          // Cap at 5 rounds so a runaway model can't lock the stream.
          for (let round = 0; round < 5; round++) {
            let stopReason: string | null = null
            const assistantBlocks: Array<Anthropic.Messages.ContentBlock> = []
            const streamResp = anthropic.messages.stream({
              model: MODEL,
              max_tokens: MAX_TOKENS,
              tools,
              messages: conversation,
            })

            for await (const event of streamResp) {
              if (event.type === 'content_block_start') {
                if (event.content_block.type === 'tool_use') {
                  send('tool_call_start', {
                    id: event.content_block.id,
                    name: event.content_block.name,
                  })
                }
              } else if (event.type === 'content_block_delta') {
                if (event.delta.type === 'text_delta') {
                  send('text', { text: event.delta.text })
                }
              } else if (event.type === 'message_delta') {
                if (event.delta.stop_reason) stopReason = event.delta.stop_reason
              }
            }

            const final = await streamResp.finalMessage()
            assistantBlocks.push(...final.content)
            conversation.push({ role: 'assistant', content: final.content })

            if (stopReason !== 'tool_use') {
              send('done', { stop_reason: stopReason ?? 'end_turn' })
              break
            }

            // Run each tool_use block, push tool_results back.
            const toolResults: Array<Anthropic.Messages.ToolResultBlockParam> = []
            for (const block of assistantBlocks) {
              if (block.type === 'tool_use') {
                const result = await runTool(block.name, block.input)
                send('tool_result', { id: block.id, name: block.name, result })
                toolResults.push({
                  type: 'tool_result',
                  tool_use_id: block.id,
                  content: JSON.stringify(result),
                })
              }
            }
            conversation.push({ role: 'user', content: toolResults })
          }
        } catch (err) {
          send('error', { message: err instanceof Error ? err.message : String(err) })
        } finally {
          controller.close()
        }
      },
    })

        return new Response(stream, {
          headers: {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache, no-transform',
            Connection: 'keep-alive',
          },
        })
      },
    },
  },
})
