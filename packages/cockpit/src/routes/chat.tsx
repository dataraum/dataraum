import { useRef, useState } from 'react'
import { createFileRoute } from '@tanstack/react-router'
import { Alert, Badge, Button, Code, Group, Stack, Text, Textarea, Title } from '@mantine/core'

type Turn =
  | { role: 'user'; text: string }
  | { role: 'assistant'; text: string; toolCalls: ToolCall[] }

type ToolCall = {
  id: string
  name: string
  result?: unknown
}

export const Route = createFileRoute('/chat')({ component: Chat })

function Chat() {
  const [turns, setTurns] = useState<Turn[]>([])
  const [input, setInput] = useState('')
  const [streaming, setStreaming] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const abortRef = useRef<AbortController | null>(null)

  async function send() {
    const text = input.trim()
    if (!text || streaming) return

    setError(null)
    setInput('')
    const userTurn: Turn = { role: 'user', text }
    const assistantTurn: Turn = { role: 'assistant', text: '', toolCalls: [] }
    setTurns((prev) => [...prev, userTurn, assistantTurn])
    setStreaming(true)

    // Bridge stream events into the latest assistant turn — immutable so it's
    // safe under React strict-mode's double-invoke of state updaters.
    const updateAssistant = (
      transform: (turn: Turn & { role: 'assistant' }) => Turn & { role: 'assistant' },
    ) => {
      setTurns((prev) => {
        const last = prev[prev.length - 1]
        if (!last || last.role !== 'assistant') return prev
        return [...prev.slice(0, -1), transform(last)]
      })
    }

    const controller = new AbortController()
    abortRef.current = controller

    try {
      const messagesForApi = [...turns, userTurn].map((t) => ({
        role: t.role,
        content: t.text,
      }))

      const res = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ messages: messagesForApi }),
        signal: controller.signal,
      })

      if (!res.ok || !res.body) {
        const errBody = await res.text().catch(() => '')
        throw new Error(`HTTP ${res.status} ${res.statusText}${errBody ? `: ${errBody}` : ''}`)
      }

      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { value, done } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })

        // Parse SSE frames: blank-line separated event blocks.
        let idx: number
        while ((idx = buffer.indexOf('\n\n')) !== -1) {
          const frame = buffer.slice(0, idx)
          buffer = buffer.slice(idx + 2)

          let eventName = 'message'
          let dataLine = ''
          for (const line of frame.split('\n')) {
            if (line.startsWith('event: ')) eventName = line.slice(7).trim()
            else if (line.startsWith('data: ')) dataLine = line.slice(6)
          }
          if (!dataLine) continue

          let payload: any
          try {
            payload = JSON.parse(dataLine)
          } catch {
            continue
          }

          if (eventName === 'text') {
            updateAssistant((t) => ({ ...t, text: t.text + payload.text }))
          } else if (eventName === 'tool_call_start') {
            updateAssistant((t) =>
              t.toolCalls.some((c) => c.id === payload.id)
                ? t
                : { ...t, toolCalls: [...t.toolCalls, { id: payload.id, name: payload.name }] },
            )
          } else if (eventName === 'tool_result') {
            updateAssistant((t) => ({
              ...t,
              toolCalls: t.toolCalls.map((c) =>
                c.id === payload.id ? { ...c, result: payload.result } : c,
              ),
            }))
          } else if (eventName === 'error') {
            setError(payload.message ?? 'stream error')
          }
        }
      }
    } catch (err) {
      if ((err as Error).name !== 'AbortError') {
        setError(err instanceof Error ? err.message : String(err))
      }
    } finally {
      setStreaming(false)
      abortRef.current = null
    }
  }

  return (
    <Stack p="xl" gap="md" style={{ maxWidth: 800 }}>
      <Title order={1}>Chat</Title>
      <Text c="dimmed">
        First end-to-end agent turn. One tool: <Code>list_sources</Code>.
      </Text>

      {error && (
        <Alert color="red" title="Error">
          {error}
        </Alert>
      )}

      <Stack gap="md" data-testid="chat-log">
        {turns.map((turn, i) => (
          <Stack
            key={i}
            gap={6}
            p="md"
            style={{
              borderRadius: 6,
              border: '1px solid var(--mantine-color-gray-3)',
              background:
                turn.role === 'user' ? 'var(--mantine-color-gray-0)' : 'transparent',
            }}
            data-testid={`turn-${turn.role}`}
          >
            <Badge variant="light" color={turn.role === 'user' ? 'gray' : 'blue'} size="sm">
              {turn.role}
            </Badge>
            <Text style={{ whiteSpace: 'pre-wrap' }}>{turn.text || (streaming ? '…' : '')}</Text>
            {turn.role === 'assistant' &&
              turn.toolCalls.map((call) => (
                <Stack
                  key={call.id}
                  gap={4}
                  p="xs"
                  style={{
                    borderRadius: 4,
                    background: 'var(--mantine-color-gray-1)',
                  }}
                  data-testid="tool-call"
                >
                  <Group gap="xs">
                    <Badge size="xs" color="grape">
                      tool
                    </Badge>
                    <Code>{call.name}</Code>
                  </Group>
                  {call.result !== undefined && (
                    <Code block style={{ fontSize: '0.7rem', maxHeight: 200, overflow: 'auto' }}>
                      {JSON.stringify(call.result, null, 2)}
                    </Code>
                  )}
                </Stack>
              ))}
          </Stack>
        ))}
      </Stack>

      <Stack gap="xs">
        <Textarea
          placeholder="Ask something — e.g. 'What sources are registered?'"
          minRows={2}
          autosize
          value={input}
          onChange={(e) => setInput(e.currentTarget.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
              e.preventDefault()
              void send()
            }
          }}
          disabled={streaming}
          data-testid="chat-input"
        />
        <Group justify="flex-end">
          <Button onClick={() => void send()} loading={streaming} data-testid="chat-send">
            Send
          </Button>
        </Group>
      </Stack>
    </Stack>
  )
}
