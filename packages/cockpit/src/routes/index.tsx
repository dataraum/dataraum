import { createFileRoute } from '@tanstack/react-router'
import { Stack, Text, Title } from '@mantine/core'

export const Route = createFileRoute('/')({ component: Home })

function Home() {
  return (
    <Stack p="xl" gap="md">
      <Title order={1}>DataRaum Cockpit</Title>
      <Text c="dimmed">
        Scaffold ready. Engine REST integration lands in step 4.
      </Text>
    </Stack>
  )
}
