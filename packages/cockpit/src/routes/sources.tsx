import { createFileRoute } from '@tanstack/react-router'
import { useQuery } from '@tanstack/react-query'
import {
  Alert,
  Badge,
  Code,
  Group,
  Loader,
  Stack,
  Table,
  Text,
  Title,
} from '@mantine/core'

import { apiClient } from '../api/client'
import type { components } from '../api/types'

type Source = components['schemas']['Source']

export const Route = createFileRoute('/sources')({ component: Sources })

function Sources() {
  const { data, isLoading, error } = useQuery({
    queryKey: ['sources'],
    queryFn: async (): Promise<Source[]> => {
      const { data, error } = await apiClient.GET('/api/sources')
      if (error) {
        throw new Error(`Failed to fetch sources: ${JSON.stringify(error)}`)
      }
      return data ?? []
    },
  })

  return (
    <Stack p="xl" gap="md">
      <Title order={1}>Sources</Title>
      <Text c="dimmed">Registered in the workspace via the engine REST surface.</Text>

      {isLoading && (
        <Group gap="sm">
          <Loader size="sm" />
          <Text size="sm" c="dimmed">
            Loading…
          </Text>
        </Group>
      )}

      {error && (
        <Alert color="red" title="Could not load sources">
          {error instanceof Error ? error.message : String(error)}
        </Alert>
      )}

      {data && data.length === 0 && (
        <Text c="dimmed" fs="italic">
          No sources registered yet. Use the engine to add one — `add_source` lands here in a later
          step.
        </Text>
      )}

      {data && data.length > 0 && (
        <Table withTableBorder withColumnBorders striped highlightOnHover>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Name</Table.Th>
              <Table.Th>Type</Table.Th>
              <Table.Th>Status</Table.Th>
              <Table.Th>Path</Table.Th>
              <Table.Th>Backend</Table.Th>
              <Table.Th>Recipe tables</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {data.map((source) => (
              <Table.Tr key={source.name}>
                <Table.Td>
                  <Code>{source.name}</Code>
                </Table.Td>
                <Table.Td>
                  <Badge variant="light">{source.type}</Badge>
                </Table.Td>
                <Table.Td>{source.status}</Table.Td>
                <Table.Td>
                  {source.path ? (
                    <Code style={{ fontSize: '0.75rem' }}>{source.path}</Code>
                  ) : (
                    <Text c="dimmed">—</Text>
                  )}
                </Table.Td>
                <Table.Td>
                  {source.backend ? <Badge color="grape">{source.backend}</Badge> : <Text c="dimmed">—</Text>}
                </Table.Td>
                <Table.Td>
                  {source.recipe_tables && source.recipe_tables.length > 0 ? (
                    <Group gap={4} wrap="wrap">
                      {source.recipe_tables.map((t) => (
                        <Code key={t}>{t}</Code>
                      ))}
                    </Group>
                  ) : (
                    <Text c="dimmed">—</Text>
                  )}
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
    </Stack>
  )
}
