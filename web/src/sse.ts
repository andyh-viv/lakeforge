import { getToken } from './api'

/** Consume a server-sent-events endpoint with an Authorization header (EventSource cannot set headers). */
export async function streamEvents<T>(path: string, onEvent: (ev: T) => void, signal?: AbortSignal): Promise<void> {
  const headers: Record<string, string> = { Accept: 'text/event-stream' }
  const tok = getToken()
  if (tok) headers['Authorization'] = `Bearer ${tok}`
  const res = await fetch(path, { headers, signal })
  if (!res.ok || !res.body) throw new Error(`stream failed: ${res.status}`)
  const reader = res.body.getReader()
  const dec = new TextDecoder()
  let buf = ''
  for (;;) {
    const { value, done } = await reader.read()
    if (done) break
    buf += dec.decode(value, { stream: true })
    let idx: number
    while ((idx = buf.indexOf('\n\n')) >= 0) {
      const chunk = buf.slice(0, idx)
      buf = buf.slice(idx + 2)
      const data = chunk
        .split('\n')
        .filter((l) => l.startsWith('data:'))
        .map((l) => l.slice(5).trimStart())
        .join('\n')
      if (!data) continue
      try {
        onEvent(JSON.parse(data) as T)
      } catch {
        /* ignore malformed frame */
      }
    }
  }
}
