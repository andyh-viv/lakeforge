import { useEffect, useRef, useState, type ReactNode, type KeyboardEvent } from 'react'

export function Page({ title, actions, children, subtitle }: { title: ReactNode; subtitle?: ReactNode; actions?: ReactNode; children: ReactNode }) {
  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>{title}</h1>
          {subtitle && <div className="muted">{subtitle}</div>}
        </div>
        <div className="actions">{actions}</div>
      </div>
      {children}
    </div>
  )
}

export function Card({ title, children, actions, className }: { title?: ReactNode; actions?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <section className={`card ${className ?? ''}`}>
      {(title || actions) && (
        <header className="card-head">
          <h3>{title}</h3>
          <div className="actions">{actions}</div>
        </header>
      )}
      {children}
    </section>
  )
}

export function Badge({ state, children }: { state?: string; children?: ReactNode }) {
  const s = (state ?? String(children ?? '')).toUpperCase()
  let cls = 'badge'
  if (/RUNNING|SUCCESS|READY|COMPLETED|ACTIVE|FINISHED|HEALTHY|SUCCEEDED|IDLE|TERMINATED\/SUCCESS/.test(s)) cls += ' ok'
  else if (/PENDING|STARTING|QUEUED|RESIZING|RESTARTING|INITIALIZING|WAITING|CANCELLING|NOT_READY|IN_PROGRESS|CREATING|UPDATE_IN_PROGRESS/.test(s)) cls += ' warn'
  else if (/FAIL|ERROR|CANCEL|TIMEDOUT|TIMED_OUT|UNHEALTHY|DELETED/.test(s)) cls += ' err'
  return <span className={cls}>{children ?? state}</span>
}

export function Spinner({ label }: { label?: string }) {
  return (
    <div className="spinner">
      <span className="dot" /> {label ?? 'Loading…'}
    </div>
  )
}

export function ErrorBox({ error }: { error: unknown }) {
  if (!error) return null
  const msg = error instanceof Error ? error.message : String(error)
  return <div className="error-box">{msg}</div>
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="empty">{children}</div>
}

export function Table<T>({ rows, columns, keyOf, onRowClick, emptyText }: {
  rows: T[]
  columns: { key: string; title: ReactNode; render: (row: T) => ReactNode; width?: string }[]
  keyOf: (row: T) => string | number
  onRowClick?: (row: T) => void
  emptyText?: string
}) {
  if (!rows.length) return <Empty>{emptyText ?? 'Nothing here yet.'}</Empty>
  return (
    <div className="table-wrap">
      <table className="table">
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c.key} style={c.width ? { width: c.width } : undefined}>
                {c.title}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={keyOf(r)} className={onRowClick ? 'clickable' : ''} onClick={onRowClick ? () => onRowClick(r) : undefined}>
              {columns.map((c) => (
                <td key={c.key}>{c.render(r)}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function ResultGrid({ columns, rows, truncated }: { columns: string[]; rows: (unknown | null)[][]; truncated?: boolean }) {
  if (!columns.length) return <Empty>Query returned no columns.</Empty>
  return (
    <div className="table-wrap result-grid">
      <table className="table mono">
        <thead>
          <tr>
            <th className="rownum">#</th>
            {columns.map((c, i) => (
              <th key={i}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              <td className="rownum">{i + 1}</td>
              {r.map((v, j) => (
                <td key={j} className={v === null || v === undefined ? 'null' : ''}>
                  {v === null || v === undefined ? 'null' : typeof v === 'object' ? JSON.stringify(v) : String(v)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {truncated && <div className="muted small">Results truncated.</div>}
    </div>
  )
}

export function Modal({ title, onClose, children, footer, wide }: { title: ReactNode; onClose: () => void; children: ReactNode; footer?: ReactNode; wide?: boolean }) {
  useEffect(() => {
    const onKey = (e: globalThis.KeyboardEvent) => e.key === 'Escape' && onClose()
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])
  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className={`modal ${wide ? 'wide' : ''}`} onClick={(e) => e.stopPropagation()}>
        <header>
          <h3>{title}</h3>
          <button className="icon" onClick={onClose} aria-label="Close">
            ×
          </button>
        </header>
        <div className="modal-body">{children}</div>
        {footer && <footer>{footer}</footer>}
      </div>
    </div>
  )
}

export function Field({ label, children, hint }: { label: string; children: ReactNode; hint?: string }) {
  return (
    <label className="field">
      <span>{label}</span>
      {children}
      {hint && <small className="muted">{hint}</small>}
    </label>
  )
}

/** Minimal code editor: textarea with tab / shift-enter handling and line numbers. */
export function CodeEditor({ value, onChange, language, onRun, minRows = 3, readOnly, autoFocus }: {
  value: string
  onChange: (v: string) => void
  language?: string
  onRun?: () => void
  minRows?: number
  readOnly?: boolean
  autoFocus?: boolean
}) {
  const ref = useRef<HTMLTextAreaElement>(null)
  const lines = Math.max(minRows, value.split('\n').length)
  const onKey = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if ((e.shiftKey || e.ctrlKey || e.metaKey) && e.key === 'Enter' && onRun) {
      e.preventDefault()
      onRun()
      return
    }
    if (e.key === 'Tab') {
      e.preventDefault()
      const el = e.currentTarget
      const s = el.selectionStart
      const en = el.selectionEnd
      const next = value.slice(0, s) + '    ' + value.slice(en)
      onChange(next)
      requestAnimationFrame(() => {
        el.selectionStart = el.selectionEnd = s + 4
      })
    }
  }
  useEffect(() => {
    if (autoFocus) ref.current?.focus()
  }, [autoFocus])
  return (
    <div className={`code-editor lang-${language ?? 'text'}`}>
      <div className="gutter">
        {Array.from({ length: lines }, (_, i) => (
          <div key={i}>{i + 1}</div>
        ))}
      </div>
      <textarea
        ref={ref}
        value={value}
        readOnly={readOnly}
        spellCheck={false}
        rows={lines}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={onKey}
      />
    </div>
  )
}

export function Tabs({ tabs, active, onChange }: { tabs: { id: string; label: ReactNode }[]; active: string; onChange: (id: string) => void }) {
  return (
    <div className="tabs">
      {tabs.map((t) => (
        <button key={t.id} className={t.id === active ? 'active' : ''} onClick={() => onChange(t.id)}>
          {t.label}
        </button>
      ))}
    </div>
  )
}

export function KV({ items }: { items: [string, ReactNode][] }) {
  return (
    <dl className="kv">
      {items.map(([k, v]) => (
        <div key={k}>
          <dt>{k}</dt>
          <dd>{v ?? '—'}</dd>
        </div>
      ))}
    </dl>
  )
}

export function useToast() {
  const [msg, setMsg] = useState<{ text: string; kind: 'ok' | 'err' } | null>(null)
  useEffect(() => {
    if (!msg) return
    const t = setTimeout(() => setMsg(null), 4000)
    return () => clearTimeout(t)
  }, [msg])
  const toast = (text: string, kind: 'ok' | 'err' = 'ok') => setMsg({ text, kind })
  const node = msg ? <div className={`toast ${msg.kind}`}>{msg.text}</div> : null
  return { toast, node }
}

export function JsonView({ value }: { value: unknown }) {
  return <pre className="json">{JSON.stringify(value, null, 2)}</pre>
}

export function confirmAction(text: string): boolean {
  return window.confirm(text)
}
