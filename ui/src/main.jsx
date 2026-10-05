import React, { useEffect, useState } from 'react'
import { createRoot } from 'react-dom/client'
import {
  Activity,
  CheckCircle2,
  Crown,
  Database,
  Eye,
  KeyRound,
  Moon,
  Radio,
  RefreshCw,
  Server,
  ShieldCheck,
  Sun,
  Trash2,
  Zap,
} from 'lucide-react'
import './styles.css'

const api = async (path, options) => {
  const r = await fetch(path, options)
  const data = await r.json()
  if (!r.ok) throw new Error(data.error || 'Request failed')
  return data
}

function App() {
  const [status, setStatus] = useState(null)
  const [keys, setKeys] = useState([])
  const [selected, setSelected] = useState(null)
  const [theme, setTheme] = useState(() => localStorage.getItem('raftkv-theme') || 'dark')
  const [key, setKey] = useState('')
  const [value, setValue] = useState('')
  const [message, setMessage] = useState('')
  const [loading, setLoading] = useState(true)

  const refresh = async () => {
    try {
      const [s, k] = await Promise.all([api('/api/status'), api('/api/keys')])
      setStatus(s)
      setKeys(k.keys.map((x) => new TextDecoder().decode(Uint8Array.from(x))))
      setMessage('Cluster synced')
    } catch (e) {
      setMessage(e.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    refresh()
    const t = setInterval(refresh, 3500)
    return () => clearInterval(t)
  }, [])

  useEffect(() => {
    document.documentElement.dataset.theme = theme
    localStorage.setItem('raftkv-theme', theme)
  }, [theme])

  const save = async () => {
    if (!key.trim()) return
    try {
      await api(`/api/keys/${encodeURIComponent(key)}`, {
        method: 'PUT',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ value }),
      })
      setMessage(`Committed key: ${key}`)
      setKey('')
      setValue('')
      refresh()
    } catch (e) {
      setMessage(e.message)
    }
  }

  const remove = async (k) => {
    try {
      await api(`/api/keys/${encodeURIComponent(k)}`, { method: 'DELETE' })
      setMessage(`Deleted key: ${k}`)
      refresh()
    } catch (e) {
      setMessage(e.message)
    }
  }

  const inspect = async (k) => {
    try {
      const d = await api(`/api/keys/${encodeURIComponent(k)}`)
      setSelected({ key: k, value: d.value ?? 'null' })
    } catch (e) {
      setMessage(e.message)
    }
  }

  const role = status?.role || 'offline'

  return (
    <main>
      <header>
        <div className="brand">
          <div className="logo-frame">
            <img src="/logo.png" alt="RaftKV Logo" className="brand-logo-img" />
          </div>
          <div className="brand-info">
            <div className="brand-name">
              Raft<span className="brand-kv">KV</span>
            </div>
            <span className="brand-tagline">CONSENSUS • CONSISTENCY • SCALE</span>
          </div>
        </div>

        <div className="header-actions">
          <div className="connection-badge">
            <i className={status ? 'online' : 'offline'} />
            <span>{status ? `NODE ${status.node_id} CONNECTED` : 'DISCONNECTED'}</span>
          </div>
          <button className="icon-action-btn" onClick={refresh} aria-label="Refresh status" title="Refresh">
            <RefreshCw size={15} />
          </button>
          <button
            className="theme-toggle"
            onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
            aria-label="Toggle light and dark mode"
            title={theme === 'dark' ? 'Switch to Light (White) Mode' : 'Switch to Dark Mode'}
          >
            {theme === 'dark' ? <Sun size={16} /> : <Moon size={16} />}
            <span className="theme-text">{theme === 'dark' ? 'Light' : 'Dark'}</span>
          </button>
        </div>
      </header>

      <section className="hero">
        <div className="hero-left">
          <div className="eyebrow-container">
            <span className="hero-chip">DISTRIBUTED KEY-VALUE STORE</span>
            <span className="hero-chip-node">NODE #{status?.node_id || '—'}</span>
          </div>
          <h1>
            Consensus, <em className="hero-highlight">visible.</em>
          </h1>
          <p className="sub">
            A high-performance Raft-replicated state machine with RESP2/RESP3 Redis wire protocol compatibility and durable Sled storage.
          </p>
        </div>

        <div className="hero-card">
          <div className="hero-card-header">
            <Radio size={14} className="pulse-icon" />
            <span>CLUSTER STATE</span>
          </div>
          <div className={`role-badge ${role === 'leader' ? 'is-leader' : ''}`}>
            {role === 'leader' ? <Crown size={19} className="crown-glow" /> : <Server size={17} />}
            <span className="role-text">{role.toUpperCase()}</span>
          </div>
          <div className="hero-meta">
            <div>
              <span>Current Term</span>
              <strong>{status?.term ?? '—'}</strong>
            </div>
            <div>
              <span>Leader ID</span>
              <strong>{status?.leader_id ? `Node ${status.leader_id}` : 'None'}</strong>
            </div>
          </div>
          <div className="hero-log-msg">{message || 'Connected to Raft consensus'}</div>
        </div>
      </section>

      <section className="stats">
        <Stat
          icon={<Server />}
          label="Cluster Nodes"
          value={status?.nodes?.length ?? '—'}
          detail="Raft voting quorum"
        />
        <Stat
          icon={<Database />}
          label="Keys Stored"
          value={status?.keys ?? '—'}
          detail="Applied to state machine"
        />
        <Stat
          icon={<Activity />}
          label="Commit Index"
          value={status?.commit_index ?? '—'}
          detail={`Applied: ${status?.applied_index ?? '—'}`}
        />
        <Stat
          icon={<ShieldCheck />}
          label="State Hash"
          value={status ? status.state_hash.slice(0, 8) : '—'}
          detail="Deterministic checksum"
        />
      </section>

      <section className="grid">
        <div className="panel nodes-panel">
          <div className="panel-head">
            <div>
              <p className="eyebrow">TOPOLOGY & MEMBERSHIP</p>
              <h2>Active Nodes</h2>
            </div>
            <span className="pill">
              <i className="online" /> Quorum Active
            </span>
          </div>
          <div className="node-list">
            {(status?.nodes || []).map((n) => (
              <div className={`node-item ${n.role === 'leader' ? 'leader-node' : ''}`} key={n.id}>
                <div className="node-icon-box">
                  {n.role === 'leader' ? <Crown size={16} /> : `N${n.id}`}
                </div>
                <div className="node-details">
                  <div className="node-name-row">
                    <b>Node {n.id}</b>
                    {n.id === status.node_id && <span className="current-badge">THIS NODE</span>}
                  </div>
                  <span className="node-addr">{n.addr.replace('http://', '')}</span>
                </div>
                <span className={`tag ${n.role === 'leader' ? 'tag-leader' : 'tag-follower'}`}>
                  {n.role}
                </span>
              </div>
            ))}
            {!status && <div className="empty">Connecting to cluster topology...</div>}
          </div>
        </div>

        <div className="panel console-panel">
          <div className="panel-head">
            <div>
              <p className="eyebrow">STATE MACHINE OPERATIONS</p>
              <h2>Keyspace Explorer</h2>
            </div>
            <KeyRound size={18} className="panel-head-icon" />
          </div>

          <div className="form">
            <input
              value={key}
              onChange={(e) => setKey(e.target.value)}
              placeholder="key name (e.g. greeting)"
              aria-label="Key"
            />
            <input
              value={value}
              onChange={(e) => setValue(e.target.value)}
              placeholder="value content"
              aria-label="Value"
              onKeyDown={(e) => e.key === 'Enter' && save()}
            />
            <button onClick={save} className="btn-set">
              SET <span>↵</span>
            </button>
          </div>

          <div className="key-list-header">
            <span>KEY</span>
            <span>ACTION</span>
          </div>

          <div className="key-list">
            {keys.map((k) => (
              <div className="key-row" key={k}>
                <button className="key-name" onClick={() => inspect(k)} title="Click to view full value">
                  <span className="key-bullet">•</span> {k}
                </button>
                <div className="key-actions">
                  <button className="view-btn" onClick={() => inspect(k)}>
                    View
                  </button>
                  <button className="icon-button" onClick={() => remove(k)} aria-label={`Delete ${k}`} title="Delete key">
                    <Trash2 size={14} />
                  </button>
                </div>
              </div>
            ))}
            {!keys.length && (
              <div className="empty">
                No keys stored yet. Enter a key and value above to propose a replicated write.
              </div>
            )}
          </div>
        </div>
      </section>

      <footer>
        <div className="footer-left">
          <span>
            <Eye size={14} /> Linearizable Read-Index
          </span>
          <span>
            <Zap size={14} /> Redis RESP2 / RESP3 Protocol
          </span>
          <span>
            <CheckCircle2 size={14} /> Raft v0.1.0 Consensus
          </span>
        </div>
        <div className="footer-right">
          <span>CONSENSUS • CONSISTENCY • SCALE</span>
        </div>
      </footer>

      {selected && (
        <div className="modal" onClick={() => setSelected(null)}>
          <div className="dialog" onClick={(e) => e.stopPropagation()}>
            <p className="eyebrow">KEY INSPECTION</p>
            <h2>{selected.key}</h2>
            <pre>{selected.value}</pre>
            <button onClick={() => setSelected(null)} className="dialog-close-btn">
              Close
            </button>
          </div>
        </div>
      )}
    </main>
  )
}

const Stat = ({ icon, label, value, detail }) => (
  <div className="stat">
    <div className="stat-icon">{icon}</div>
    <div className="stat-text">
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </div>
  </div>
)

createRoot(document.getElementById('root')).render(<App />)

