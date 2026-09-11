import React, { useEffect, useState } from 'react'
import { createRoot } from 'react-dom/client'
import { Activity, Database, Eye, KeyRound, Moon, RefreshCw, Server, ShieldCheck, Sun, Trash2, Zap } from 'lucide-react'
import './styles.css'

const api = async (path, options) => { const r = await fetch(path, options); const data = await r.json(); if (!r.ok) throw new Error(data.error || 'Request failed'); return data }

function App() {
  const [status, setStatus] = useState(null), [keys, setKeys] = useState([]), [selected, setSelected] = useState(null)
  const [theme, setTheme] = useState(() => localStorage.getItem('raftkv-theme') || 'dark')
  const [key, setKey] = useState(''), [value, setValue] = useState(''), [message, setMessage] = useState(''), [loading, setLoading] = useState(true)
  const refresh = async () => { try { const [s, k] = await Promise.all([api('/api/status'), api('/api/keys')]); setStatus(s); setKeys(k.keys.map(x => new TextDecoder().decode(Uint8Array.from(x)))); setMessage('Live cluster connected') } catch (e) { setMessage(e.message) } finally { setLoading(false) } }
  useEffect(() => { refresh(); const t = setInterval(refresh, 4000); return () => clearInterval(t) }, [])
  useEffect(() => { document.documentElement.dataset.theme = theme; localStorage.setItem('raftkv-theme', theme) }, [theme])
  const save = async () => { if (!key.trim()) return; try { await api(`/api/keys/${encodeURIComponent(key)}`, { method: 'PUT', headers: {'content-type':'application/json'}, body: JSON.stringify({value}) }); setMessage(`Committed ${key}`); setKey(''); setValue(''); refresh() } catch (e) { setMessage(e.message) } }
  const remove = async k => { try { await api(`/api/keys/${encodeURIComponent(k)}`, { method: 'DELETE' }); setMessage(`Deleted ${k}`); refresh() } catch (e) { setMessage(e.message) } }
  const inspect = async k => { try { const d = await api(`/api/keys/${encodeURIComponent(k)}`); setSelected({key:k, value:d.value ?? 'null'}) } catch (e) { setMessage(e.message) } }
  const role = status?.role || 'offline'
  return <main><header><div className="brand"><div className="mark"><Zap size={19}/></div><div><strong>raftkv</strong><span>control plane</span></div></div><div className="connection"><i className={status ? 'online' : 'offline'} />{status ? 'LOCAL CLUSTER' : 'DISCONNECTED'}<button onClick={refresh} aria-label="Refresh"><RefreshCw size={15}/></button><button className="theme-toggle" onClick={()=>setTheme(theme === 'dark' ? 'light' : 'dark')} aria-label="Toggle light and dark mode">{theme === 'dark' ? <Sun size={15}/> : <Moon size={15}/>}</button></div></header>
    <section className="hero"><div><p className="eyebrow">DISTRIBUTED KEY-VALUE STORE / NODE {status?.node_id || '—'}</p><h1>Consensus, <em>visible.</em></h1><p className="sub">A focused operator console for your Raft-replicated state machine.</p></div><div className="hero-status"><span>LEADER STATUS</span><b className={role === 'leader' ? 'green' : ''}>{role.toUpperCase()}</b><small>Term {status?.term ?? '—'} · {message || 'Waiting for node'}</small></div></section>
    <section className="stats"><Stat icon={<Server/>} label="Cluster nodes" value={status?.nodes?.length ?? '—'} detail="Raft membership"/><Stat icon={<Database/>} label="Stored keys" value={status?.keys ?? '—'} detail="Sled state machine"/><Stat icon={<Activity/>} label="Commit index" value={status?.commit_index ?? '—'} detail={`Applied ${status?.applied_index ?? '—'}`}/><Stat icon={<ShieldCheck/>} label="State hash" value={status ? status.state_hash.slice(0,8) : '—'} detail="Deterministic snapshot"/></section>
    <section className="grid"><div className="panel nodes"><div className="panel-head"><div><p className="eyebrow">TOPOLOGY</p><h2>Cluster members</h2></div><span className="pill"><i className="online"/> quorum ready</span></div>{(status?.nodes || []).map(n => <div className="node" key={n.id}><div className="node-icon">N{n.id}</div><div className="node-copy"><b>Node {n.id} {n.id === status.node_id && <small>THIS NODE</small>}</b><span>{n.addr.replace('http://','')}</span></div><span className={n.role === 'leader' ? 'tag leader' : 'tag'}>{n.role}</span></div>)}{!status && <div className="empty">Start a raftkv node to see topology.</div>}</div>
      <div className="panel console"><div className="panel-head"><div><p className="eyebrow">STATE MACHINE</p><h2>Keyspace explorer</h2></div><KeyRound size={18}/></div><div className="form"><input value={key} onChange={e=>setKey(e.target.value)} placeholder="key"/><input value={value} onChange={e=>setValue(e.target.value)} placeholder="value" onKeyDown={e=>e.key==='Enter'&&save()}/><button onClick={save}>SET <span>↵</span></button></div><div className="key-list">{keys.map(k => <div className="key-row" key={k}><button className="key-name" onClick={()=>inspect(k)}>{k}</button><button className="icon-button" onClick={()=>remove(k)} aria-label={`Delete ${k}`}><Trash2 size={15}/></button></div>)}{!keys.length && <div className="empty">No keys yet. Commit your first value above.</div>}</div></div></section>
    <footer><span><Eye size={14}/> Read-index protected</span><span>RESP2 / RESP3</span><span>Raft core v0.1.0</span></footer>
    {selected && <div className="modal" onClick={()=>setSelected(null)}><div className="dialog" onClick={e=>e.stopPropagation()}><p className="eyebrow">KEY INSPECTION</p><h2>{selected.key}</h2><pre>{selected.value}</pre><button onClick={()=>setSelected(null)}>Close</button></div></div>}
  </main>
}
const Stat = ({icon,label,value,detail}) => <div className="stat"><div className="stat-icon">{icon}</div><div><span>{label}</span><strong>{value}</strong><small>{detail}</small></div></div>
createRoot(document.getElementById('root')).render(<App />)
