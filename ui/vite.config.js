import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import http from 'node:http'

// A development-only localhost gateway. Node connection settings select a
// local port, never an arbitrary host. Stream bodies instead of buffering KV.
function localNodeGateway() {
  const install = (server) => {
    server.middlewares.use((req, res, next) => {
    const match = req.url.match(/^\/nodes\/(\d+)(\/api(?:\/|\?|$).*)$/)
    if (!match) return next()
    const port = Number(match[1])
    if (!Number.isInteger(port) || port < 1024 || port > 65535) {
      res.writeHead(400, { 'content-type': 'application/json' }).end(JSON.stringify({ error: 'Invalid local API port.' }))
      return
    }
    const upstream = http.request({ hostname: '127.0.0.1', port, path: match[2], method: req.method, headers: { ...req.headers, host: `127.0.0.1:${port}` } }, response => {
      res.writeHead(response.statusCode, response.headers)
      response.pipe(res)
    })
    upstream.setTimeout(6000, () => upstream.destroy(new Error('Local node API request timed out.')))
    upstream.on('error', () => {
      if (!res.headersSent) res.writeHead(502, { 'content-type': 'application/json' }).end(JSON.stringify({ error: `Cannot reach local node API on port ${port}. Start the node or update its API port.` }))
      else res.destroy()
    })
    req.on('aborted', () => upstream.destroy())
    res.on('close', () => { if (!res.writableEnded) upstream.destroy() })
    req.pipe(upstream)
    })
  }
  return { name: 'raftkv-local-node-gateway', configureServer: install, configurePreviewServer: install }
}
export default defineConfig({
  plugins: [react(), localNodeGateway()],
  server: { host: '127.0.0.1', port: 5173, proxy: { '/api': 'http://127.0.0.1:8080' } },
  preview: { host: '127.0.0.1', proxy: { '/api': 'http://127.0.0.1:8080' } },
})
