import pathlib,subprocess,tempfile,time,json,socket,urllib.request,urllib.error,ssl,hashlib
root=pathlib.Path(__file__).resolve().parents[1];td=tempfile.TemporaryDirectory(prefix='raftkv-security-');d=pathlib.Path(td.name);p=None
# Locally generated CA and SAN certificate, discarded with this test directory.
def run(*args):return subprocess.check_output(args,stderr=subprocess.DEVNULL)
run('openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(d/'ca.key'),'-out',str(d/'ca.crt'),'-days','1','-subj','/CN=RaftKV-Test-CA','-addext','keyUsage=critical,keyCertSign,cRLSign','-addext','basicConstraints=critical,CA:TRUE')
run('openssl','req','-newkey','rsa:2048','-nodes','-keyout',str(d/'node.key'),'-out',str(d/'node.csr'),'-subj','/CN=localhost');(d/'ext').write_text('subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=critical,digitalSignature,keyEncipherment\n')
run('openssl','x509','-req','-in',str(d/'node.csr'),'-CA',str(d/'ca.crt'),'-CAkey',str(d/'ca.key'),'-CAcreateserial','-out',str(d/'node.crt'),'-days','1','-extfile',str(d/'ext'))
cert_der=run('openssl','x509','-in',str(d/'node.crt'),'-outform','DER');fingerprint=hashlib.sha256(cert_der).hexdigest();context=ssl.create_default_context(cafile=str(d/'ca.crt'))
def api(path,body=None,token='admin-secret',method=None,cookie=None,origin=None):
 req=urllib.request.Request('https://localhost:18880'+path,data=None if body is None else json.dumps(body).encode(),method=method,headers={'Content-Type':'application/json',**({'Authorization':'Bearer '+token} if token else {}),**({'Cookie':cookie} if cookie else {}),**({'Origin':origin} if origin else {})})
 try:
  with urllib.request.urlopen(req,context=context,timeout=10) as res:return res.status,res.read(),res.headers
 except urllib.error.HTTPError as e:return e.code,e.read(),e.headers
def resp(parts,auth=None):
 with socket.create_connection(('localhost',16879),timeout=10) as raw:
  with context.wrap_socket(raw,server_hostname='localhost') as sock:
   def send(parts):
    parts=[p.encode() for p in parts];sock.sendall(b'*'+str(len(parts)).encode()+b'\r\n'+b''.join(b'$'+str(len(p)).encode()+b'\r\n'+p+b'\r\n' for p in parts));return sock.recv(65536)
   if auth:assert send(auth)==b'+OK\r\n'
   return send(parts)
try:
 conf=f'''id=1
raft_listen="127.0.0.1:17801"
client_listen="127.0.0.1:16879"
metrics_listen="127.0.0.1:19810"
ui_listen="127.0.0.1:18880"
data_dir="{d/'data'}"
election_timeout_ms=300
heartbeat_ms=50
tick_ms=10
snapshot_entries_threshold=8
pre_vote=true
admin_token="admin-secret"
client_token="client-secret"
[[admin_users]]
name="viewer"
token="view-secret"
role="viewer"
[[admin_users]]
name="operator"
token="operator-secret"
role="operator"
[[client_users]]
name="reader"
password="reader-secret"
read=true
write=false
key_prefixes=["tenant:"]
[[peers]]
id=1
raft_addr="https://localhost:17801"
client_addr="localhost:16879"
admin_addr="https://localhost:18880"
certificate_sha256="{fingerprint}"
'''
 for plane in ('peer_tls','client_tls','admin_tls'):conf+=f'\n[{plane}]\ncert="{d/"node.crt"}"\nkey="{d/"node.key"}"\nca="{d/"ca.crt"}"\n'
 (d/'config.toml').write_text(conf);log=open(d/'server.log','w');p=subprocess.Popen([str(root/'target/debug/raftkv-server'),'--config',str(d/'config.toml')],stdout=log,stderr=log)
 for _ in range(100):
  try:
   status,body,_=api('/api/v1/status')
   if status==200 and json.loads(body)['role']=='leader':break
  except Exception as e:last_error=e
  time.sleep(.1)
 else:raise AssertionError(f'secure node not ready: process={p.poll()} last={locals().get("last_error")} status={locals().get("status")} body={locals().get("body")}')
 assert api('/api/v1/status',token=None)[0]==401
 assert api('/api/v1/status',token='view-secret')[0]==200
 assert api('/api/v1/keys/tenant:test',token='view-secret')[0]==403
 assert api('/api/v1/admin/snapshot',{},token='operator-secret')[0]==200
 assert api('/api/v1/admin/members',{},token='operator-secret')[0]==403
 assert resp(['GET','tenant:test']).startswith(b'-NOAUTH')
 assert resp(['SET','tenant:test','backup-value'],['AUTH','client-secret'])==b'+OK\r\n'
 assert resp(['GET','tenant:test'],['AUTH','reader','reader-secret'])==b'$12\r\nbackup-value\r\n'
 assert resp(['GET','other'],['AUTH','reader','reader-secret']).startswith(b'-NOPERM')
 assert resp(['SET','tenant:test','no'],['AUTH','reader','reader-secret']).startswith(b'-NOPERM')
 status,body,headers=api('/api/v1/auth',{'token':'view-secret'},token=None);assert status==200 and 'Secure' in headers['Set-Cookie']
 cookie=headers['Set-Cookie'].split(';')[0];assert api('/api/v1/status',token=None,cookie=cookie)[0]==200
 assert api('/api/v1/status',token=None,cookie=cookie+'x')[0]==401
 assert api('/api/v1/admin/snapshot',{},token=None,cookie=cookie,origin='https://untrusted.invalid')[0]==403
 import hmac
 issued=int(time.time())-43201;signature=hmac.new(b'view-secret',b'raftkv-session-v1'+issued.to_bytes(8,'little'),hashlib.sha256).hexdigest()
 assert api('/api/v1/status',token=None,cookie=f'raftkv_session={issued}.{signature}')[0]==401
 # Backup CLI validates HTTPS against the explicit test CA.
 cli=root/'target/debug/raftkv-cli';backup=d/'backup.rkv'
 subprocess.check_call([str(cli),'--admin-addr','https://localhost:18880','--admin-token','admin-secret','--ca-cert',str(d/'ca.crt'),'backup','create',str(backup)])
 out=subprocess.check_output([str(cli),'backup','inspect',str(backup)]);assert json.loads(out)['key_count']==1
 p.kill();p.wait();p=None
 restored=d/'restore';subprocess.check_call([str(cli),'restore',str(backup),'--data-dir',str(restored),'--bootstrap-node-id','1','--raft-addr','https://localhost:17801','--client-addr','localhost:16879','--admin-addr','https://localhost:18880'])
 (d/'config.toml').write_text(conf.replace(str(d/'data'),str(restored)));p=subprocess.Popen([str(root/'target/debug/raftkv-server'),'--config',str(d/'config.toml')],stdout=log,stderr=log)
 for _ in range(100):
  try:
   result=resp(['GET','tenant:test'],['AUTH','client-secret'])
   if result==b'$12\r\nbackup-value\r\n':break
  except Exception:pass
  time.sleep(.1)
 else:raise AssertionError('restored node failed')
 assert api('/api/v1/commands',{'command':'SET tenant:receipt x'})[0]==200
 records=[json.loads(line) for line in (restored/'audit.jsonl').read_text().splitlines()]
 assert any(row.get('requestId') is not None for row in records),records
 print('HTTPS, peer TLS startup, RESP TLS/AUTH/ACL, RBAC, secure cookie, backup integrity, offline restore passed',flush=True)
except Exception:
 print((d/'server.log').read_text()[-8000:]);raise
finally:
 if p and p.poll() is None:p.kill();p.wait()
 td.cleanup()
