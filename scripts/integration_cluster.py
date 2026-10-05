import json, socket, subprocess, tempfile, time, urllib.request, pathlib, argparse, concurrent.futures, threading, hashlib
options=argparse.ArgumentParser();options.add_argument("--nodes",type=int,choices=(3,5),default=3);options.add_argument("--peer-tls",action="store_true");args=options.parse_args();IDS=tuple(range(1,args.nodes+1))
root=pathlib.Path(__file__).resolve().parents[1];procs={};tmp=tempfile.TemporaryDirectory(prefix='raftkv-check-');data=pathlib.Path(tmp.name)
def api(n,path,body=None,method=None):
 req=urllib.request.Request(f'http://127.0.0.1:{18080+n}{path}',data=None if body is None else json.dumps(body).encode(),headers={'Content-Type':'application/json'},method=method)
 with urllib.request.urlopen(req,timeout=8) as res:return json.load(res)
def resp(n,*parts):
 with socket.create_connection(('127.0.0.1',16379+n),timeout=8) as sock:
  parts=[p if isinstance(p,bytes) else str(p).encode() for p in parts];sock.sendall(b'*'+str(len(parts)).encode()+b'\r\n'+b''.join(b'$'+str(len(p)).encode()+b'\r\n'+p+b'\r\n' for p in parts));return sock.recv(65536)
def start(n):
 f=open(data/f'{n}.log','a');procs[n]=subprocess.Popen([str(root/'target/debug/raftkv-server'),'--config',str(data/f'{n}.toml')],stdout=f,stderr=f)
def leader(ids=IDS):
 end=time.time()+15
 while time.time()<end:
  for n in ids:
   try:
    s=api(n,'/api/v1/status')
    if s['role']=='leader':return n
   except Exception:pass
  time.sleep(.1)
 raise AssertionError('no elected leader')
certificates={}
if args.peer_tls:
 def openssl(*arguments):return subprocess.check_output(['openssl',*arguments],stderr=subprocess.DEVNULL)
 openssl('req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(data/'ca.key'),'-out',str(data/'ca.crt'),'-days','1','-subj','/CN=RaftKVTestCA','-addext','keyUsage=critical,keyCertSign,cRLSign')
 (data/'ext').write_text('subjectAltName=IP:127.0.0.1\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\n')
 for n in IDS:
  openssl('req','-newkey','rsa:2048','-nodes','-keyout',str(data/f'{n}.key'),'-out',str(data/f'{n}.csr'),'-subj',f'/CN=node{n}')
  openssl('x509','-req','-in',str(data/f'{n}.csr'),'-CA',str(data/'ca.crt'),'-CAkey',str(data/'ca.key'),'-CAcreateserial','-out',str(data/f'{n}.crt'),'-days','1','-extfile',str(data/'ext'))
  certificates[n]=hashlib.sha256(openssl('x509','-in',str(data/f'{n}.crt'),'-outform','DER')).hexdigest()
try:
 for n in IDS:
  s=f'id={n}\nraft_listen="127.0.0.1:{17000+n}"\nclient_listen="127.0.0.1:{16379+n}"\nmetrics_listen="127.0.0.1:{19100+n}"\nui_listen="127.0.0.1:{18080+n}"\ndata_dir="{data/n.__str__()}"\nelection_timeout_ms=300\nheartbeat_ms=50\ntick_ms=10\nsnapshot_entries_threshold=8\npre_vote=true\n'
  for i in IDS:s+=f'\n[[peers]]\nid={i}\nraft_addr="http://127.0.0.1:{17000+i}"\nclient_addr="127.0.0.1:{16379+i}"\nadmin_addr="http://127.0.0.1:{18080+i}"\n'
  if args.peer_tls:
   s=s.replace('raft_addr="http://','raft_addr="https://')
   for i in IDS:s=s.replace(f'id={i}\nraft_addr=',f'id={i}\ncertificate_sha256="{certificates[i]}"\nraft_addr=')
   s+=f'\n[peer_tls]\ncert="{data/f"{n}.crt"}"\nkey="{data/f"{n}.key"}"\nca="{data/"ca.crt"}"\n'
  (data/f'{n}.toml').write_text(s);start(n)
 l=leader();print('leader',l,flush=True)
 assert resp(l,'SET','test','value')==b'+OK\r\n'
 assert resp(l,'GET','test')==b'$5\r\nvalue\r\n'
 for _ in range(12):assert resp(l,'INCR','count').startswith(b':')
 binary=b'\xff/k';assert resp(l,'SET',binary,b'\x00\xff')==b'+OK\r\n'
 assert resp(l,'GET',binary)==b'$2\r\n\x00\xff\r\n'
 assert resp(l,'SET','ttl','x','PX',100)==b'+OK\r\n';time.sleep(1.2);assert resp(l,'GET','ttl')==b'$-1\r\n'
 assert resp(l,'SCAN','0','COUNT','2').startswith(b'*2')
 # Concurrent histories include the real request's term and proposal receipt.
 history=[];history_lock=threading.Lock();origin=time.perf_counter_ns()
 def client(client_id):
  for turn in range(2):
   value=f'{client_id}-{turn}';begin=time.perf_counter_ns()-origin
   result=api(l,'/api/v1/commands',{'command':f'SET history {value}'})
   end=time.perf_counter_ns()-origin;assert result['success'];assert result['execution']['proposedIndex']>0
   record={'id':f'{client_id}-set-{turn}','client':client_id,'invocation':begin,'response':end,'nodeContacted':l,'leaderTerm':result['execution']['term'],'logIndex':result['execution']['proposedIndex'],'input':value,'result':'OK','op':{'Set':{'key':list(b'history'),'value':list(value.encode())}}}
   with history_lock:history.append(record)
   begin=time.perf_counter_ns()-origin;result=api(l,'/api/v1/commands',{'command':'GET history'});end=time.perf_counter_ns()-origin;assert result['success']
   record={'id':f'{client_id}-get-{turn}','client':client_id,'invocation':begin,'response':end,'nodeContacted':l,'leaderTerm':result['execution']['term'],'result':result['display'],'op':{'Get':{'key':list(b'history'),'observed':list(result['display'].encode())}}}
   with history_lock:history.append(record)
 with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:list(pool.map(client,range(1,4)))
 file=data/'history.json';file.write_text(json.dumps(history));subprocess.check_call([str(root/'target/debug/linearizability-checker'),str(file)])
 print('Concurrent linearizability history passed',flush=True)
 follower=next(i for i in IDS if i!=l);assert api(follower,'/api/v1/keys/test')['value']=='value'
 result=api(l,'/api/v1/commands',{'command':'SET target x','targetNodeId':follower});assert not result['success'] and result['display'].startswith('MOVED'),result
 status=api(l,'/api/v1/status');assert all('data' not in entry for entry in status['logEntries'])
 with urllib.request.urlopen(f'http://127.0.0.1:{18080+follower}/api/v1/admin/backup',timeout=8) as response:backup=response.read()
 assert backup.startswith(b'RKVBAK01')
 # Metrics listener must not expose the admin API.
 try:urllib.request.urlopen(f'http://127.0.0.1:{19100+l}/api/v1/status',timeout=3);raise AssertionError('metrics listener leaked API')
 except __import__('urllib.error',fromlist=['HTTPError']).HTTPError as error:assert error.code==404
 api(l,'/api/v1/admin/snapshot',{});assert api(l,'/api/v1/snapshots')
 print('RESP, binary, TTL, SCAN, HTTP forwarding, snapshot passed',flush=True)
 procs[l].kill();procs[l].wait();nl=leader(tuple(i for i in IDS if i!=l));assert resp(nl,'GET','count')==b'$2\r\n12\r\n'
 for _ in range(25):assert resp(nl,'INCR','count').startswith(b':')
 start(l);time.sleep(3);assert api(l,'/api/v1/keys/count')['value']=='37'
 print('failover and snapshot catchup passed',flush=True)
 for p in procs.values():p.kill();p.wait()
 for i in IDS:start(i)
 l=leader();assert resp(l,'GET','count')==b'$2\r\n37\r\n';print('full restart persisted INCR exactly once',flush=True)
 others=[i for i in IDS if i!=l]
 for i in others:procs[i].kill();procs[i].wait()
 result=resp(l,'GET','count');assert result.startswith(b'-'),result
 try:urllib.request.urlopen(f'http://127.0.0.1:{19100+l}/health/ready',timeout=3);raise AssertionError('isolated leader remained ready')
 except __import__('urllib.error',fromlist=['HTTPError']).HTTPError as error:assert error.code==503
 print('isolated leader read rejected:',result.decode().strip(),flush=True)
except Exception:
 for n in procs:
  print('NODE',n,(data/f'{n}.log').read_text()[-5000:])
 raise
finally:
 for p in procs.values():
  if p.poll() is None:p.kill();p.wait()
 tmp.cleanup()
