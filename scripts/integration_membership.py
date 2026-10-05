#!/usr/bin/env python3
"""Learner catch-up, promotion, joint consensus, transfer and removal."""
import json,pathlib,subprocess,tempfile,time,urllib.request
root=pathlib.Path(__file__).resolve().parents[1];td=tempfile.TemporaryDirectory(prefix='raftkv-members-');d=pathlib.Path(td.name);procs={}
def api(n,path,body=None,method=None):
 request=urllib.request.Request(f'http://127.0.0.1:{28080+n}{path}',data=None if body is None else json.dumps(body).encode(),method=method,headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(request,timeout=8) as response:return json.load(response)
def wait(predicate):
 end=time.time()+15
 while time.time()<end:
  try:
   value=predicate()
   if value:return value
  except Exception:pass
  time.sleep(.1)
 raise AssertionError('timed out waiting for membership state')
def leader():
 for n in procs:
  if procs[n].poll() is None and api(n,'/api/v1/status')['role']=='leader':return n
 return None
def start(n):
 config=f'id={n}\nraft_listen="127.0.0.1:{27000+n}"\nclient_listen="127.0.0.1:{26379+n}"\nmetrics_listen="127.0.0.1:{29100+n}"\nui_listen="127.0.0.1:{28080+n}"\ndata_dir="{d/str(n)}"\nelection_timeout_ms=300\nheartbeat_ms=50\ntick_ms=10\nsnapshot_entries_threshold=8\npre_vote=true\n'
 for i in range(1,4):config+=f'\n[[peers]]\nid={i}\nraft_addr="http://127.0.0.1:{27000+i}"\nclient_addr="127.0.0.1:{26379+i}"\nadmin_addr="http://127.0.0.1:{28080+i}"\n'
 if n==4:config+='\n[[peers]]\nid=4\nraft_addr="http://127.0.0.1:27004"\nclient_addr="127.0.0.1:26383"\nadmin_addr="http://127.0.0.1:28084"\nlearner=true\n'
 file=d/f'{n}.toml';file.write_text(config);log=open(d/f'{n}.log','a');procs[n]=subprocess.Popen([str(root/'target/debug/raftkv-server'),'--config',str(file)],stdout=log,stderr=log)
try:
 for n in (1,2,3):start(n)
 l=wait(leader)
 for i in range(20):assert api(l,'/api/v1/commands',{'command':f'SET k{i} value'})['success']
 start(4);member={'id':4,'role':'learner','raftAddress':'http://127.0.0.1:27004','clientAddress':'127.0.0.1:26383','adminAddress':'http://127.0.0.1:28084'}
 api(l,'/api/v1/admin/members',member)
 wait(lambda:api(4,'/api/v1/status')['appliedIndex']>=api(l,'/api/v1/status')['commitIndex']-1)
 member['role']='voter';api(l,'/api/v1/admin/members',member)
 wait(lambda:len(api(l,'/api/v1/status')['voters'])==4 and api(l,'/api/v1/status')['configurationState']=='stable')
 api(l,'/api/v1/admin/leadership',{'targetId':4});wait(lambda:api(4,'/api/v1/status')['role']=='leader')
 assert api(4,'/api/v1/keys/k19')['value']=='value'
 remove=next(n for n in (1,2,3) if n!=l);api(4,f'/api/v1/admin/members/{remove}',method='DELETE')
 wait(lambda:len(api(4,'/api/v1/status')['voters'])==3)
 assert remove not in [m['id'] for m in api(4,'/api/v1/cluster')['members']]
 procs[4].kill();procs[4].wait();start(4);wait(lambda:api(4,'/api/v1/status')['health']=='healthy')
 assert remove not in [m['id'] for m in api(4,'/api/v1/cluster')['members']]
 print('Learner snapshot catch-up, promotion to 4 voters, leadership transfer, joint removal and membership restart passed')
except Exception:
 for n in procs:print('NODE',n,(d/f'{n}.log').read_text()[-4000:])
 raise
finally:
 for process in procs.values():
  if process.poll() is None:process.kill();process.wait()
 td.cleanup()
