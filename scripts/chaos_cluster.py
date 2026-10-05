#!/usr/bin/env python3
"""External real-process fault harness. HTTP/RESP stay reachable while peer TCP
proxies partition Raft traffic. Plaintext peer transport only; mTLS is tested
separately by integration_cluster.py. No fault endpoint exists in the server.
"""
import argparse,asyncio,concurrent.futures,json,pathlib,socket,subprocess,tempfile,threading,time,urllib.request,urllib.error
ROOT=pathlib.Path(__file__).resolve().parents[1]

def varint(data,position):
 value=0
 for shift in range(0,70,7):
  if position>=len(data):return None
  byte=data[position];position+=1;value|=(byte&127)<<shift
  if byte<128:return value,position
 return None

def sender(payload):
 # First gRPC DATA message: Envelope.node_id, or unary/chunk term + leader_id.
 if len(payload)<7 or payload[0]!=0:return None
 data=payload[5:];tag=varint(data,0)
 if not tag or tag[0]!=8:return None
 first=varint(data,tag[1])
 if not first:return None
 second=varint(data,first[1])
 if second and second[0]==16:
  result=varint(data,second[1]);return result[0] if result else None
 return first[0]

class Network:
 def __init__(self,count):
  self.count=count;self.blocked=set();self.delay=0.;self.connections=[];self.bytes=0;self.loop=asyncio.new_event_loop();self.ready=threading.Event()
  self.thread=threading.Thread(target=self.run,daemon=True);self.thread.start();assert self.ready.wait(5)
 def run(self):
  asyncio.set_event_loop(self.loop);self.loop.run_until_complete(self.listen());self.ready.set();self.loop.run_forever()
 async def listen(self):
  self.servers=[]
  for node in range(1,self.count+1):
   self.servers.append(await asyncio.start_server(lambda reader,writer,node=node:self.connect(node,reader,writer),'127.0.0.1',22000+node))
 async def connect(self,destination,reader,writer):
  remote=None;context={'from':None,'to':destination,'writers':[writer]};self.connections.append(context)
  try:
   upstream,remote=await asyncio.open_connection('127.0.0.1',23000+destination);context['writers'].append(remote)
   preface=await reader.readexactly(24)
   if preface!=b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n':return
   remote.write(preface);await remote.drain()
   async def pipe(source,target,outbound):
    while True:
     header=await source.readexactly(9);size=int.from_bytes(header[:3],'big')
     if size>2*1024*1024:raise ValueError('oversized HTTP2 frame')
     payload=await source.readexactly(size)
     if outbound and header[3]==0 and context['from'] is None:
      content=payload[1:len(payload)-payload[0]] if header[4]&8 else payload
      context['from']=sender(content)
     if context['from'] is not None:
      edge=(context['from'],destination)
      if edge in self.blocked or edge[::-1] in self.blocked:raise ConnectionError('partition')
     if self.delay and header[3]==0:await asyncio.sleep(self.delay)
     target.write(header+payload);await target.drain();self.bytes+=9+len(payload)
   tasks=[asyncio.create_task(pipe(reader,remote,True)),asyncio.create_task(pipe(upstream,writer,False))]
   _,pending=await asyncio.wait(tasks,return_when=asyncio.FIRST_COMPLETED)
   for task in pending:task.cancel()
   await asyncio.gather(*tasks,return_exceptions=True)
  except (ConnectionError,asyncio.IncompleteReadError,OSError,ValueError):pass
  finally:
   for connection in context['writers']:connection.close()
   self.connections.remove(context)
 def partition(self,groups):
  async def change():
   self.blocked={(a,b) for group in groups for a in group for other in groups if other is not group for b in other}
   for connection in list(self.connections):
    if (connection['from'],connection['to']) in self.blocked:
     for writer in connection['writers']:writer.close()
  asyncio.run_coroutine_threadsafe(change(),self.loop).result(5)
 def heal(self):self.partition([list(range(1,self.count+1))])
 def close(self):
  async def stop():
   for server in self.servers:server.close();await server.wait_closed()
   for connection in list(self.connections):
    for writer in connection['writers']:writer.close()
  asyncio.run_coroutine_threadsafe(stop(),self.loop).result(5);self.loop.call_soon_threadsafe(self.loop.stop);self.thread.join(5)

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--nodes',type=int,choices=(3,5),default=3);parser.add_argument('--history',type=pathlib.Path);args=parser.parse_args();ids=list(range(1,args.nodes+1));processes={};network=Network(args.nodes)
 with tempfile.TemporaryDirectory(prefix='raftkv-chaos-') as temporary:
  directory=pathlib.Path(temporary)
  def api(node,path,body=None):
   request=urllib.request.Request(f'http://127.0.0.1:{24000+node}{path}',data=None if body is None else json.dumps(body).encode(),headers={'Content-Type':'application/json'})
   with urllib.request.urlopen(request,timeout=8) as response:return json.load(response)
  def start(node):
   log=open(directory/f'{node}.log','a');processes[node]=subprocess.Popen([str(ROOT/'target/debug/raftkv-server'),'--config',str(directory/f'{node}.toml')],stdout=log,stderr=log)
  def wait_started(node):
   end=time.monotonic()+10
   while time.monotonic()<end:
    assert processes[node].poll() is None,f'node {node} exited during startup: '+(directory/f'{node}.log').read_text()[-2000:]
    try:
     api(node,'/api/v1/status');return
    except (OSError,urllib.error.URLError):time.sleep(.05)
   raise AssertionError(f'node {node} did not start')
  def leader(available):
   end=time.monotonic()+15
   while time.monotonic()<end:
    for node in available:
     try:
      if api(node,'/api/v1/status')['role']=='leader':return node
     except Exception:pass
    time.sleep(.1)
   raise AssertionError('no leader in expected component')
  history=[];lock=threading.Lock();origin=time.perf_counter_ns()
  def clients(current):
   def client(client_id):
    for turn in range(3):
     value=f'{client_id}:{turn}';begin=time.perf_counter_ns()-origin
     result=api(current,'/api/v1/commands',{'command':f'SET chaos {value}','targetNodeId':current});end=time.perf_counter_ns()-origin;assert result['success'],result
     with lock:history.append({'id':len(history),'client':client_id,'invocation':begin,'response':end,'nodeContacted':current,'leaderTerm':result['execution']['term'],'logIndex':result['execution']['proposedIndex'],'input':value,'result':'OK','op':{'Set':{'key':list(b'chaos'),'value':list(value.encode())}}})
     begin=time.perf_counter_ns()-origin;result=api(current,'/api/v1/commands',{'command':'GET chaos','targetNodeId':current});end=time.perf_counter_ns()-origin;assert result['success'],result
     with lock:history.append({'id':len(history),'client':client_id,'invocation':begin,'response':end,'nodeContacted':current,'leaderTerm':result['execution']['term'],'result':result['display'],'op':{'Get':{'key':list(b'chaos'),'observed':list(result['display'].encode())}}})
   with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:list(pool.map(client,range(1,4)))
  try:
   for node in ids:
    config=f'id={node}\nraft_listen="127.0.0.1:{23000+node}"\nclient_listen="127.0.0.1:{25000+node}"\nmetrics_listen="127.0.0.1:{26000+node}"\nui_listen="127.0.0.1:{24000+node}"\ndata_dir="{directory/str(node)}"\nelection_timeout_ms=800\nheartbeat_ms=80\ntick_ms=10\nsnapshot_entries_threshold=12\npre_vote=true\n'
    for peer in ids:config+=f'\n[[peers]]\nid={peer}\nraft_addr="http://127.0.0.1:{22000+peer}"\nclient_addr="127.0.0.1:{25000+peer}"\nadmin_addr="http://127.0.0.1:{24000+peer}"\n'
    (directory/f'{node}.toml').write_text(config);start(node)
   for node in ids:wait_started(node)
   old=leader(ids);print("initial leader",old,flush=True);clients(old);majority=[node for node in ids if node!=old];network.partition([[old],majority]);time.sleep(2);current=leader(majority);print("majority leader",current,flush=True)
   result=api(old,'/api/v1/commands',{'command':'GET chaos','targetNodeId':old});assert not result['success'],result
   result=api(old,'/api/v1/commands',{'command':'SET uncommitted minority','targetNodeId':old});assert not result['success'],result
   assert network.blocked and any(connection['from'] for connection in network.connections),'proxy did not identify peer streams'
   print("partition workload",flush=True);clients(current);api(current,'/api/v1/admin/snapshot',{});processes[old].kill();processes[old].wait();start(old);wait_started(old);network.heal();time.sleep(2)
   assert api(current,'/api/v1/commands',{'command':'GET uncommitted'})['display']=='(nil)'
   # Directed components are supported; test a second minority and peer latency.
   minority=[node for node in ids if node!=current][:max(1,args.nodes//2)];majority=[node for node in ids if node not in minority];network.partition([minority,majority]);network.delay=.01;print("latency workload",flush=True);clients(current);network.delay=0;network.heal()
   processes[current].kill();processes[current].wait();replacement=leader([node for node in ids if node!=current]);print("replacement leader",replacement,flush=True);clients(replacement);start(current);wait_started(current);time.sleep(2)
   file=args.history or directory/'history.json';file.parent.mkdir(parents=True,exist_ok=True);file.write_text(json.dumps(history,indent=2));subprocess.check_call([str(ROOT/'target/debug/linearizability-checker'),str(file)])
   print(f'{args.nodes}-node external peer partition/heal/latency/leader-kill/snapshot/restart passed; {len(history)} checked operations; {network.bytes} peer HTTP2 bytes')
  except Exception:
   for node in processes:print('NODE',node,(directory/f'{node}.log').read_text()[-5000:])
   raise
  finally:
   for process in processes.values():
    if process.poll() is None:process.kill();process.wait()
   network.close()
if __name__=='__main__':main()
