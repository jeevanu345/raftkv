#!/usr/bin/env python3
"""Local quorum-cost benchmark matrix; raw host-specific evidence, not Redis claims."""
import argparse,json,pathlib,random,socket,subprocess,tempfile,time,urllib.request,itertools
ROOT=pathlib.Path(__file__).resolve().parents[1]
def frame(parts):
 parts=[p if isinstance(p,bytes) else str(p).encode() for p in parts];return b'*'+str(len(parts)).encode()+b'\r\n'+b''.join(b'$'+str(len(p)).encode()+b'\r\n'+p+b'\r\n' for p in parts)
def read_reply(reader):
 line=reader.readline()
 if not line:raise EOFError('server disconnected')
 if line[:1]in(b'+',b':'):return len(line)
 if line[:1]==b'-':raise RuntimeError(line.decode())
 if line[:1]==b'$':
  n=int(line[1:-2]);payload=reader.read(n+2) if n>=0 else b''
  if n>=0 and len(payload)!=n+2:raise EOFError('partial bulk')
  return len(line)+len(payload)
 raise ValueError(line)
def quantile(values,p):return sorted(values)[min(len(values)-1,int((len(values)-1)*p))]
def sizes(path):return sum(p.stat().st_size for p in path.rglob('*') if p.is_file())
def process_stats(processes):
 cpu=0.;rss=0
 for process in processes:
  fields=subprocess.check_output(['ps','-o','time=','-o','rss=','-p',str(process.pid)],text=True).split()
  if not fields:continue
  chunks=fields[0].split(':');seconds=sum(float(v)*60**i for i,v in enumerate(reversed(chunks)));cpu+=seconds;rss+=int(fields[1])*1024
 return cpu,rss
p=argparse.ArgumentParser();p.add_argument('--ops',type=int,default=1000);p.add_argument('--profile',choices=('debug','release'),default='release');p.add_argument('--quick',action='store_true');p.add_argument('--output',type=pathlib.Path,default=ROOT/'benchmark-results.json');args=p.parse_args();results=[]
for count in [1,3,5]:
 with tempfile.TemporaryDirectory(prefix='raftkv-bench-') as temporary:
  data=pathlib.Path(temporary);processes=[]
  try:
   for n in range(1,count+1):
    config=f'id={n}\nraft_listen="127.0.0.1:{27000+n}"\nclient_listen="127.0.0.1:{26379+n}"\nmetrics_listen="127.0.0.1:{29100+n}"\nui_listen="127.0.0.1:{28080+n}"\ndata_dir="{data/str(n)}"\nelection_timeout_ms=300\nheartbeat_ms=50\ntick_ms=10\nsnapshot_entries_threshold=10000\npre_vote=true\n'
    for i in range(1,count+1):config+=f'\n[[peers]]\nid={i}\nraft_addr="http://127.0.0.1:{27000+i}"\nclient_addr="127.0.0.1:{26379+i}"\nadmin_addr="http://127.0.0.1:{28080+i}"\n'
    file=data/f'{n}.toml';file.write_text(config);log=open(data/f'{n}.log','w');processes.append(subprocess.Popen([str(ROOT/f'target/{args.profile}/raftkv-server'),'--config',str(file)],stdout=log,stderr=log))
   leader=None;end=time.time()+15
   while time.time()<end and leader is None:
    for n in range(1,count+1):
     try:
      with urllib.request.urlopen(f'http://127.0.0.1:{28080+n}/api/v1/status',timeout=1) as response:status=json.load(response)
      if status['role']=='leader':leader=n;break
     except Exception:pass
    time.sleep(.1)
   if leader is None:raise AssertionError('election failed')
   combinations=itertools.product([.5,.95,0],[1,8,32],[64,1024,16384])
   if args.quick:combinations=[(.5,1,64),(.95,8,1024),(0,32,16384)]
   for read_ratio,depth,size in combinations:
    latencies=[];wire=0;before=sizes(data);cpu_before,_=process_stats(processes);rng=random.Random(42);started=time.perf_counter()
    with socket.create_connection(('127.0.0.1',26379+leader),timeout=10) as sock:
     reader=sock.makefile('rb');init=frame(['SET','bench',b'x'*size]);sock.sendall(init);read_reply(reader)
     completed=0
     while completed<args.ops:
      commands=[frame(['GET','bench'] if rng.random()<read_ratio else ['SET','bench',b'x'*size]) for _ in range(min(depth,args.ops-completed))]
      begin=time.perf_counter();sock.sendall(b''.join(commands));wire+=sum(map(len,commands))
      for command in commands:wire+=read_reply(reader);latencies.append((time.perf_counter()-begin)*1000)
      completed+=len(commands)
    elapsed=time.perf_counter()-started;cpu_after,rss=process_stats(processes)
    result={'nodes':count,'reads':read_ratio,'pipelineDepth':depth,'valueBytes':size,'operations':args.ops,'seconds':elapsed,'opsPerSecond':args.ops/elapsed,'latencyMs':{label:quantile(latencies,q) for label,q in [('p50',.5),('p95',.95),('p99',.99),('p99_9',.999)]},'cpuPercent':max(0,cpu_after-cpu_before)/elapsed*100,'memoryBytes':rss,'clientWireBytesPerSecond':wire/elapsed,'dataGrowthBytesPerSecond':max(0,sizes(data)-before)/elapsed,'diskBytesPerSecond':None,'networkBytesPerSecond':None,'notes':'Client wire bytes and data growth are measured separately. OS disk I/O and peer network byte counters unavailable in this portable harness.'};results.append(result);print(json.dumps(result),flush=True)
  finally:
   for process in processes:
    if process.poll() is None:process.kill();process.wait()
args.output.write_text(json.dumps({'build':args.profile,'host':__import__('platform').platform(),'results':results},indent=2)+'\n')
