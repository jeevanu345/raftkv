#!/usr/bin/env python3
import pathlib,subprocess,time,json,urllib.request,os
root=pathlib.Path(__file__).resolve().parents[1];p=subprocess.Popen([str(root/'target/debug/raftkv-lab')],env={**os.environ,'RAFTKV_LAB_LISTEN':'127.0.0.1:18990'},stdout=subprocess.DEVNULL)
def request(path,body=None):
 req=urllib.request.Request('http://127.0.0.1:18990/lab/api/v1/'+path,data=None if body is None else json.dumps(body).encode(),headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=10) as response:return json.load(response)
try:
 for _ in range(100):
  try:request('state');break
  except Exception:time.sleep(.05)
 request('configure',{'seed':42,'nodeCount':5,'delayMinTicks':1,'delayMaxTicks':4,'dropProbability':.05})
 for _ in range(80):request('step',{})
 request('partition',{'nodeId':2});request('crash',{'nodeId':3})
 for _ in range(20):request('step',{})
 request('restart',{'nodeId':3});request('heal',{})
 for _ in range(30):request('step',{})
 state=request('state');replay=request('replay');restored=request('replay',replay)
 for field in ('tick','leaderId','seed','nodes','events'):assert state[field]==restored[field],field
 print('Deterministic lab replay passed')
finally:p.kill();p.wait()
