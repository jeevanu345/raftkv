import { Copy, Database, RefreshCw, Save, Search, Trash2 } from "lucide-react";
import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../lib/api";
import { formatBytes, formatTtl } from "../lib/format";
import { convertEncoding } from "../lib/encoding";
import type { KeyDetail } from "../types/api";
import Panel from "../components/common/Panel";
import ConfirmDialog from "../components/common/ConfirmDialog";

export default function KeyExplorerPage() {
 const queryClient=useQueryClient();
 const [pattern,setPattern]=useState("");const [cursor,setCursor]=useState<string|null>(null);const [previous,setPrevious]=useState<(string|null)[]>([]);
 const [selectedKey,setSelectedKey]=useState<string|null>(null);const [draftValue,setDraftValue]=useState("");const [encoding,setEncoding]=useState<KeyDetail["encoding"]>("utf8");const [ttl,setTtl]=useState("");const [confirmDelete,setConfirmDelete]=useState(false);const [notice,setNotice]=useState("");
 const keys=useQuery({queryKey:["keys",pattern,cursor],queryFn:()=>api.getKeys({pattern,cursor,limit:100})});
 const detail=useQuery({queryKey:["key",selectedKey],queryFn:()=>api.getKey(selectedKey!),enabled:selectedKey!==null});
 useEffect(()=>{if(detail.data){setDraftValue(detail.data.value);setEncoding(detail.data.encoding);setTtl(detail.data.ttlMs===null?"":String(detail.data.ttlMs));}},[detail.data]);
 useEffect(()=>{if(!selectedKey && keys.data?.items.length){setSelectedKey(keys.data.items[0].key);}},[keys.data,selectedKey]);
 const invalidate=()=>Promise.all([queryClient.invalidateQueries({queryKey:["key",selectedKey]}),queryClient.invalidateQueries({queryKey:["keys"]}),queryClient.invalidateQueries({queryKey:["cluster"]})]);
 const save=useMutation({mutationFn:async()=>{if(ttl && (!Number.isSafeInteger(Number(ttl)) || Number(ttl)<0)){throw new Error("TTL must be a nonnegative number of milliseconds.");}await api.putKey(selectedKey!,{value:draftValue,encoding,ttlMs:ttl===""?null:Number(ttl)});},onSuccess:async()=>{await invalidate();setNotice("Value committed through Raft.");}});
 const remove=useMutation({mutationFn:()=>api.deleteKey(selectedKey!),onSuccess:async()=>{setSelectedKey(null);await invalidate();setNotice("Key deleted through Raft.");}});
 async function copy(value:string){try{await navigator.clipboard.writeText(value);setNotice("Copied.");}catch{setNotice("Clipboard is unavailable.");}}
 function changeEncoding(next:KeyDetail["encoding"]){try{setDraftValue(convertEncoding(draftValue,encoding,next));setEncoding(next);setNotice("");}catch(error){setNotice(error instanceof Error?error.message:"Invalid encoding");}}
 const failure=keys.error??detail.error??save.error??remove.error;
 return <>
  {failure && <div className="feedback feedback--error" role="alert">{failure.message}</div>}
  {notice && <div className="feedback" role="status">{notice}</div>}
  <div className="key-explorer">
   <Panel className="key-browser" title="Keys" description={`${keys.data?.totalApproximate??0} keys · SCAN pagination`} action={<button className="icon-button" title="Refresh keys" onClick={()=>keys.refetch()}><RefreshCw size={15}/></button>}>
    <div className="search-field"><Search size={15}/><input aria-label="Key pattern" value={pattern} onChange={event=>{setPattern(event.target.value);setCursor(null);setPrevious([]);}} placeholder="Filter, for example user:*"/></div>
    {keys.isLoading && <p role="status">Loading keys</p>}
    {keys.data?.items.length===0 && <p className="feedback">No matching keys in this page. Continue scanning if another cursor is available.</p>}
    <div className="key-list">{keys.data?.items.map(item=><button key={item.key} className={`key-list-item ${selectedKey===item.key?"key-list-item--active":""}`} onClick={()=>{setSelectedKey(item.key);setNotice("");}}><div className="key-list-item__main"><Database size={14}/><span>{item.key}</span></div><div className="key-list-item__meta"><span>{formatBytes(item.sizeBytes)}</span><span>{formatTtl(item.ttlMs)}</span></div></button>)}</div>
    <div className="inline-actions pagination"><button className="button button--ghost" disabled={!previous.length || keys.isFetching} onClick={()=>{setCursor(previous[previous.length-1]);setPrevious(previous.slice(0,-1));}}>Previous</button><button className="button button--ghost" disabled={!keys.data?.cursor || keys.isFetching} onClick={()=>{setPrevious([...previous,cursor]);setCursor(keys.data!.cursor);}}>Next page</button></div>
   </Panel>
   <Panel className="key-editor" title={selectedKey??"Select a key"} description="Values read after ReadIndex quorum confirmation" action={detail.data && <div className="inline-actions"><button className="button button--ghost button--small" onClick={()=>copy(detail.data!.key)}><Copy size={14}/>Copy key</button><button className="button button--danger-ghost button--small" disabled={remove.isPending} onClick={()=>setConfirmDelete(true)}><Trash2 size={14}/>Delete</button></div>}>
    {detail.isLoading?<div className="empty-panel">Loading value</div>:!detail.data?<div className="empty-panel">Select a key to inspect its value.</div>:<div className="key-detail">
     <div className="key-detail__metadata"><div><span>Type</span><strong>{detail.data.type}</strong></div><div><span>Size</span><strong>{formatBytes(detail.data.sizeBytes)}</strong></div><div><span>TTL</span><strong>{formatTtl(detail.data.ttlMs)}</strong></div><div><label htmlFor="encoding">Encoding</label><select id="encoding" value={encoding} onChange={event=>changeEncoding(event.target.value as KeyDetail["encoding"])}><option value="utf8">UTF-8</option><option value="base64">Base64</option><option value="hex">Hex</option></select></div></div>
     <div className="field-group"><div className="field-label-row"><label htmlFor="value">Value</label><button className="text-button" onClick={()=>copy(draftValue)}><Copy size={13}/>Copy value</button></div><textarea id="value" className="code-editor" value={draftValue} spellCheck={false} onChange={event=>setDraftValue(event.target.value)}/></div>
     <div className="field-group"><label htmlFor="ttl">Expiration in milliseconds (blank means persistent)</label><input id="ttl" type="number" min="0" value={ttl} onChange={event=>setTtl(event.target.value)}/><button className="button button--ghost" onClick={()=>setTtl("")}>Persist expiry on save</button></div>
     <div className="replication-warning">Saving or deleting this value is replicated through Raft.</div><div className="form-actions"><button className="button button--primary" disabled={save.isPending || remove.isPending} onClick={()=>save.mutate()}><Save size={15}/>{save.isPending?"Saving":"Save value and TTL"}</button></div>
    </div>}
   </Panel>
  </div>
  <ConfirmDialog open={confirmDelete} title="Delete replicated key" description={`Delete "${selectedKey}" through Raft?`} confirmLabel="Delete key" danger onClose={()=>setConfirmDelete(false)} onConfirm={()=>remove.mutate()}/>
 </>;
}
