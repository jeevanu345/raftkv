import { useEffect, useRef } from "react";
import type { NodeDiagnostics } from "../../types/api";
import { formatBytes } from "../../lib/format";
export default function NodeDetails({node,onClose}:{node:NodeDiagnostics;onClose:()=>void}) {
  const ref=useRef<HTMLDialogElement>(null);
  useEffect(()=>{ref.current?.showModal();},[]);
  return <dialog ref={ref} className="dialog node-details" aria-labelledby="node-details-title" onCancel={event=>{event.preventDefault();onClose();}}>
    <div className="dialog__content"><h3 id="node-details-title">Node {node.nodeId} diagnostics</h3>
      <dl className="node-details__fields">{Object.entries({Role:node.role,Health:node.health,Term:node.term,Leader:node.leaderId,"Voted for":node.votedFor,"Last log":node.lastLogIndex,Commit:node.commitIndex,Applied:node.appliedIndex,Snapshot:node.snapshotIndex,"State hash":node.stateHash,"Disk usage":node.storageBytes==null?null:formatBytes(node.storageBytes),"Raft endpoint":node.raftAddress,"Client endpoint":node.clientAddress,"Admin endpoint":node.adminAddress}).map(([key,value])=><div key={key}><dt>{key}</dt><dd>{value??"Unavailable"}</dd></div>)}</dl>
      <h4>Peer replication</h4><div className="table-wrap"><table><thead><tr><th>Peer</th><th>Match</th><th>Next</th><th>Lag</th><th>Inflight</th><th>Active</th><th>Snapshot</th></tr></thead><tbody>{node.peers.map(peer=><tr key={peer.id}><td>{peer.id}</td><td>{peer.matchIndex}</td><td>{peer.nextIndex}</td><td>{peer.replicationLag}</td><td>{peer.inflight}</td><td>{peer.recentlyActive?"Yes":"No"}</td><td>{peer.snapshotInProgress?"Installing":"No"}</td></tr>)}</tbody></table>{node.peers.length===0&&<p>No leader-side peer progress is available on this node.</p>}</div>
      <div className="dialog__actions"><button className="button button--ghost" onClick={onClose}>Close diagnostics</button></div>
    </div></dialog>;
}
