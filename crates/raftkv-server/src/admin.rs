//! Canonical admin and membership gRPC services.
use crate::runtime::Runtime;
use raft_net::pb::{admin as a, membership as m};
use std::sync::Arc;
use tonic::{Request, Response, Status};
pub struct Services(pub Arc<Runtime>);
impl Services {
    fn identity<T>(&self, request: &Request<T>) -> String {
        let token = request
            .metadata()
            .get("authorization")
            .and_then(|m| m.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "));
        self.0
            .cfg
            .admin_users
            .iter()
            .find(|u| Some(u.token.as_str()) == token)
            .map(|u| u.name.clone())
            .unwrap_or_else(|| {
                if self.0.cfg.admin_token.is_some() {
                    "admin".into()
                } else {
                    "local-development".into()
                }
            })
    }
    fn audit(
        &self,
        identity: &str,
        action: &str,
        target: &str,
        result: &str,
        id: u64,
    ) -> Result<(), Status> {
        self.0
            .audit(identity, action, target, result, id)
            .map_err(|e| Status::internal(format!("audit failed: {e}")))
    }
    fn authorize<T>(&self, request: &Request<T>, required: &str) -> Result<(), Status> {
        if self.0.cfg.admin_token.is_none() && self.0.cfg.admin_users.is_empty() {
            return Ok(());
        }
        let supplied = request
            .metadata()
            .get("authorization")
            .and_then(|m| m.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "));
        if supplied == self.0.cfg.admin_token.as_deref() && supplied.is_some() {
            return Ok(());
        }
        let user = self
            .0
            .cfg
            .admin_users
            .iter()
            .find(|u| Some(u.token.as_str()) == supplied)
            .ok_or_else(|| Status::unauthenticated("admin token required"))?;
        let rank = |r: &str| match r {
            "admin" => 3,
            "operator" => 2,
            "viewer" => 1,
            _ => 0,
        };
        if rank(&user.role) < rank(required) {
            return Err(Status::permission_denied(
                "role does not permit this operation",
            ));
        }
        Ok(())
    }
}
#[tonic::async_trait]
impl a::admin_server::Admin for Services {
    async fn status(
        &self,
        request: Request<a::StatusRequest>,
    ) -> Result<Response<a::StatusResponse>, Status> {
        self.authorize(&request, "viewer")?;
        let s = self.0.status();
        let members = self
            .0
            .members
            .lock()
            .values()
            .map(|m| a::NodeInfo {
                id: m.id,
                addr: m.raft_addr.clone(),
                voter: !m.learner,
            })
            .collect();
        Ok(Response::new(a::StatusResponse {
            node_id: self.0.local_id(),
            role: s["role"].as_str().unwrap_or("unknown").into(),
            term: s["term"].as_u64().unwrap_or(0),
            commit_index: s["commitIndex"].as_u64().unwrap_or(0),
            applied_index: s["appliedIndex"].as_u64().unwrap_or(0),
            last_log_index: s["lastLogIndex"].as_u64().unwrap_or(0),
            leader_id: self.0.leader_id().unwrap_or(0),
            members,
        }))
    }
    async fn trigger_snapshot(
        &self,
        request: Request<a::TriggerSnapshotRequest>,
    ) -> Result<Response<a::TriggerSnapshotResponse>, Status> {
        self.authorize(&request, "operator")?;
        let identity = self.identity(&request);
        let id = self.0.trace_id();
        self.audit(&identity, "snapshot", "local", "attempted", id)?;
        let result = self
            .0
            .trigger_snapshot()
            .map_err(|e| Status::internal(e.to_string()));
        self.audit(
            &identity,
            "snapshot",
            "local",
            if result.is_ok() { "success" } else { "failed" },
            id,
        )?;
        result?;
        Ok(Response::new(a::TriggerSnapshotResponse { accepted: true }))
    }
}
#[tonic::async_trait]
impl m::membership_server::Membership for Services {
    async fn add_server(
        &self,
        request: Request<m::AddServerRequest>,
    ) -> Result<Response<m::AddServerResponse>, Status> {
        self.authorize(&request, "admin")?;
        let identity = self.identity(&request);
        let id = self.0.trace_id();
        self.audit(&identity, "add_member", "membership", "attempted", id)?;
        let Some(server) = request.into_inner().server else {
            self.audit(&identity, "add_member", "membership", "invalid", id)?;
            return Err(Status::invalid_argument("server required"));
        };
        let member = raft_core::config::Member {
            id: server.id,
            raft_addr: server.addr,
            client_addr: server.client_addr,
            admin_addr: server.admin_addr,
            learner: !server.voter,
            certificate_sha256: (!server.certificate_sha256.is_empty())
                .then_some(server.certificate_sha256),
        };
        let result = self
            .0
            .member_change(member)
            .await
            .map_err(Status::failed_precondition);
        self.audit(
            &identity,
            "add_member",
            "membership",
            if result.is_ok() { "success" } else { "failed" },
            id,
        )?;
        result?;
        Ok(Response::new(m::AddServerResponse {
            status: m::Status::Ok as i32,
            leader_hint: String::new(),
        }))
    }
    async fn remove_server(
        &self,
        request: Request<m::RemoveServerRequest>,
    ) -> Result<Response<m::RemoveServerResponse>, Status> {
        self.authorize(&request, "admin")?;
        let identity = self.identity(&request);
        let id = self.0.trace_id();
        let target = request.get_ref().id.to_string();
        self.audit(&identity, "remove_member", &target, "attempted", id)?;
        let result = self
            .0
            .remove_member(request.into_inner().id)
            .await
            .map_err(Status::failed_precondition);
        self.audit(
            &identity,
            "remove_member",
            &target,
            if result.is_ok() { "success" } else { "failed" },
            id,
        )?;
        result?;
        Ok(Response::new(m::RemoveServerResponse {
            status: m::Status::Ok as i32,
            leader_hint: String::new(),
        }))
    }
    async fn transfer_leadership(
        &self,
        request: Request<m::TransferLeadershipRequest>,
    ) -> Result<Response<m::TransferLeadershipResponse>, Status> {
        self.authorize(&request, "operator")?;
        let identity = self.identity(&request);
        let id = self.0.trace_id();
        let target = request.get_ref().target_id.to_string();
        self.audit(&identity, "transfer", &target, "attempted", id)?;
        let result = self
            .0
            .transfer(request.into_inner().target_id)
            .map_err(Status::failed_precondition);
        self.audit(
            &identity,
            "transfer",
            &target,
            if result.is_ok() { "accepted" } else { "failed" },
            id,
        )?;
        result?;
        Ok(Response::new(m::TransferLeadershipResponse {
            status: m::Status::Ok as i32,
        }))
    }
}
