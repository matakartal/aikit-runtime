use aikit_core::{
    Session, SessionExecutionLease, SessionStore, SessionStoreError, SessionStoreResult,
};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Clone)]
struct PersistedClaim {
    session_id: String,
    owner: String,
    token: String,
    expires_at_unix_ms: u64,
}

#[derive(Default)]
struct State {
    sessions: HashMap<String, Session>,
    claim: Option<PersistedClaim>,
}

#[derive(Default)]
struct CustomSessionStore {
    state: Mutex<State>,
}

impl CustomSessionStore {
    fn replace_claim_for_test(
        &self,
        session: Session,
        owner: &str,
    ) -> SessionStoreResult<SessionExecutionLease> {
        let claim = SessionExecutionLease::issue_for_store(session, owner)?;
        self.state.lock().unwrap().claim = Some(PersistedClaim {
            session_id: claim.session().id.clone(),
            owner: claim.owner().to_owned(),
            token: claim.fencing_token().to_owned(),
            expires_at_unix_ms: claim.expires_at_unix_ms(),
        });
        Ok(claim)
    }
}

impl SessionStore for CustomSessionStore {
    fn create_session(&self, mut session: Session) -> SessionStoreResult<Session> {
        let mut state = self.state.lock().unwrap();
        if let Some(current) = state.sessions.get(&session.id) {
            return Err(conflict(&session, current.revision));
        }
        session.revision = 1;
        state.sessions.insert(session.id.clone(), session.clone());
        Ok(session)
    }

    fn load_session(&self, session_id: &str) -> SessionStoreResult<Session> {
        self.state
            .lock()
            .unwrap()
            .sessions
            .get(session_id)
            .cloned()
            .ok_or_else(|| SessionStoreError::NotFound {
                id: session_id.to_owned(),
            })
    }

    fn compare_and_swap(
        &self,
        expected_revision: u64,
        mut replacement: Session,
    ) -> SessionStoreResult<Session> {
        let mut state = self.state.lock().unwrap();
        let current =
            state
                .sessions
                .get(&replacement.id)
                .ok_or_else(|| SessionStoreError::NotFound {
                    id: replacement.id.clone(),
                })?;
        if current.revision != expected_revision {
            return Err(conflict(&replacement, current.revision));
        }
        replacement.revision = expected_revision + 1;
        state
            .sessions
            .insert(replacement.id.clone(), replacement.clone());
        Ok(replacement)
    }

    fn acquire_execution_lease(
        &self,
        base: Session,
        owner: &str,
    ) -> SessionStoreResult<SessionExecutionLease> {
        let claim = SessionExecutionLease::issue_for_store(base, owner)?;
        let mut state = self.state.lock().unwrap();
        if state.claim.is_some() {
            return Err(conflict(claim.session(), claim.session().revision + 1));
        }
        state.claim = Some(PersistedClaim {
            session_id: claim.session().id.clone(),
            owner: claim.owner().to_owned(),
            token: claim.fencing_token().to_owned(),
            expires_at_unix_ms: claim.expires_at_unix_ms(),
        });
        Ok(claim)
    }

    fn commit_execution_lease(&self, claim: SessionExecutionLease) -> SessionStoreResult<Session> {
        let mut state = self.state.lock().unwrap();
        let persisted = state
            .claim
            .as_ref()
            .ok_or_else(|| conflict(claim.session(), claim.session().revision + 1))?;
        if persisted.session_id != claim.session().id
            || persisted.owner != claim.owner()
            || persisted.token != claim.fencing_token()
            || persisted.expires_at_unix_ms != claim.expires_at_unix_ms()
        {
            return Err(conflict(claim.session(), claim.session().revision + 1));
        }

        let mut session = claim.into_session();
        let actual_revision = state
            .sessions
            .get(&session.id)
            .map_or(0, |current| current.revision);
        if session.revision != actual_revision {
            return Err(conflict(&session, actual_revision));
        }
        session.revision = actual_revision + 1;
        state.claim = None;
        state.sessions.insert(session.id.clone(), session.clone());
        Ok(session)
    }
}

fn conflict(session: &Session, actual_revision: u64) -> SessionStoreError {
    SessionStoreError::Conflict {
        id: session.id.clone(),
        expected_revision: session.revision,
        actual_revision,
    }
}

#[test]
fn external_store_can_issue_persist_fence_and_consume_a_claim() {
    let store = CustomSessionStore::default();
    let stale = store
        .acquire_execution_lease(Session::new("custom-store", Vec::new()), "same-owner")
        .unwrap();
    let stale_token = stale.fencing_token().to_owned();

    let mut current = store
        .replace_claim_for_test(stale.session().clone(), "same-owner")
        .unwrap();
    assert_ne!(stale_token, current.fencing_token());
    assert!(current.expires_at_unix_ms() > 0);
    assert!(matches!(
        store.commit_execution_lease(stale),
        Err(SessionStoreError::Conflict { .. })
    ));

    current
        .session_mut()
        .metadata
        .insert("committed".into(), true.into());
    let saved = store.commit_execution_lease(current).unwrap();
    assert_eq!(saved.revision, 1);
    assert_eq!(saved.metadata.get("committed"), Some(&true.into()));
}
