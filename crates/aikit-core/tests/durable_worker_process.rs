use aikit_core::{
    CancellationToken, DurabilityMode, DurableActivity, DurableStore, DurableWorker,
    DurableWorkerConfig, DurableWorkerError, DurableWorkerOutcome, RunState, SideEffectClass,
    SqliteDurableStore,
};
use serde_json::{json, Value};
use std::io::{Read as _, Write as _};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

const CHILD_ENV: &str = "AIKIT_DURABLE_WORKER_CHILD";
const DATABASE_ENV: &str = "AIKIT_DURABLE_WORKER_DATABASE";
const READY_ENV: &str = "AIKIT_DURABLE_WORKER_READY";
const EFFECT_ENV: &str = "AIKIT_DURABLE_WORKER_EFFECT";
const RUN_ID: &str = "durable-worker-process-run";

struct ChildCleanup {
    child: Option<Child>,
}

impl ChildCleanup {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("child has not been reaped")
    }

    fn kill_and_wait(mut self) -> std::io::Result<ExitStatus> {
        let mut child = self.child.take().expect("child has not been reaped");
        let kill_result = child.kill();
        let wait_result = child.wait();
        kill_result?;
        wait_result
    }
}

impl Drop for ChildCleanup {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
            }
            let _ = child.wait();
        }
    }
}

fn worker_config(owner_id: &str) -> DurableWorkerConfig {
    DurableWorkerConfig {
        owner_id: owner_id.to_string(),
        lease_ttl: Duration::from_millis(500),
        heartbeat_interval: Duration::from_millis(80),
        initial_poll_backoff: Duration::from_millis(5),
        max_poll_backoff: Duration::from_millis(10),
        max_poll_attempts: 1,
        cancellation_grace: Duration::from_millis(50),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_activity_writer_child() {
    if std::env::var_os(CHILD_ENV).is_none() {
        return;
    }

    let database = std::env::var_os(DATABASE_ENV).expect("child database path is required");
    let ready = std::env::var_os(READY_ENV).expect("child readiness path is required");
    let effect = std::env::var_os(EFFECT_ENV).expect("child effect path is required");
    let store: Arc<dyn DurableStore> =
        Arc::new(SqliteDurableStore::open(database).expect("child opens SQLite durable store"));
    let worker = DurableWorker::new(store, worker_config("process-worker"))
        .expect("child creates durable worker");

    let result = worker
        .run(
            RUN_ID,
            CancellationToken::new(),
            move |driver, _| async move {
                let activity = driver
                    .begin_activity(
                        "completed-result-v1",
                        "result-1",
                        json!({"input": "stable"}),
                        SideEffectClass::Pure,
                        None,
                    )
                    .expect("child starts activity");
                let DurableActivity::Execute {
                    activity_id,
                    attempt,
                    ..
                } = activity
                else {
                    panic!("first process must execute the activity")
                };

                let mut marker = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(effect)
                    .expect("child opens execution marker");
                marker
                    .write_all(b"executed\n")
                    .expect("child records one execution");
                marker.sync_all().expect("child syncs execution marker");

                driver
                    .complete_activity(
                        &activity_id,
                        attempt,
                        json!({"result": "persisted-before-kill"}),
                    )
                    .expect("child persists completed result");
                std::fs::write(ready, b"ready").expect("child publishes readiness marker");
                std::future::pending::<Value>().await
            },
        )
        .await;
    panic!("child must be force-killed while holding its claim: {result:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_kill_fences_concurrent_owner_and_reuses_completed_result_after_restart() {
    let directory = tempfile::tempdir().expect("temporary worker directory");
    let database = directory.path().join("durable-worker.db");
    let ready = directory.path().join("ready");
    let effect = directory.path().join("executions");
    let initial = SqliteDurableStore::open(&database).expect("create SQLite durable store");
    initial
        .create(
            &RunState::new(
                "durable-worker-process-session",
                RUN_ID,
                DurabilityMode::Sync,
            )
            .expect("create process run"),
        )
        .expect("persist process run");
    drop(initial);

    let executable = std::env::current_exe().expect("current integration-test executable");
    let child = Command::new(executable)
        .arg("--exact")
        .arg("completed_activity_writer_child")
        .arg("--nocapture")
        .env(CHILD_ENV, "1")
        .env(DATABASE_ENV, &database)
        .env(READY_ENV, &ready)
        .env(EFFECT_ENV, &effect)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn durable worker child");
    let mut child = ChildCleanup::new(child);

    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready.is_file() && Instant::now() < deadline {
        if let Some(status) = child.child_mut().try_wait().expect("poll worker child") {
            let mut stderr = String::new();
            if let Some(mut pipe) = child.child_mut().stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            panic!("durable worker child exited early with {status}: {stderr}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(ready.is_file(), "durable worker child did not become ready");

    let live_store: Arc<dyn DurableStore> =
        Arc::new(SqliteDurableStore::open(&database).expect("open live worker store"));
    let live_state = live_store.load(RUN_ID).expect("load child-owned run");
    assert_eq!(
        live_state
            .worker_lease()
            .map(|lease| lease.owner_id.as_str()),
        Some("process-worker")
    );
    let contender = DurableWorker::new(live_store, worker_config("concurrent-worker"))
        .expect("create contender");
    let contender_result = contender
        .run(RUN_ID, CancellationToken::new(), |_, _| async {
            panic!("concurrent worker must not execute while the child lease is active")
        })
        .await;
    assert!(matches!(
        contender_result,
        Err(DurableWorkerError::ClaimUnavailable {
            owner_id: Some(ref owner_id),
            ..
        }) if owner_id == "process-worker"
    ));

    let status = child
        .kill_and_wait()
        .expect("force-kill and reap durable worker child");
    assert!(
        !status.success(),
        "worker child must be terminated forcibly"
    );
    tokio::time::sleep(Duration::from_millis(700)).await;

    let restart_store: Arc<dyn DurableStore> =
        Arc::new(SqliteDurableStore::open(&database).expect("reopen store after process kill"));
    let restart = DurableWorker::new(restart_store.clone(), worker_config("restart-worker"))
        .expect("create restarted worker");
    let outcome = restart
        .run(RUN_ID, CancellationToken::new(), |driver, _| async move {
            match driver
                .begin_activity(
                    "completed-result-v1",
                    "result-1",
                    json!({"input": "stable"}),
                    SideEffectClass::Pure,
                    None,
                )
                .expect("restart loads completed activity")
            {
                DurableActivity::ReuseCompleted { output, .. } => output,
                DurableActivity::Execute { .. } => {
                    panic!("restart must reuse the committed result")
                }
            }
        })
        .await
        .expect("restart claims the expired lease");
    assert_eq!(
        outcome,
        DurableWorkerOutcome::Executed {
            value: json!({"result": "persisted-before-kill"}),
            recovered_claim: true,
        }
    );
    assert_eq!(
        std::fs::read_to_string(&effect).expect("read execution marker"),
        "executed\n"
    );
    assert!(restart_store.load(RUN_ID).unwrap().worker_lease().is_none());
}
