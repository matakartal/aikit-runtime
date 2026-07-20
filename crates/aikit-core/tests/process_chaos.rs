use aikit_core::{DurabilityMode, DurableStore, RunState, SqliteDurableStore};
use serde_json::json;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const CHILD_ENV: &str = "AIKIT_PROCESS_CHAOS_CHILD";
const DATABASE_ENV: &str = "AIKIT_PROCESS_CHAOS_DATABASE";
const READY_ENV: &str = "AIKIT_PROCESS_CHAOS_READY";
const RUN_ID: &str = "process-chaos-run";

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

#[test]
fn checkpoint_writer_child() {
    if std::env::var_os(CHILD_ENV).is_none() {
        return;
    }

    let database = std::env::var_os(DATABASE_ENV).expect("child database path is required");
    let ready = std::env::var_os(READY_ENV).expect("child readiness path is required");
    let store = SqliteDurableStore::open(database).expect("child opens SQLite durable store");
    let mut state = RunState::new("process-chaos-session", RUN_ID, DurabilityMode::Sync)
        .expect("child creates durable run");
    state
        .replace_state("before-kill", json!({"phase": "checkpointed"}))
        .expect("child records projected state");
    state
        .checkpoint(
            "before-kill",
            Some("committed before forced termination".into()),
        )
        .expect("child commits checkpoint event");
    store.create(&state).expect("child persists committed run");

    std::fs::write(ready, b"ready").expect("child publishes readiness marker");
    loop {
        thread::park_timeout(Duration::from_secs(60));
    }
}

#[test]
fn committed_checkpoint_survives_process_kill_and_resumes_append_only() {
    let directory = tempfile::tempdir().expect("temporary chaos directory");
    let database = directory.path().join("durable.db");
    let ready = directory.path().join("ready");
    let executable = std::env::current_exe().expect("current integration-test executable");

    let child = Command::new(executable)
        .arg("--exact")
        .arg("checkpoint_writer_child")
        .arg("--nocapture")
        .env(CHILD_ENV, "1")
        .env(DATABASE_ENV, &database)
        .env(READY_ENV, &ready)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn checkpoint writer child");
    let mut child = ChildCleanup::new(child);

    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready.is_file() && Instant::now() < deadline {
        if let Some(status) = child.child_mut().try_wait().expect("poll child") {
            let stderr = child
                .child_mut()
                .stderr
                .take()
                .map(|mut stderr| {
                    use std::io::Read as _;
                    let mut output = String::new();
                    let _ = stderr.read_to_string(&mut output);
                    output
                })
                .unwrap_or_default();
            panic!("checkpoint writer exited early with {status}: {stderr}");
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(ready.is_file(), "checkpoint writer did not become ready");

    let status = child
        .kill_and_wait()
        .expect("force-kill and reap checkpoint writer");
    assert!(!status.success(), "chaos child must be terminated forcibly");

    let first_recovery =
        SqliteDurableStore::open(&database).expect("open store after process kill");
    let recovered = first_recovery
        .load(RUN_ID)
        .expect("load committed checkpoint after process kill");
    assert_eq!(
        recovered.projection().state,
        json!({"phase": "checkpointed"})
    );
    assert!(recovered.projection().current_checkpoint_id.is_some());

    let expected_sequence = recovered
        .events()
        .last()
        .expect("run-start and checkpoint events exist")
        .sequence;
    let mut resumed = recovered;
    resumed
        .replace_state("after-restart", json!({"phase": "resumed"}))
        .expect("append state after process restart");
    first_recovery
        .compare_and_swap(expected_sequence, &resumed)
        .expect("append-only CAS succeeds after process restart");

    let second_recovery =
        SqliteDurableStore::open(&database).expect("open independent recovery connection");
    assert_eq!(
        second_recovery.load(RUN_ID).expect("reload resumed run"),
        resumed
    );
}
