mod common;

use std::time::Duration;

use common::*;
use omni_chat::{
    chat::generate::{Draft, DraftStatus},
    chat::PostOutcome,
};
use tokio::{sync::watch, time::Instant};

/// Collect every draft state published on `rx` until `stop` returns true.
async fn collect_until(
    rx: &mut watch::Receiver<Option<Draft>>,
    stop: impl Fn(&Draft) -> bool,
) -> Vec<Draft> {
    let mut seen = Vec::new();
    let deadline = Instant::now() + T;
    loop {
        if let Some(d) = rx.borrow_and_update().clone() {
            let done = stop(&d);
            seen.push(d);
            if done {
                return seen;
            }
        }
        tokio::select! {
            r = rx.changed() => r.expect("draft channel open"),
            _ = tokio::time::sleep_until(deadline) => panic!("timed out; saw {seen:#?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reply_streams_then_commits() {
    let c = cluster(Duration::from_millis(400)).await;
    let leader = c.wait_for_leader(T).await.unwrap();
    let mut rx = c.node(leader).shared.draft.subscribe();

    let via = c.node(others(leader)[0]).shared.clone();
    let post = tokio::spawn(async move { via.post("stream this".into(), None, T).await.unwrap() });
    let seen = collect_until(&mut rx, |d| d.status != DraftStatus::Streaming).await;

    let last = seen.last().unwrap();
    assert_eq!(last.status, DraftStatus::Committed);
    assert!(last.first_token_ms.is_some());
    let partial = seen
        .iter()
        .filter(|d| d.status == DraftStatus::Streaming && !d.text.is_empty())
        .count();
    assert!(
        partial >= 2,
        "expected several partial states, saw {partial}"
    );
    for w in seen.windows(2) {
        assert!(w[1].text.starts_with(&w[0].text), "text only grows");
    }

    match post.await.unwrap() {
        PostOutcome::Replied { reply, .. } => {
            assert_eq!(
                reply.content, last.text,
                "the committed reply is the streamed text"
            );
            assert_eq!(reply.node, leader);
        }
        o => panic!("{o:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_mid_stream_aborts_draft_and_new_leader_regenerates() {
    let c = cluster(Duration::from_millis(1500)).await;
    let old = c.wait_for_leader(T).await.unwrap();
    let survivors = others(old);
    let mut old_rx = c.node(old).shared.draft.subscribe();

    let via = c.node(survivors[0]).shared.clone();
    let post = tokio::spawn(async move { via.post("interrupt me".into(), None, T).await.unwrap() });
    collect_until(&mut old_rx, |d| !d.text.is_empty()).await;
    c.crash(old);

    // Aborting the generator task drops the guard, which marks the draft aborted.
    let after = collect_until(&mut old_rx, |d| d.status != DraftStatus::Streaming).await;
    assert_eq!(after.last().unwrap().status, DraftStatus::Aborted);

    match post.await.unwrap() {
        PostOutcome::Replied { reply, .. } => assert!(survivors.contains(&reply.node)),
        o => panic!("{o:?}"),
    }
    let msgs = converged(&c, &survivors, 2).await;
    assert_one_reply_each(&msgs);
}
