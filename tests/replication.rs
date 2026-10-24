mod common;

use std::time::Duration;

use common::*;
use omni_chat::chat::PostOutcome;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transcript_replicates_to_all_nodes() {
    let c = cluster(Duration::ZERO).await;
    c.wait_for_leader(T).await.unwrap();

    chat(&c, 1, "hello").await;
    let (_, reply) = chat(&c, 1, "how are you").await;
    assert!(reply.content.contains("turn 2"), "{}", reply.content);
    assert!(reply.content.contains("you first said \"hello\""));

    let msgs = converged(&c, &[1, 2, 3], 4).await;
    assert_one_reply_each(&msgs);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_accepts_post_and_leader_generates() {
    let c = cluster(Duration::ZERO).await;
    let leader = c.wait_for_leader(T).await.unwrap();
    let follower = others(leader)[0];

    let (user, reply) = chat(&c, follower, "from a follower").await;
    assert_eq!(user.node, follower);
    assert_eq!(reply.node, leader, "only the leader generates");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_posts_get_exactly_one_reply_each() {
    let c = cluster(Duration::from_millis(5)).await;
    c.wait_for_leader(T).await.unwrap();

    let mut posts = Vec::new();
    for i in 0..10 {
        let shared = c.node(1 + i % 2).shared.clone();
        posts.push(tokio::spawn(async move {
            shared.post(format!("msg {i}"), None, T).await.unwrap()
        }));
    }
    for p in posts {
        assert!(matches!(p.await.unwrap(), PostOutcome::Replied { .. }));
    }
    let msgs = converged(&c, &[1, 2, 3], 20).await;
    assert_one_reply_each(&msgs);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_retry_with_same_id_is_idempotent() {
    let c = cluster(Duration::ZERO).await;
    c.wait_for_leader(T).await.unwrap();

    let cid = Some("client-42".to_string());
    let a = c
        .node(1)
        .shared
        .post("once".into(), cid.clone(), T)
        .await
        .unwrap();
    // Retry on a different node, as a client would after a timeout.
    let b = c.node(3).shared.post("once".into(), cid, T).await.unwrap();
    let id = |o: &PostOutcome| match o {
        PostOutcome::Replied { user, .. } => user.msg_id.clone(),
        PostOutcome::Pending { msg_id } => msg_id.clone(),
    };
    assert_eq!(id(&a), id(&b));
    converged(&c, &[1, 2, 3], 2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_retries_with_same_id_collapse() {
    let c = cluster(Duration::ZERO).await;
    c.wait_for_leader(T).await.unwrap();

    let s1 = c.node(1).shared.clone();
    let s2 = c.node(2).shared.clone();
    let cid = || Some("dup".to_string());
    let (a, b) = tokio::join!(s1.post("x".into(), cid(), T), s2.post("x".into(), cid(), T));
    let (a, b) = (a.unwrap(), b.unwrap());
    let reply = |o: PostOutcome| match o {
        PostOutcome::Replied { reply, .. } => reply,
        o => panic!("{o:?}"),
    };
    assert_eq!(reply(a), reply(b));
    converged(&c, &[1, 2, 3], 2).await;
}
