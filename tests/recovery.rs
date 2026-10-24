mod common;

use std::time::Duration;

use common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_follower_recovers_store_and_catches_up() {
    let c = cluster(Duration::ZERO).await;
    let leader = c.wait_for_leader(T).await.unwrap();
    let f = others(leader)[0];

    chat(&c, leader, "one").await;
    chat(&c, leader, "two").await;
    c.crash(f);
    chat(&c, leader, "three, while a follower is down").await;

    let node = c.restart(f).await.unwrap();
    assert!(!node.is_crashed());
    // The follower kept "one"/"two" on disk; only the entry decided while it
    // was down needs to sync from the leader.
    let msgs = converged(&c, &[1, 2, 3], 6).await;
    assert_one_reply_each(&msgs);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_old_leader_recovers_and_counts_for_quorum() {
    let c = cluster(Duration::ZERO).await;
    let old = c.wait_for_leader(T).await.unwrap();
    let survivors = others(old);

    chat(&c, old, "before the crash").await;
    c.crash(old);
    leader_among(&c, &survivors).await;
    chat(&c, survivors[0], "after the crash").await;

    c.restart(old).await.unwrap();
    converged(&c, &[1, 2, 3], 4).await;

    // The restarted node is a full member again: with another node down,
    // it is needed for the majority, and the chat still makes progress.
    let other = survivors[1];
    c.crash(other);
    let alive = [old, survivors[0]];
    leader_among(&c, &alive).await;
    let (_, reply) = chat(&c, old, "needs the restarted node for quorum").await;
    assert!(reply.content.contains("turn 3"), "{}", reply.content);
    let msgs = converged(&c, &alive, 6).await;
    assert_one_reply_each(&msgs);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn whole_cluster_restart_keeps_conversation() {
    let c = cluster(Duration::ZERO).await;
    let leader = c.wait_for_leader(T).await.unwrap();
    chat(&c, leader, "my name is Ada").await;

    // Nobody is left to sync from: the log can only come back from disk.
    for pid in [1, 2, 3] {
        c.crash(pid);
    }
    for pid in [1, 2, 3] {
        c.restart(pid).await.unwrap();
    }
    let leader = c.wait_for_leader(T).await.unwrap();
    let (_, reply) = chat(&c, leader, "what is my name?").await;
    assert!(reply.content.contains("my name is Ada"), "{}", reply.content);
    let msgs = converged(&c, &[1, 2, 3], 4).await;
    assert_one_reply_each(&msgs);
}
