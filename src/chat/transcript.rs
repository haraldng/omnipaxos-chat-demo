//! Deterministic fold of the decided log into a chat transcript.
//!
//! Every replica applies the same decided entries in the same order, so these
//! rules yield identical transcripts everywhere:
//!
//! 1. A `User` whose `msg_id` or `client_msg_id` was already seen is ignored.
//! 2. The **first** decided `Assistant` for a given `in_reply_to` wins; later
//!    ones are ignored. (An old leader's in-flight reply may still be decided
//!    after failover — this rule, not the generator, guarantees one reply.)
//! 3. Replies pair with user messages by `in_reply_to`, not by position.

use std::collections::HashMap;

use super::message::{ChatMessage, MsgId, Role};
use crate::omni_paxos::ChatEntry;

/// A user message and (once decided) its single reply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Turn {
    pub user: ChatMessage,
    pub reply: Option<ChatMessage>,
}

/// What applying one entry did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Applied {
    User(MsgId),
    Assistant {
        in_reply_to: MsgId,
    },
    /// Dropped by a dedupe rule (or an orphan reply).
    Ignored,
}

#[derive(Clone, Debug, Default)]
pub struct Transcript {
    turns: Vec<Turn>,
    by_msg_id: HashMap<MsgId, usize>,
    by_client_id: HashMap<String, MsgId>,
    /// Number of decided log entries folded so far (== next log index).
    applied_len: usize,
    duplicates_ignored: usize,
}

impl Transcript {
    /// Fold the decided entry at log index `self.applied_len()`.
    pub fn apply(&mut self, entry: &ChatEntry) -> Applied {
        let log_idx = self.applied_len;
        self.applied_len += 1;
        let applied = match entry {
            ChatEntry::User {
                msg_id,
                client_msg_id,
                content,
                client_node,
                ts,
            } => {
                let dup_client = client_msg_id
                    .as_ref()
                    .is_some_and(|c| self.by_client_id.contains_key(c));
                if dup_client || self.by_msg_id.contains_key(msg_id) {
                    Applied::Ignored
                } else {
                    if let Some(c) = client_msg_id {
                        self.by_client_id.insert(c.clone(), msg_id.clone());
                    }
                    self.by_msg_id.insert(msg_id.clone(), self.turns.len());
                    self.turns.push(Turn {
                        user: ChatMessage {
                            msg_id: msg_id.clone(),
                            role: Role::User,
                            content: content.clone(),
                            log_idx,
                            in_reply_to: None,
                            client_msg_id: client_msg_id.clone(),
                            model: None,
                            node: *client_node,
                            ts: *ts,
                        },
                        reply: None,
                    });
                    Applied::User(msg_id.clone())
                }
            }
            ChatEntry::Assistant {
                msg_id,
                in_reply_to,
                content,
                model,
                generated_by,
                ts,
            } => match self.by_msg_id.get(in_reply_to).copied() {
                Some(i) if self.turns[i].reply.is_none() => {
                    self.turns[i].reply = Some(ChatMessage {
                        msg_id: msg_id.clone(),
                        role: Role::Assistant,
                        content: content.clone(),
                        log_idx,
                        in_reply_to: Some(in_reply_to.clone()),
                        client_msg_id: None,
                        model: Some(model.clone()),
                        node: *generated_by,
                        ts: *ts,
                    });
                    Applied::Assistant {
                        in_reply_to: in_reply_to.clone(),
                    }
                }
                _ => Applied::Ignored,
            },
        };
        if applied == Applied::Ignored {
            self.duplicates_ignored += 1;
        }
        applied
    }

    /// Account for a decided log slot that carries no chat entry (e.g. a stop sign).
    pub fn skip(&mut self) {
        self.applied_len += 1;
    }

    pub fn applied_len(&self) -> usize {
        self.applied_len
    }

    pub fn duplicates_ignored(&self) -> usize {
        self.duplicates_ignored
    }

    pub fn turns(&self) -> &[Turn] {
        &self.turns
    }

    /// Conversation view: each user message followed by its reply.
    pub fn messages(&self) -> Vec<ChatMessage> {
        self.turns
            .iter()
            .flat_map(|t| std::iter::once(t.user.clone()).chain(t.reply.clone()))
            .collect()
    }

    pub fn contains(&self, msg_id: &str) -> bool {
        self.by_msg_id.contains_key(msg_id)
    }

    pub fn msg_id_for_client_id(&self, client_msg_id: &str) -> Option<&MsgId> {
        self.by_client_id.get(client_msg_id)
    }

    pub fn reply_to(&self, msg_id: &str) -> Option<&ChatMessage> {
        let i = *self.by_msg_id.get(msg_id)?;
        self.turns[i].reply.as_ref()
    }

    /// Earliest user message that has no decided reply yet.
    pub fn next_unanswered(&self) -> Option<&ChatMessage> {
        self.turns
            .iter()
            .find(|t| t.reply.is_none())
            .map(|t| &t.user)
    }

    /// Prompt context for answering `msg_id`: every earlier turn (with its reply,
    /// if any) followed by the user message itself.
    pub fn context_for(&self, msg_id: &str) -> Option<Vec<ChatMessage>> {
        let i = *self.by_msg_id.get(msg_id)?;
        let mut out = Vec::with_capacity(2 * i + 1);
        for t in &self.turns[..i] {
            out.push(t.user.clone());
            out.extend(t.reply.clone());
        }
        out.push(self.turns[i].user.clone());
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(id: &str, cid: Option<&str>, content: &str) -> ChatEntry {
        ChatEntry::User {
            msg_id: id.into(),
            client_msg_id: cid.map(Into::into),
            content: content.into(),
            client_node: 1,
            ts: 0,
        }
    }

    fn asst(id: &str, to: &str, content: &str) -> ChatEntry {
        ChatEntry::Assistant {
            msg_id: id.into(),
            in_reply_to: to.into(),
            content: content.into(),
            model: "mock".into(),
            generated_by: 1,
            ts: 0,
        }
    }

    fn fold(log: &[ChatEntry]) -> Transcript {
        let mut t = Transcript::default();
        for e in log {
            t.apply(e);
        }
        t
    }

    fn contents(t: &Transcript) -> Vec<String> {
        t.messages().into_iter().map(|m| m.content).collect()
    }

    #[test]
    fn first_reply_wins() {
        let t = fold(&[
            user("u1", None, "hi"),
            asst("a1", "u1", "old leader"),
            asst("a2", "u1", "new leader"),
        ]);
        assert_eq!(contents(&t), ["hi", "old leader"]);
        assert_eq!(t.duplicates_ignored(), 1);
        assert_eq!(t.applied_len(), 3);
    }

    #[test]
    fn duplicate_client_msg_id_is_dropped() {
        let t = fold(&[
            user("u1", Some("c1"), "hi"),
            user("u2", Some("c1"), "hi"),
            asst("a2", "u2", "orphan reply to dropped dup"),
        ]);
        assert_eq!(contents(&t), ["hi"]);
        assert_eq!(t.duplicates_ignored(), 2);
        assert_eq!(t.msg_id_for_client_id("c1").unwrap(), "u1");
    }

    #[test]
    fn duplicate_msg_id_is_dropped() {
        // Same entry re-appended after an ambiguous `Superseded` error.
        let t = fold(&[user("u1", None, "hi"), user("u1", None, "hi")]);
        assert_eq!(t.turns().len(), 1);
    }

    #[test]
    fn interleaved_replies_pair_by_in_reply_to() {
        let t = fold(&[
            user("uA", None, "A"),
            user("uB", None, "B"),
            asst("aA", "uA", "re A"),
        ]);
        assert_eq!(contents(&t), ["A", "re A", "B"]);
        assert_eq!(t.next_unanswered().unwrap().msg_id, "uB");
        let ctx: Vec<_> = t
            .context_for("uB")
            .unwrap()
            .into_iter()
            .map(|m| m.content)
            .collect();
        assert_eq!(ctx, ["A", "re A", "B"]);
    }

    #[test]
    fn next_unanswered_is_earliest() {
        let t = fold(&[user("uA", None, "A"), user("uB", None, "B")]);
        assert_eq!(t.next_unanswered().unwrap().msg_id, "uA");
        assert_eq!(t.context_for("uA").unwrap().len(), 1);
    }

    #[test]
    fn reply_to_unknown_user_is_ignored() {
        let t = fold(&[asst("a1", "nope", "x")]);
        assert!(t.messages().is_empty());
        assert_eq!(t.duplicates_ignored(), 1);
    }

    #[test]
    fn log_idx_tracks_position() {
        let t = fold(&[user("u1", None, "a"), asst("a1", "u1", "b")]);
        let m = t.messages();
        assert_eq!((m[0].log_idx, m[1].log_idx), (0, 1));
    }
}
