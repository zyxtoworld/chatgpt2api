use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::sync::Mutex;

use super::{AppInitError, read_account_snapshot};

pub(super) type AccountModelGroup = String;

#[derive(Clone, Debug)]
pub(super) struct AccountRecord {
    pub(super) token: String,
    pub(super) status: String,
    pub(super) source_type: String,
    pub(super) chatgpt_account_id: Option<String>,
    pub(super) account_type: String,
    pub(super) models: Vec<String>,
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub(super) struct CatalogAccountCandidate {
    pub(super) token: String,
    pub(super) source_type: String,
    pub(super) chatgpt_account_id: Option<String>,
}

pub(super) struct AccountSlot {
    pub(super) record: AccountRecord,
    pub(super) inflight: AtomicUsize,
}

#[derive(Clone)]
pub(super) struct AccountSnapshot {
    pub(super) generation: u64,
    pub(super) fingerprint: [u8; 32],
    pub(super) valid: bool,
    pub(super) accounts: Arc<Vec<Arc<AccountSlot>>>,
}

pub(super) struct AccountLease {
    pub(super) slot: Arc<AccountSlot>,
}

impl AccountLease {
    pub(super) fn token(&self) -> &str {
        &self.slot.record.token
    }

    pub(super) fn source_type(&self) -> &str {
        &self.slot.record.source_type
    }

    pub(super) fn account_type(&self) -> &str {
        &self.slot.record.account_type
    }

    pub(super) fn chatgpt_account_id(&self) -> Option<&str> {
        self.slot.record.chatgpt_account_id.as_deref()
    }
}

impl Drop for AccountLease {
    fn drop(&mut self) {
        let _ = self
            .slot
            .inflight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_sub(1)
            });
    }
}

#[derive(Clone)]
pub(super) struct AccountStore {
    pub(super) path: Option<Arc<PathBuf>>,
    pub(super) snapshot: Arc<RwLock<AccountSnapshot>>,
    reload_gate: Arc<Mutex<()>>,
    pub(super) cursor: Arc<AtomicUsize>,
}

#[derive(Default)]
pub(super) struct AccountHealthSummary {
    pub(super) total: usize,
    pub(super) active: usize,
    pub(super) limited: usize,
    pub(super) abnormal: usize,
    pub(super) disabled: usize,
}

impl AccountStore {
    pub(super) fn load(path: Option<&Path>) -> Result<Self, AppInitError> {
        let (records, fingerprint) = if let Some(path) = path {
            read_account_snapshot(path)?
        } else {
            (Vec::new(), [0; 32])
        };
        let accounts = Arc::new(account_slots(records));
        Ok(Self {
            path: path.map(|path| Arc::new(path.to_owned())),
            snapshot: Arc::new(RwLock::new(AccountSnapshot {
                generation: 0,
                fingerprint,
                valid: true,
                accounts,
            })),
            reload_gate: Arc::new(Mutex::new(())),
            cursor: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub(super) async fn reload(&self) -> bool {
        let Some(path) = self.path.clone() else {
            return true;
        };
        let _reload_guard = self.reload_gate.lock().await;
        let result = tokio::task::spawn_blocking(move || read_account_snapshot(&path)).await;
        let mut snapshot = self.snapshot.write().expect("account snapshot lock");
        match result {
            Ok(Ok((accounts, fingerprint))) => {
                if !snapshot.valid || snapshot.fingerprint != fingerprint {
                    snapshot.generation = snapshot.generation.saturating_add(1);
                    snapshot.fingerprint = fingerprint;
                    snapshot.accounts = Arc::new(account_slots(accounts));
                    snapshot.valid = true;
                }
                true
            }
            _ => {
                snapshot.generation = snapshot.generation.saturating_add(1);
                snapshot.valid = false;
                snapshot.accounts = Arc::new(Vec::new());
                false
            }
        }
    }

    #[cfg(test)]
    pub(super) async fn acquire_excluding_with_type_filter(
        &self,
        model: &str,
        excluded_tokens: &HashSet<String>,
        allowed_groups: Option<&HashSet<AccountModelGroup>>,
    ) -> Option<AccountLease> {
        self.acquire_excluding_with_type_and_source_filter(
            model,
            excluded_tokens,
            allowed_groups,
            None,
        )
        .await
    }

    pub(super) async fn acquire_excluding_with_type_and_source_filter(
        &self,
        model: &str,
        excluded_tokens: &HashSet<String>,
        allowed_groups: Option<&HashSet<AccountModelGroup>>,
        required_source_type: Option<&str>,
    ) -> Option<AccountLease> {
        if !self.reload().await {
            return None;
        }
        let snapshot = self.snapshot.read().expect("account snapshot lock");
        if !snapshot.valid || snapshot.accounts.is_empty() {
            return None;
        }
        let accounts = snapshot.accounts.as_ref();
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        for offset in 0..accounts.len() {
            let slot = accounts[(start.wrapping_add(offset)) % accounts.len()].clone();
            if excluded_tokens.contains(&slot.record.token)
                || required_source_type.is_some_and(|source| slot.record.source_type != source)
                || matches!(slot.record.status.as_str(), "禁用" | "限流" | "异常")
            {
                continue;
            }
            if let Some(allowed_groups) = allowed_groups {
                if !allowed_groups.contains(&slot.record.account_type) {
                    continue;
                }
            } else if model != "auto"
                && !slot.record.models.is_empty()
                && !slot
                    .record
                    .models
                    .iter()
                    .any(|candidate| candidate == model)
            {
                continue;
            }
            slot.inflight.fetch_add(1, Ordering::AcqRel);
            return Some(AccountLease { slot });
        }
        None
    }

    #[cfg(test)]
    pub(super) async fn acquire(&self, model: &str) -> Option<AccountLease> {
        self.acquire_excluding_with_type_filter(model, &HashSet::new(), None)
            .await
    }

    pub(super) async fn active_type_candidates(
        &self,
    ) -> Option<(
        u64,
        HashMap<AccountModelGroup, Vec<CatalogAccountCandidate>>,
    )> {
        if !self.reload().await {
            return None;
        }
        let snapshot = self.snapshot.read().expect("account snapshot lock");
        if !snapshot.valid {
            return None;
        }
        let mut groups = HashMap::<AccountModelGroup, Vec<CatalogAccountCandidate>>::new();
        for slot in snapshot.accounts.iter() {
            if slot.record.status != "正常" {
                continue;
            }
            groups
                .entry(slot.record.account_type.clone())
                .or_default()
                .push(CatalogAccountCandidate {
                    token: slot.record.token.clone(),
                    source_type: slot.record.source_type.clone(),
                    chatgpt_account_id: slot.record.chatgpt_account_id.clone(),
                });
        }
        for candidates in groups.values_mut() {
            candidates.sort_by(|left, right| {
                fn source_priority(source: &str) -> u8 {
                    match source {
                        "web" | "password" | "password-oauth" => 0,
                        "codex" => 1,
                        _ => 2,
                    }
                }
                source_priority(&left.source_type)
                    .cmp(&source_priority(&right.source_type))
                    .then_with(|| {
                        left.token
                            .cmp(&right.token)
                            .then_with(|| left.chatgpt_account_id.cmp(&right.chatgpt_account_id))
                    })
            });
            candidates.dedup();
        }
        Some((snapshot.generation, groups))
    }

    pub(super) fn health_summary(&self) -> AccountHealthSummary {
        let snapshot = self.snapshot.read().expect("account snapshot lock");
        let mut summary = AccountHealthSummary {
            total: snapshot.accounts.len(),
            ..AccountHealthSummary::default()
        };
        for slot in snapshot.accounts.iter() {
            match slot.record.status.as_str() {
                "正常" => summary.active += 1,
                "限流" => summary.limited += 1,
                "异常" => summary.abnormal += 1,
                "禁用" => summary.disabled += 1,
                _ => {}
            }
        }
        summary
    }

    #[cfg(test)]
    pub(super) fn inflight(&self) -> usize {
        self.snapshot
            .read()
            .expect("account snapshot lock")
            .accounts
            .iter()
            .map(|slot| slot.inflight.load(Ordering::Acquire))
            .sum()
    }
}

fn account_slots(records: Vec<AccountRecord>) -> Vec<Arc<AccountSlot>> {
    records
        .into_iter()
        .map(|record| {
            Arc::new(AccountSlot {
                record,
                inflight: AtomicUsize::new(0),
            })
        })
        .collect()
}
