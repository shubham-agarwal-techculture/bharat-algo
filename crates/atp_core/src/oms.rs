//! Order-lifecycle transition table.
//!
//! This slice is the only place a state may change. `apply` refuses anything
//! the table does not list. Partial fills that arrive while a cancel or
//! replace is pending update quantities and stay pending; a complete fill
//! still moves to FILLED.

use crate::types::OrderState;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Trigger {
    RiskApprove,
    RiskReject,
    Dispatch,
    DispatchUncertain,
    Withdraw,
    Ack,
    BrokerReject,
    FillPartial,
    FillComplete,
    CancelRequest,
    CancelConfirm,
    CancelRejectSent,
    CancelRejectAcked,
    CancelRejectPartial,
    ReplaceRequest,
    ReplaceConfirm,
    ReplaceRejectAcked,
    ReplaceRejectPartial,
    Expire,
    TimeoutUnknown,
    DisconnectUnknown,
    ReconToSent,
    ReconToAcked,
    ReconToRejected,
    ReconToPartial,
    ReconToFilled,
    ReconToCancelled,
    ReconToReplaced,
    ReconToExpired,
    Resend,
}

impl Trigger {
    pub fn all() -> &'static [Trigger] {
        &ALL_TRIGGERS
    }
}

const ALL_TRIGGERS: [Trigger; 30] = [
    Trigger::RiskApprove,
    Trigger::RiskReject,
    Trigger::Dispatch,
    Trigger::DispatchUncertain,
    Trigger::Withdraw,
    Trigger::Ack,
    Trigger::BrokerReject,
    Trigger::FillPartial,
    Trigger::FillComplete,
    Trigger::CancelRequest,
    Trigger::CancelConfirm,
    Trigger::CancelRejectSent,
    Trigger::CancelRejectAcked,
    Trigger::CancelRejectPartial,
    Trigger::ReplaceRequest,
    Trigger::ReplaceConfirm,
    Trigger::ReplaceRejectAcked,
    Trigger::ReplaceRejectPartial,
    Trigger::Expire,
    Trigger::TimeoutUnknown,
    Trigger::DisconnectUnknown,
    Trigger::ReconToSent,
    Trigger::ReconToAcked,
    Trigger::ReconToRejected,
    Trigger::ReconToPartial,
    Trigger::ReconToFilled,
    Trigger::ReconToCancelled,
    Trigger::ReconToReplaced,
    Trigger::ReconToExpired,
    Trigger::Resend,
];

impl fmt::Display for Trigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Single source of truth for the lifecycle. Keep the README table in sync.
pub const TRANSITIONS: &[(OrderState, Trigger, OrderState)] = &[
    (
        OrderState::New,
        Trigger::RiskApprove,
        OrderState::RiskApproved,
    ),
    (
        OrderState::New,
        Trigger::RiskReject,
        OrderState::RiskRejected,
    ),
    (
        OrderState::RiskApproved,
        Trigger::Dispatch,
        OrderState::Sent,
    ),
    (
        OrderState::RiskApproved,
        Trigger::DispatchUncertain,
        OrderState::Unknown,
    ),
    (
        OrderState::RiskApproved,
        Trigger::Withdraw,
        OrderState::Cancelled,
    ),
    (OrderState::Sent, Trigger::Ack, OrderState::Acked),
    (
        OrderState::Sent,
        Trigger::BrokerReject,
        OrderState::Rejected,
    ),
    (
        OrderState::Sent,
        Trigger::FillPartial,
        OrderState::PartiallyFilled,
    ),
    (OrderState::Sent, Trigger::FillComplete, OrderState::Filled),
    (
        OrderState::Sent,
        Trigger::CancelRequest,
        OrderState::CancelPending,
    ),
    (
        OrderState::Sent,
        Trigger::ReplaceRequest,
        OrderState::ReplacePending,
    ),
    (
        OrderState::Sent,
        Trigger::TimeoutUnknown,
        OrderState::Unknown,
    ),
    (
        OrderState::Sent,
        Trigger::DisconnectUnknown,
        OrderState::Unknown,
    ),
    (OrderState::Sent, Trigger::Expire, OrderState::Expired),
    (
        OrderState::Acked,
        Trigger::FillPartial,
        OrderState::PartiallyFilled,
    ),
    (OrderState::Acked, Trigger::FillComplete, OrderState::Filled),
    (
        OrderState::Acked,
        Trigger::CancelRequest,
        OrderState::CancelPending,
    ),
    (
        OrderState::Acked,
        Trigger::ReplaceRequest,
        OrderState::ReplacePending,
    ),
    (OrderState::Acked, Trigger::Expire, OrderState::Expired),
    (
        OrderState::Acked,
        Trigger::DisconnectUnknown,
        OrderState::Unknown,
    ),
    (
        OrderState::PartiallyFilled,
        Trigger::FillPartial,
        OrderState::PartiallyFilled,
    ),
    (
        OrderState::PartiallyFilled,
        Trigger::FillComplete,
        OrderState::Filled,
    ),
    (
        OrderState::PartiallyFilled,
        Trigger::CancelRequest,
        OrderState::CancelPending,
    ),
    (
        OrderState::PartiallyFilled,
        Trigger::ReplaceRequest,
        OrderState::ReplacePending,
    ),
    (
        OrderState::PartiallyFilled,
        Trigger::Expire,
        OrderState::Expired,
    ),
    (
        OrderState::PartiallyFilled,
        Trigger::DisconnectUnknown,
        OrderState::Unknown,
    ),
    (
        OrderState::CancelPending,
        Trigger::CancelConfirm,
        OrderState::Cancelled,
    ),
    (
        OrderState::CancelPending,
        Trigger::CancelRejectSent,
        OrderState::Sent,
    ),
    (
        OrderState::CancelPending,
        Trigger::CancelRejectAcked,
        OrderState::Acked,
    ),
    (
        OrderState::CancelPending,
        Trigger::CancelRejectPartial,
        OrderState::PartiallyFilled,
    ),
    (
        OrderState::CancelPending,
        Trigger::FillComplete,
        OrderState::Filled,
    ),
    (
        OrderState::CancelPending,
        Trigger::TimeoutUnknown,
        OrderState::Unknown,
    ),
    (
        OrderState::CancelPending,
        Trigger::DisconnectUnknown,
        OrderState::Unknown,
    ),
    (
        OrderState::ReplacePending,
        Trigger::ReplaceConfirm,
        OrderState::Replaced,
    ),
    (
        OrderState::ReplacePending,
        Trigger::ReplaceRejectAcked,
        OrderState::Acked,
    ),
    (
        OrderState::ReplacePending,
        Trigger::ReplaceRejectPartial,
        OrderState::PartiallyFilled,
    ),
    (
        OrderState::ReplacePending,
        Trigger::FillComplete,
        OrderState::Filled,
    ),
    (
        OrderState::ReplacePending,
        Trigger::TimeoutUnknown,
        OrderState::Unknown,
    ),
    (
        OrderState::ReplacePending,
        Trigger::DisconnectUnknown,
        OrderState::Unknown,
    ),
    (OrderState::Unknown, Trigger::ReconToSent, OrderState::Sent),
    (
        OrderState::Unknown,
        Trigger::ReconToAcked,
        OrderState::Acked,
    ),
    (
        OrderState::Unknown,
        Trigger::ReconToRejected,
        OrderState::Rejected,
    ),
    (
        OrderState::Unknown,
        Trigger::ReconToPartial,
        OrderState::PartiallyFilled,
    ),
    (
        OrderState::Unknown,
        Trigger::ReconToFilled,
        OrderState::Filled,
    ),
    (
        OrderState::Unknown,
        Trigger::ReconToCancelled,
        OrderState::Cancelled,
    ),
    (
        OrderState::Unknown,
        Trigger::ReconToReplaced,
        OrderState::Replaced,
    ),
    (
        OrderState::Unknown,
        Trigger::ReconToExpired,
        OrderState::Expired,
    ),
    (OrderState::Unknown, Trigger::Resend, OrderState::Sent),
    (OrderState::Unknown, Trigger::Ack, OrderState::Acked),
    (
        OrderState::Unknown,
        Trigger::FillPartial,
        OrderState::PartiallyFilled,
    ),
    (
        OrderState::Unknown,
        Trigger::FillComplete,
        OrderState::Filled,
    ),
    (
        OrderState::Unknown,
        Trigger::BrokerReject,
        OrderState::Rejected,
    ),
    (
        OrderState::Unknown,
        Trigger::CancelConfirm,
        OrderState::Cancelled,
    ),
    (OrderState::Unknown, Trigger::Expire, OrderState::Expired),
    (
        OrderState::Unknown,
        Trigger::CancelRequest,
        OrderState::CancelPending,
    ),
];

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("illegal transition {from} via {trigger}")]
pub struct IllegalTransition {
    pub from: OrderState,
    pub trigger: Trigger,
}

pub fn next_state(from: OrderState, trigger: Trigger) -> Result<OrderState, IllegalTransition> {
    for (f, t, to) in TRANSITIONS {
        if *f == from && *t == trigger {
            return Ok(*to);
        }
    }
    Err(IllegalTransition { from, trigger })
}

pub fn requires_prior_risk_approval(state: OrderState) -> bool {
    !matches!(state, OrderState::New | OrderState::RiskRejected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn table_has_no_duplicate_edges() {
        let mut seen = std::collections::BTreeSet::new();
        for (from, trigger, _) in TRANSITIONS {
            assert!(
                seen.insert((*from, *trigger)),
                "duplicate edge {from} {trigger}"
            );
        }
    }

    #[test]
    fn terminal_states_have_no_exits() {
        for state in OrderState::all() {
            if !state.is_terminal() {
                continue;
            }
            for trigger in Trigger::all() {
                assert!(
                    next_state(*state, *trigger).is_err(),
                    "{state} allowed {trigger}"
                );
            }
        }
    }

    #[test]
    fn every_listed_edge_is_accepted_and_unlisted_rejected() {
        let mut allowed = std::collections::BTreeSet::new();
        for (from, trigger, to) in TRANSITIONS {
            assert_eq!(next_state(*from, *trigger).unwrap(), *to);
            if from.is_terminal() {
                panic!("terminal {from} listed");
            }
            allowed.insert((*from, *trigger));
        }
        for from in OrderState::all() {
            for trigger in Trigger::all() {
                let listed = allowed.contains(&(*from, *trigger));
                assert_eq!(next_state(*from, *trigger).is_ok(), listed);
            }
        }
    }

    #[test]
    fn sent_is_only_reachable_after_risk_or_recon() {
        for (from, trigger, to) in TRANSITIONS {
            if *to == OrderState::Sent {
                assert!(
                    matches!(
                        (from, trigger),
                        (OrderState::RiskApproved, Trigger::Dispatch)
                            | (OrderState::Unknown, Trigger::Resend)
                            | (OrderState::Unknown, Trigger::ReconToSent)
                            | (OrderState::CancelPending, Trigger::CancelRejectSent)
                    ),
                    "unexpected path to SENT: {from} {trigger}"
                );
            }
        }
    }

    proptest! {
        #[test]
        fn random_edges_match_table(bits in any::<u16>()) {
            let states = OrderState::all();
            let triggers = Trigger::all();
            let from = states[(bits as usize) % states.len()];
            let trigger = triggers[((bits >> 8) as usize) % triggers.len()];
            let via_scan = TRANSITIONS.iter().find(|(f, t, _)| *f == from && *t == trigger).map(|(_, _, to)| *to);
            prop_assert_eq!(next_state(from, trigger).ok(), via_scan);
            if from.is_terminal() {
                prop_assert!(via_scan.is_none());
            }
        }
    }
}
