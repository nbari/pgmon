//! Explain planning and safety helpers for `PgClient`.

mod execute;
mod parse;

use super::{ExplainMode, PgClient};

pub(crate) use self::parse::analyze_explain_query;
use self::parse::validate_explain_query;

#[cfg(test)]
mod regression;
#[cfg(test)]
mod tests;
