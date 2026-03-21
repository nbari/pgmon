pub struct ActivitySummarySection {
    pub title: &'static str,
    pub metrics: Vec<ActivitySummaryMetric>,
}

pub struct ActivitySummaryMetric {
    pub label: &'static str,
    pub value: String,
}

impl ActivitySummaryMetric {
    pub fn new(label: &'static str, value: String) -> Self {
        Self { label, value }
    }
}
