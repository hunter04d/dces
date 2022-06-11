#![allow(dead_code)]

use crate::msg::types::LogValue;

#[derive(Debug)]
pub struct ReplicationLog {
    data: Vec<Option<LogValue>>,
}

impl ReplicationLog {
    pub fn new() -> ReplicationLog {
        ReplicationLog { data: Vec::new() }
    }

    pub fn populated_with_empty(count: usize) -> ReplicationLog {
        ReplicationLog {
            data: vec![Some(LogValue::empty()); count],
        }
    }

    pub fn get(&self, index: usize) -> Option<&LogValue> {
        let value = self.data.get(index)?;
        value.as_ref()
    }

    pub fn set(&mut self, index: usize, value: LogValue) {
        let current_len = self.data.len();
        if index >= current_len {
            let extend_by = index - current_len + 1;
            self.data
                .extend(std::iter::repeat_with(|| None).take(extend_by));
        }
        self.data[index] = Some(value);
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}
