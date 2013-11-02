
pub trait StatisticsManager {
    fn size(&self) -> uint;
}

pub struct NotSyncedStatistics {
    size: uint
}
impl NotSyncedStatistics {
    pub fn new() -> NotSyncedStatistics {
        NotSyncedStatistics { size: 0 }
    }
}
impl StatisticsManager for NotSyncedStatistics {
    fn size(&self) -> uint {
        self.size()
    }
}

