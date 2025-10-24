use megacommerce_proto::{TaskGetResponse, Timestamp};

pub trait Pretty {
  fn pretty(&self) -> String;
}

impl Pretty for TaskGetResponse {
  fn pretty(&self) -> String {
    // similar formatting logic as above, returning a String
    let fmt_ts = |ts: &Option<Timestamp>| -> String {
      ts.as_ref()
        .map(|ts| format!("{}.{:09}s", ts.seconds, ts.nanos))
        .unwrap_or_else(|| "null".into())
    };
    let details = self.details.as_ref().map(|d| format!("{:?}", d)).unwrap_or_else(|| "{}".into());
    let canceled_by =
      self.canceled_by.as_ref().map(|v| v.value.to_string()).unwrap_or_else(|| "null".into());
    let err = self
      .error
      .as_ref()
      .map(|e| {
        format!("code={}, message={}, type={}, link={}", e.code, e.message, e.r#type, e.link)
      })
      .unwrap_or_else(|| "null".into());

    format!(
            "TaskGetResponse(uid={}, index_uid={}, status={}, type={}, duration={}, enqueued_at={}, started_at={}, finished_at={}, canceled_by={}, details={}, error={})",
            self.uid,
            self.index_uid,
            self.status,
            self.r#type,
            self.duration,
            fmt_ts(&self.enqueued_at),
            fmt_ts(&self.started_at),
            fmt_ts(&self.finished_at),
            canceled_by,
            details,
            err,
        )
  }
}
