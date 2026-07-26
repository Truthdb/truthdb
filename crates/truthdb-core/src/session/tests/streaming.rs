use super::*;

#[tokio::test]
async fn statement_dones_carry_their_own_transaction_state() {
    // Each DONE reports the transaction state after *its own* statement —
    // BEGIN and the SELECT inside the transaction say so, the COMMIT says
    // it ended — instead of the batch's final state stamped on all three.
    let h = start(LOCK_WAIT_TIMEOUT);
    let s = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    let events = drain_events(h.handle.stream_batch(
        s,
        "BEGIN TRANSACTION; SELECT 1 AS one; COMMIT".into(),
        no_cancel(),
    ))
    .await;
    assert_eq!(done_flags(&events), [true, true, false]);
    assert!(
        matches!(
            events.last(),
            Some(BatchEvent::Complete {
                in_transaction: false
            })
        ),
        "Complete carries the batch-final state: {events:?}"
    );
}

#[tokio::test]
async fn a_mid_scan_error_keeps_the_rows_already_streamed() {
    // A streamed SELECT that fails part-way has already emitted the rows
    // that preceded the failure — rows leave while the statement is still
    // running. The buffered path emitted nothing for a failed statement,
    // so any Columns/Rows here prove the stream is real.
    let h = start(LOCK_WAIT_TIMEOUT);
    let s = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    fill(&h, s, 600).await;
    // The WHERE divides by zero at id = 600, after 599 kept rows: two full
    // 256-row chunks are already out, the partial third is dropped.
    let events = drain_events(h.handle.stream_batch(
        s,
        "SELECT id FROM t WHERE 10 / (id - 600) > -100".into(),
        no_cancel(),
    ))
    .await;
    let streamed: usize = events
        .iter()
        .map(|event| match event {
            BatchEvent::Rows(rows) => rows.len(),
            _ => 0,
        })
        .sum();
    assert_eq!(streamed, 512, "two full chunks precede the failure");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, BatchEvent::Error(err) if err.number == 8134)),
        "the divide-by-zero still reaches the client: {events:?}"
    );
}

#[tokio::test]
async fn a_caught_mid_scan_error_closes_the_open_rowset() {
    // A TRY/CATCH swallows the error, but the failed SELECT's result set
    // had already started streaming — it must be closed (StatementAborted)
    // before the CATCH's own result set opens, and no Error event follows.
    let h = start(LOCK_WAIT_TIMEOUT);
    let s = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    fill(&h, s, 300).await;
    let events = drain_events(
        h.handle.stream_batch(
            s,
            "BEGIN TRY SELECT id FROM t WHERE 10 / (id - 300) > -100 END TRY \
         BEGIN CATCH SELECT 99 AS caught END CATCH"
                .into(),
            no_cancel(),
        ),
    )
    .await;
    let aborted = events
        .iter()
        .position(|e| matches!(e, BatchEvent::StatementAborted { .. }))
        .expect("the failed SELECT's rowset is closed");
    let caught = events
        .iter()
        .position(
            |e| matches!(e, BatchEvent::Columns(cols) if cols.first().is_some_and(|c| c.name == "caught")),
        )
        .expect("the CATCH's rowset follows");
    assert!(aborted < caught, "close before reopening: {events:?}");
    assert!(
        !events.iter().any(|e| matches!(e, BatchEvent::Error(_))),
        "a caught error never surfaces: {events:?}"
    );
}

#[tokio::test]
async fn a_continued_mid_scan_error_closes_the_rowset_and_reports_last() {
    // Under XACT_ABORT OFF a non-fatal in-transaction error rolls back only
    // its statement and the batch continues — so the half-streamed rowset
    // closes, the following statements run, and the error is reported at
    // the end of the batch, exactly where the buffered path put it.
    let h = start(LOCK_WAIT_TIMEOUT);
    let s = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    fill(&h, s, 300).await;
    let events = drain_events(
        h.handle.stream_batch(
            s,
            "BEGIN TRANSACTION; SELECT id FROM t WHERE 10 / (id - 300) > -100; \
         SELECT 7 AS after; COMMIT"
                .into(),
            no_cancel(),
        ),
    )
    .await;
    assert!(
        events.iter().any(|e| matches!(
            e,
            BatchEvent::StatementAborted {
                in_transaction: true
            }
        )),
        "the failed SELECT's rowset closes, still in-transaction: {events:?}"
    );
    // BEGIN, the surviving SELECT, then COMMIT — each DONE with its own state.
    assert_eq!(done_flags(&events), [true, true, false]);
    let error = events
        .iter()
        .position(|e| matches!(e, BatchEvent::Error(err) if err.number == 8134))
        .expect("the continued error is still reported");
    assert_eq!(error, events.len() - 2, "after every result: {events:?}");
}
