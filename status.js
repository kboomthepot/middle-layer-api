// status.js
const { BigQuery } = require('@google-cloud/bigquery');

const bigquery = new BigQuery();

const PROJECT_ID = 'ghs-construction-1734441714520';
const JOBS_TABLE = `${PROJECT_ID}.Client_audits.client_audits_jobs`;

/**
 * Update a specific segment status column on the main job,
 * then recompute the overall job status.
 *
 * segmentColumn is something like:
 *   '1_demographics_Status'
 *   '7_organicSearch_Status'
 *   '8_paidAds_Status'
 */
async function markSegmentStatus(jobId, segmentColumn, newStatus) {
  if (!jobId) {
    console.error('[STATUS] markSegmentStatus called without jobId');
    return;
  }
  console.log(
    `➡️ [STATUS] markSegmentStatus: setting ${segmentColumn} = '${newStatus}' for job ${jobId}`
  );

  const updateSql = `
    UPDATE \`${JOBS_TABLE}\`
    SET ${segmentColumn} = @newStatus
    WHERE jobId = @jobId
  `;

  await bigquery.query({
    query: updateSql,
    params: { jobId, newStatus },
  });

  await recomputeMainJobStatus(jobId);
}

/**
 * Recompute the main job "status" field based on the segment columns.
 *
 * Rules (your spec):
 * - queued if ALL segmentStatuses are 'queued'
 * - pending if ANY segmentStatus is 'pending'
 * - failed if ANY segmentStatus is 'failed'
 * - completed ONLY if ALL segmentStatuses are 'completed'
 *
 * I’m interpreting priorities as:
 * - if any 'failed' => failed
 * - else if all 'queued' => queued
 * - else if all 'completed' => completed
 * - else if any 'pending' => pending
 * - else => pending (fallback)
 */
async function recomputeMainJobStatus(jobId) {
  console.log(
    `➡️ [STATUS] recomputeMainJobStatus for job ${jobId}`
  );

  const [rows] = await bigquery.query({
    query: `
      SELECT
        1_demographics_Status AS demo,
        7_organicSearch_Status AS organic,
        8_paidAds_Status AS paid,
        status AS currentStatus
      FROM \`${JOBS_TABLE}\`
      WHERE jobId = @jobId
    `,
    params: { jobId },
  });

  if (!rows.length) {
    console.warn(
      `[STATUS] recomputeMainJobStatus: no job row found for jobId=${jobId}`
    );
    return;
  }

  const row = rows[0];

  const segments = [
    row.demo,
    row.organic,
    row.paid,
  ].filter(Boolean); // ignore null/undefined

  if (!segments.length) {
    console.warn(
      `[STATUS] recomputeMainJobStatus: no segment statuses yet for jobId=${jobId}`
    );
    return;
  }

  const allQueued = segments.every((s) => s === 'queued');
  const anyFailed = segments.some((s) => s === 'failed');
  const allCompleted = segments.every((s) => s === 'completed');
  const anyPending = segments.some((s) => s === 'pending');

  let newStatus;

  if (anyFailed) {
    newStatus = 'failed';
  } else if (allQueued) {
    newStatus = 'queued';
  } else if (allCompleted) {
    newStatus = 'completed';
  } else if (anyPending) {
    newStatus = 'pending';
  } else {
    newStatus = 'pending';
  }

  if (row.currentStatus === newStatus) {
    console.log(
      `[STATUS] Job ${jobId} status already '${newStatus}', no update needed.`
    );
    return;
  }

  console.log(
    `[STATUS] Updating main job status for ${jobId}: ${row.currentStatus} -> ${newStatus}`
  );

  await bigquery.query({
    query: `
      UPDATE \`${JOBS_TABLE}\`
      SET status = @newStatus
      WHERE jobId = @jobId
    `,
    params: { jobId, newStatus },
  });
}

module.exports = {
  markSegmentStatus,
  recomputeMainJobStatus,
};
