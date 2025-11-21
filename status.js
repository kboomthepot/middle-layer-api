// status.js
const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();

const PROJECT_ID = 'ghs-construction-1734441714520';
const JOBS_TABLE = `${PROJECT_ID}.Client_audits.client_audits_jobs`;

/**
 * Recompute the main status on client_audits_jobs for a given jobId.
 *
 * Rules:
 * - 'queued'    → if ALL segment statuses are 'queued'
 * - 'failed'    → if ANY segment status is 'failed'
 * - 'completed' → if ALL segment statuses are 'completed'
 * - 'pending'   → otherwise (any mix with 'pending' or completed/queued mix)
 */
async function recomputeAndUpdateMainStatus(jobId) {
  console.log(`ℹ️ [STATUS] Recomputing main status for job ${jobId}`);

  const [rows] = await bigquery.query({
    query: `
      SELECT
        \`1_demographics_Status\` AS demo,
        \`7_organicSearch_Status\` AS organic,
        \`8_paidAds_Status\` AS paid
      FROM \`${JOBS_TABLE}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId },
  });

  if (!rows || rows.length === 0) {
    console.warn(`[STATUS] No job row found for jobId=${jobId} when recomputing main status`);
    return;
  }

  const row = rows[0];

  // Collect the segment statuses that actually exist (ignore null/undefined)
  const segmentStatuses = [row.demo, row.organic, row.paid].filter((s) => !!s);

  if (segmentStatuses.length === 0) {
    console.warn(
      `[STATUS] No segment statuses set yet for jobId=${jobId}; leaving main status unchanged`
    );
    return;
  }

  let newStatus;

  const allQueued = segmentStatuses.every((s) => s === 'queued');
  const anyFailed = segmentStatuses.some((s) => s === 'failed');
  const allCompleted = segmentStatuses.every((s) => s === 'completed');
  const anyPending = segmentStatuses.some((s) => s === 'pending');

  if (allQueued) {
    newStatus = 'queued';
  } else if (anyFailed) {
    newStatus = 'failed';
  } else if (allCompleted) {
    newStatus = 'completed';
  } else if (anyPending) {
    newStatus = 'pending';
  } else {
    // Fallback for mixed completed/queued with no pending/failed → treat as pending
    newStatus = 'pending';
  }

  console.log(
    `ℹ️ [STATUS] Segment statuses for job ${jobId} = ${JSON.stringify(
      segmentStatuses
    )} → main status = ${newStatus}`
  );

  await bigquery.query({
    query: `
      UPDATE \`${JOBS_TABLE}\`
      SET status = @status,
          updatedAt = CURRENT_TIMESTAMP()
      WHERE jobId = @jobId
    `,
    params: { status: newStatus, jobId },
  });

  console.log(`✅ [STATUS] Updated main status for job ${jobId} → ${newStatus}`);
}

/**
 * Update a single segment status column on client_audits_jobs and
 * then recompute the main job status.
 *
 * @param {string} jobId
 * @param {string} columnName e.g. "1_demographics_Status" or "7_organicSearch_Status"
 * @param {string} newStatus  "queued" | "pending" | "failed" | "completed"
 */
async function markSegmentStatus(jobId, columnName, newStatus) {
  console.log(
    `➡️ [STATUS] markSegmentStatus: setting ${columnName} = '${newStatus}' for job ${jobId}`
  );

  const allowedColumns = [
    '1_demographics_Status',
    '7_organicSearch_Status',
    '8_paidAds_Status',
  ];

  if (!allowedColumns.includes(columnName)) {
    console.warn(
      `[STATUS] markSegmentStatus called with unsupported column "${columnName}" for job ${jobId}`
    );
    return;
  }

  const [result] = await bigquery.query({
    query: `
      UPDATE \`${JOBS_TABLE}\`
      SET \`${columnName}\` = @status,
          updatedAt = CURRENT_TIMESTAMP()
      WHERE jobId = @jobId
    `,
    params: { status: newStatus, jobId },
  });

  console.log(
    `✅ markSegmentStatus: set ${columnName} = '${newStatus}' for job ${jobId} (rowsAffected=${result.numDmlAffectedRows})`
  );

  // After updating a segment, always recompute the main job status
  await recomputeAndUpdateMainStatus(jobId);
}

module.exports = {
  markSegmentStatus,
  recomputeAndUpdateMainStatus,
};
