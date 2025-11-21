// jobHelpers.js
'use strict';

const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();

const PROJECT_ID = 'ghs-construction-1734441714520';
const JOBS_TABLE = `${PROJECT_ID}.Client_audits.client_audits_jobs`;

/**
 * Safely normalize a BigQuery TIMESTAMP into ISO string for JS.
 */
function bqTimestampToIso(ts) {
  if (!ts) return new Date().toISOString();
  if (ts instanceof Date) return ts.toISOString();
  if (typeof ts === 'string') return ts;
  if (typeof ts.value === 'string') return ts.value;
  return new Date().toISOString();
}

/**
 * Force value into STRING (never null) so BigQuery doesn't need explicit types.
 */
function safeStr(v) {
  if (v === undefined || v === null) return '';
  return String(v);
}

/**
 * Load a single job row from client_audits_jobs by jobId.
 */
async function loadJob(jobId) {
  console.log('➡️ Load job row from client_audits_jobs');
  const [rows] = await bigquery.query({
    query: `
      SELECT
        jobId,
        createdAt,
        status,
        businessName,
        services,
        location,
        website,
        \`1_demographics_Status\` AS demographicsStatus,
        \`7_organicSearch_Status\` AS organicSearchStatus,
        \`8_paidAds_Status\` AS paidAdsStatus
      FROM \`${JOBS_TABLE}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId }
  });

  console.log(`ℹ️ loadJob result rows: ${rows.length}`);
  return rows[0];
}

/**
 * Update a specific "segment status" column (e.g. 1_demographics_Status).
 */
async function markSegmentStatus(jobId, statusColumn, newStatus) {
  console.log(
    `✅ markSegmentStatus: set ${statusColumn} = '${newStatus}' for job ${jobId}`
  );
  await bigquery.query({
    query: `
      UPDATE \`${JOBS_TABLE}\`
      SET \`${statusColumn}\` = @newStatus
      WHERE jobId = @jobId
    `,
    params: { jobId, newStatus }
  });
}

/**
 * Compute and update overall job.status based on all segment statuses:
 *
 * - 'queued'   if ALL segment statuses are 'queued'
 * - 'failed'   if ANY segment status is 'failed'
 * - 'pending'  if ANY segment status is 'pending'
 * - 'completed' ONLY if ALL segment statuses are 'completed'
 * - otherwise  fallback to 'pending' for mixed states
 */
async function updateOverallStatus(jobId) {
  console.log('➡️ [STATUS] Load job row for overall status update');
  const [rows] = await bigquery.query({
    query: `
      SELECT
        jobId,
        status,
        \`1_demographics_Status\` AS s_demo,
        \`7_organicSearch_Status\` AS s_org,
        \`8_paidAds_Status\` AS s_paid
      FROM \`${JOBS_TABLE}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId }
  });

  console.log(`ℹ️ [STATUS] loadJob result rows: ${rows.length}`);
  if (!rows.length) return;

  const job = rows[0];

  const segments = [
    job.s_demo || 'queued',
    job.s_org || 'queued',
    job.s_paid || 'queued'
  ];

  const allQueued = segments.every(s => s === 'queued');
  const allCompleted = segments.every(s => s === 'completed');
  const anyFailed = segments.some(s => s === 'failed');
  const anyPending = segments.some(s => s === 'pending');

  let newStatus;
  if (allQueued) {
    newStatus = 'queued';
  } else if (anyFailed) {
    newStatus = 'failed';
  } else if (anyPending) {
    newStatus = 'pending';
  } else if (allCompleted) {
    newStatus = 'completed';
  } else {
    // Mixed queued/completed etc – treat as in-progress
    newStatus = 'pending';
  }

  console.log(
    `ℹ️ [STATUS] job ${jobId}: old status=${job.status}, segments=${JSON.stringify(
      segments
    )}, new status=${newStatus}`
  );

  if (newStatus !== job.status) {
    await bigquery.query({
      query: `
        UPDATE \`${JOBS_TABLE}\`
        SET status = @newStatus
        WHERE jobId = @jobId
      `,
      params: { jobId, newStatus }
    });
    console.log(
      `✅ [STATUS] Updated overall status for job ${jobId} → ${newStatus}`
    );
  }
}

module.exports = {
  bigquery,
  PROJECT_ID,
  JOBS_TABLE,
  bqTimestampToIso,
  safeStr,
  loadJob,
  markSegmentStatus,
  updateOverallStatus
};
