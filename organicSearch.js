// organicSearch.js
const { BigQuery } = require('@google-cloud/bigquery');
const axios = require('axios');
const { markSegmentStatus } = require('./status');

const bigquery = new BigQuery();

const PROJECT_ID = 'ghs-construction-1734441714520';
const JOBS_TABLE = `${PROJECT_ID}.Client_audits.client_audits_jobs`;
const ORG_TABLE = `${PROJECT_ID}.Client_audits.7_organicSearch_Jobs`;

// ⚠️ Make sure this matches your actual n8n webhook URL
const N8N_WEBHOOK_URL =
  'https://n8n.srv974379.hstgr.cloud/webhook/07_organicSearch';

/**
 * Load a single job row from client_audits_jobs.
 */
async function loadJob(jobId) {
  const [rows] = await bigquery.query({
    query: `
      SELECT
        jobId,
        location,
        businessName,
        services,
        createdAt,
        1_demographics_Status AS demographicsStatus,
        7_organicSearch_Status AS organicSearchStatus,
        8_paidAds_Status AS paidAdsStatus,
        status
      FROM \`${JOBS_TABLE}\`
      WHERE jobId = @jobId
    `,
    params: { jobId },
  });

  if (!rows.length) {
    console.warn(`[ORG] loadJob: no job row found for jobId=${jobId}`);
    return null;
  }

  return rows[0];
}

/**
 * Normalize the services field (it might be JSON string, array, or simple string).
 */
function normalizeServices(raw) {
  if (!raw) return [];
  if (Array.isArray(raw)) return raw;

  if (typeof raw === 'string') {
    const trimmed = raw.trim();
    if (!trimmed) return [];
    // Try to parse JSON string
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) return parsed;
      return [trimmed];
    } catch (_err) {
      return [trimmed];
    }
  }

  // Fallback
  return [String(raw)];
}

/**
 * Handle the "7_organicSearch" segment when triggered by Pub/Sub.
 * This:
 *  - loads the job row,
 *  - checks segment status == 'queued',
 *  - marks segment as 'pending',
 *  - sends payload to n8n,
 *  - upserts an initial row in 7_organicSearch_Jobs with status='pending'.
 */
async function handleOrganicSearchSegment(jobId) {
  console.log(`▶️ [ORG] Starting organic search processing for job ${jobId}`);

  if (!jobId) {
    console.error('[ORG] handleOrganicSearchSegment called without jobId');
    return;
  }

  const jobRow = await loadJob(jobId);
  if (!jobRow) {
    console.error(`[ORG] No job row found for jobId=${jobId}, aborting organic segment.`);
    // If you want, you could mark the segment as failed here.
    return;
  }

  const {
    location,
    businessName,
    services: rawServices,
    createdAt,
    organicSearchStatus,
  } = jobRow;

  console.log(
    `ℹ️ [ORG] Job ${jobId} location = "${location}", organicSearchStatus = ${organicSearchStatus}, services = ${JSON.stringify(
      rawServices
    )}`
  );

  // Only run this if the segment is queued or null.
  if (organicSearchStatus && organicSearchStatus !== 'queued') {
    console.log(
      `[ORG] Segment 7_organicSearch_Status is "${organicSearchStatus}", not 'queued' – skipping.`
    );
    return;
  }

  // Mark segment as pending on main job
  await markSegmentStatus(jobId, '7_organicSearch_Status', 'pending');

  const services = normalizeServices(rawServices);

  const payload = {
    jobId,
    location,
    services,
  };

  console.log(
    `ℹ️ [ORG] Sending payload to n8n webhook: ${N8N_WEBHOOK_URL} → ${JSON.stringify(
      payload
    )}`
  );

  await axios.post(N8N_WEBHOOK_URL, payload, { timeout: 30000 });

  console.log(
    `✅ [ORG] n8n webhook call succeeded for job ${jobId}. Response: (not logged)`
  );

  // Upsert initial row in 7_organicSearch_Jobs with status='pending'
  console.log(
    `ℹ️ [ORG] Upserting initial row into ${ORG_TABLE} for job ${jobId} with status='pending'`
  );

  const mergeSql = `
    MERGE \`${ORG_TABLE}\` T
    USING (
      SELECT
        @jobId AS jobId,
        @businessName AS businessName,
        @createdAt AS date,
        'pending' AS status
    ) S
    ON T.jobId = S.jobId
    WHEN NOT MATCHED THEN
      INSERT (jobId, businessName, date, status)
      VALUES (S.jobId, S.businessName, S.date, S.status)
  `;

  await bigquery.query({
    query: mergeSql,
    params: {
      jobId,
      businessName: businessName || null,
      createdAt: createdAt || null,
    },
  });

  console.log(
    `✅ [ORG] Initial row ensured in ${ORG_TABLE} for job ${jobId} (status=pending)`
  );
}

/**
 * Callback handler for organic results from n8n.
 *
 * Expected body from n8n (first item example):
 * [
 *   {
 *     "jobId": "...",
 *     "rank1Name": "...",
 *     "rank1Url": "...",
 *     ...
 *     "rank10Name": "...",
 *     "rank10Url": "..."
 *   }
 * ]
 */
async function handleOrganicResultCallback(body) {
  console.log('📥 [/organic-result] Received organic result payload:', body);

  if (!Array.isArray(body) || !body.length) {
    console.warn('[ORG-CB] Callback body is empty or not an array, ignoring.');
    return;
  }

  const item = body[0];
  const jobId = item.jobId || item.jobID || item.JobId;

  if (!jobId) {
    console.error('[ORG-CB] No jobId in callback payload, cannot proceed.');
    return;
  }

  // List of columns we expect from n8n / your organic process
  const rankCols = [];
  for (let i = 1; i <= 10; i++) {
    rankCols.push(`rank${i}Name`);
    rankCols.push(`rank${i}Url`);
  }

  // Build params and update SET clause
  const params = { jobId };
  const setClauses = [];

  let hasNull = false;

  for (const col of rankCols) {
    const val = item[col] ?? null;
    params[col] = val;
    setClauses.push(`${col} = @${col}`);
    if (val === null || val === undefined || val === '') {
      hasNull = true;
    }
  }

  // Decide status in 7_organicSearch_Jobs table:
  // - If any null/empty → failed
  // - Else → completed
  const newStatus = hasNull ? 'failed' : 'completed';
  params.status = newStatus;

  console.log(
    `ℹ️ [ORG-CB] Upserting organic results for job ${jobId} into ${ORG_TABLE}, status=${newStatus}`
  );

  const updateSql = `
    UPDATE \`${ORG_TABLE}\`
    SET
      ${setClauses.join(', ')},
      status = @status
    WHERE jobId = @jobId
  `;

  try {
    const [result] = await bigquery.query({
      query: updateSql,
      params,
    });
    console.log(
      `✅ [ORG-CB] Organic results update completed for job ${jobId}.`,
      result ? '' : ''
    );
  } catch (err) {
    console.error(
      `❌ [ORG-CB] MERGE/UPDATE FAILED for job ${jobId}:`,
      err
    );
    // Mark segment failed on error
    await markSegmentStatus(jobId, '7_organicSearch_Status', 'failed');
    return;
  }

  // Now set segment status on main jobs table
  await markSegmentStatus(jobId, '7_organicSearch_Status', newStatus);
  console.log(
    `✅ [ORG-CB] Segment 7_organicSearch_Status set to '${newStatus}' for job ${jobId} (and main status recomputed).`
  );
}

module.exports = {
  handleOrganicSearchSegment,
  handleOrganicResultCallback,
};
