// organicSearch.js
const { BigQuery } = require('@google-cloud/bigquery');
const axios = require('axios');
const { markSegmentStatus, recomputeMainStatus } = require('./status');

const bigquery = new BigQuery();

const PROJECT_ID = 'ghs-construction-1734441714520';
const JOBS_TABLE = `${PROJECT_ID}.Client_audits.client_audits_jobs`;
const ORG_TABLE = `${PROJECT_ID}.Client_audits.7_organicSearch_Jobs`;

// 🔧 configure via env var
const N8N_ORG_WEBHOOK_URL =
  process.env.N8N_ORG_WEBHOOK_URL ||
  'https://n8n.srv974379.hstgr.cloud/webhook/07_organicSearch';

async function loadJob(jobId) {
  const [rows] = await bigquery.query({
    query: `
      SELECT
        jobId,
        location,
        businessName,
        services,
        1_demographics_Status AS demographicsStatus,
        7_organicSearch_Status AS organicSearchStatus,
        paidAdsStatus,
        status,
        createdAt
      FROM \`${JOBS_TABLE}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId },
  });
  return rows[0] || null;
}

function parseServices(servicesField) {
  if (!servicesField) return [];
  if (Array.isArray(servicesField)) return servicesField;
  if (typeof servicesField === 'string') {
    try {
      const parsed = JSON.parse(servicesField);
      if (Array.isArray(parsed)) return parsed;
    } catch (e) {
      // fall through
      return [servicesField];
    }
  }
  return [];
}

async function handleOrganicSearchSegment(jobId) {
  console.log(`▶️ [ORG] Starting organic search processing for job ${jobId}`);

  const job = await loadJob(jobId);
  if (!job) {
    console.warn(`⚠️ [ORG] No job row found for jobId=${jobId}, skipping.`);
    return;
  }

  const {
    location,
    businessName,
    organicSearchStatus,
    createdAt,
    services,
  } = job;

  // 🔒 Idempotency guard: only run when status is "queued"
  if (organicSearchStatus && organicSearchStatus !== 'queued') {
    console.log(
      `ℹ️ [ORG] Job ${jobId} organicSearchStatus=${organicSearchStatus}, not 'queued' – skipping.`
    );
    return;
  }

  const servicesArr = parseServices(services);

  console.log(
    `ℹ️ [ORG] Job ${jobId} location = "${location}", organicSearchStatus = ${organicSearchStatus}, services = ${JSON.stringify(
      servicesArr
    )}`
  );

  // Step 1: mark segment pending
  await markSegmentStatus(jobId, '7_organicSearch_Status', 'pending');

  // Step 2: send payload to n8n
  const payload = {
    jobId,
    location,
    services: servicesArr,
  };

  console.log(
    `ℹ️ [ORG] Sending payload to n8n webhook: ${N8N_ORG_WEBHOOK_URL} → ${JSON.stringify(
      payload
    )}`
  );

  await axios.post(N8N_ORG_WEBHOOK_URL, payload, { timeout: 30000 });
  console.log(
    `✅ [ORG] n8n webhook call succeeded for job ${jobId}. Response: "Workflow was started"`
  );

  // Step 3: insert or update initial row in 7_organicSearch_Jobs with pending status
  const createdAtTs = createdAt?.value || createdAt || null;

  console.log(
    `ℹ️ [ORG] Inserting initial row into ${ORG_TABLE} for job ${jobId} with status='pending'`
  );

  await bigquery.query({
    query: `
      MERGE \`${ORG_TABLE}\` T
      USING (
        SELECT
          @jobId AS jobId,
          @businessName AS businessName,
          @date AS date,
          'pending' AS status
      ) S
      ON T.jobId = S.jobId
      WHEN NOT MATCHED THEN
        INSERT (jobId, businessName, date, status)
        VALUES (S.jobId, S.businessName, S.date, S.status)
      WHEN MATCHED THEN
        UPDATE SET
          businessName = S.businessName,
          date = S.date,
          status = S.status
    `,
    params: {
      jobId,
      businessName: businessName || null,
      date: createdAtTs,
    },
    types: {
      jobId: 'STRING',
      businessName: 'STRING',
      date: 'TIMESTAMP',
    },
  });
}

/**
 * Callback from n8n with final organic results.
 * body example:
 * [
 *   {
 *     "jobId": "...",
 *     "rank1Name": "...",
 *     "rank1Url": "...",
 *     ...
 *   }
 * ]
 */
async function handleOrganicResultCallback(body) {
  const item = Array.isArray(body) ? body[0] : body || {};
  const jobId = item.jobId;

  if (!jobId) {
    console.error(
      '❌ [ORG-CB] Missing jobId in organic callback payload:',
      JSON.stringify(body)
    );
    return;
  }

  const job = await loadJob(jobId);
  if (!job) {
    console.warn(
      `⚠️ [ORG-CB] No job found for jobId=${jobId}, marking segment failed.`
    );
    await markSegmentStatus(jobId, '7_organicSearch_Status', 'failed');
    await recomputeMainStatus(jobId);
    return;
  }

  const { businessName, createdAt } = job;
  const createdAtTs = createdAt?.value || createdAt || null;

  console.log(
    `▶️ [ORG-CB] Processing organic callback for job ${jobId} (location=${job.location}, services=${job.services})`
  );

  // Build MERGE for all rank fields coming back from n8n
  const params = {
    jobId,
    businessName: businessName || null,
    date: createdAtTs,
    rank1Name: item.rank1Name || null,
    rank1Url: item.rank1Url || null,
    rank2Name: item.rank2Name || null,
    rank2Url: item.rank2Url || null,
    rank3Name: item.rank3Name || null,
    rank3Url: item.rank3Url || null,
    rank4Name: item.rank4Name || null,
    rank4Url: item.rank4Url || null,
    rank5Name: item.rank5Name || null,
    rank5Url: item.rank5Url || null,
    rank6Name: item.rank6Name || null,
    rank6Url: item.rank6Url || null,
    rank7Name: item.rank7Name || null,
    rank7Url: item.rank7Url || null,
    rank8Name: item.rank8Name || null,
    rank8Url: item.rank8Url || null,
    rank9Name: item.rank9Name || null,
    rank9Url: item.rank9Url || null,
    rank10Name: item.rank10Name || null,
    rank10Url: item.rank10Url || null,
  };

  try {
    await bigquery.query({
      query: `
        MERGE \`${ORG_TABLE}\` T
        USING (
          SELECT
            @jobId AS jobId,
            @businessName AS businessName,
            @date AS date,
            @rank1Name AS rank1Name,
            @rank1Url AS rank1Url,
            @rank2Name AS rank2Name,
            @rank2Url AS rank2Url,
            @rank3Name AS rank3Name,
            @rank3Url AS rank3Url,
            @rank4Name AS rank4Name,
            @rank4Url AS rank4Url,
            @rank5Name AS rank5Name,
            @rank5Url AS rank5Url,
            @rank6Name AS rank6Name,
            @rank6Url AS rank6Url,
            @rank7Name AS rank7Name,
            @rank7Url AS rank7Url,
            @rank8Name AS rank8Name,
            @rank8Url AS rank8Url,
            @rank9Name AS rank9Name,
            @rank9Url AS rank9Url,
            @rank10Name AS rank10Name,
            @rank10Url AS rank10Url,
            'completed' AS status
        ) S
        ON T.jobId = S.jobId
        WHEN NOT MATCHED THEN
          INSERT (
            jobId,
            businessName,
            date,
            rank1Name,
            rank1Url,
            rank2Name,
            rank2Url,
            rank3Name,
            rank3Url,
            rank4Name,
            rank4Url,
            rank5Name,
            rank5Url,
            rank6Name,
            rank6Url,
            rank7Name,
            rank7Url,
            rank8Name,
            rank8Url,
            rank9Name,
            rank9Url,
            rank10Name,
            rank10Url,
            status
          )
          VALUES (
            S.jobId,
            S.businessName,
            S.date,
            S.rank1Name,
            S.rank1Url,
            S.rank2Name,
            S.rank2Url,
            S.rank3Name,
            S.rank3Url,
            S.rank4Name,
            S.rank4Url,
            S.rank5Name,
            S.rank5Url,
            S.rank6Name,
            S.rank6Url,
            S.rank7Name,
            S.rank7Url,
            S.rank8Name,
            S.rank8Url,
            S.rank9Name,
            S.rank9Url,
            S.rank10Name,
            S.rank10Url,
            S.status
          )
        WHEN MATCHED THEN
          UPDATE SET
            businessName = S.businessName,
            date = S.date,
            rank1Name = S.rank1Name,
            rank1Url = S.rank1Url,
            rank2Name = S.rank2Name,
            rank2Url = S.rank2Url,
            rank3Name = S.rank3Name,
            rank3Url = S.rank3Url,
            rank4Name = S.rank4Name,
            rank4Url = S.rank4Url,
            rank5Name = S.rank5Name,
            rank5Url = S.rank5Url,
            rank6Name = S.rank6Name,
            rank6Url = S.rank6Url,
            rank7Name = S.rank7Name,
            rank7Url = S.rank7Url,
            rank8Name = S.rank8Name,
            rank8Url = S.rank8Url,
            rank9Name = S.rank9Name,
            rank9Url = S.rank9Url,
            rank10Name = S.rank10Name,
            rank10Url = S.rank10Url,
            status = S.status
      `,
      params,
      types: {
        jobId: 'STRING',
        businessName: 'STRING',
        date: 'TIMESTAMP',
        rank1Name: 'STRING',
        rank1Url: 'STRING',
        rank2Name: 'STRING',
        rank2Url: 'STRING',
        rank3Name: 'STRING',
        rank3Url: 'STRING',
        rank4Name: 'STRING',
        rank4Url: 'STRING',
        rank5Name: 'STRING',
        rank5Url: 'STRING',
        rank6Name: 'STRING',
        rank6Url: 'STRING',
        rank7Name: 'STRING',
        rank7Url: 'STRING',
        rank8Name: 'STRING',
        rank8Url: 'STRING',
        rank9Name: 'STRING',
        rank9Url: 'STRING',
        rank10Name: 'STRING',
        rank10Url: 'STRING',
      },
    });

    console.log(
      `✅ [ORG-CB] MERGE completed for job ${jobId} into ${ORG_TABLE}`
    );

    // If we want, we could re-check for nulls here and set status=failed, but
    // BigQuery MERGE succeeded and we set status='completed' above.
    await markSegmentStatus(jobId, '7_organicSearch_Status', 'completed');
    await recomputeMainStatus(jobId);
  } catch (err) {
    console.error(
      `❌ [ORG-CB] MERGE FAILED for job ${jobId}:`,
      err.message || err
    );
    // mark failed so main status can move on
    await markSegmentStatus(jobId, '7_organicSearch_Status', 'failed');
    await recomputeMainStatus(jobId);
  }
}

module.exports = {
  handleOrganicSearchSegment,
  handleOrganicResultCallback,
};
