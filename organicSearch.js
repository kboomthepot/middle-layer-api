// organicSearch.js
const { BigQuery } = require('@google-cloud/bigquery');
const axios = require('axios');
const { markSegmentStatus } = require('./status');

const bigquery = new BigQuery();

const PROJECT_ID = 'ghs-construction-1734441714520';
const JOBS_TABLE = `${PROJECT_ID}.Client_audits.client_audits_jobs`;
const ORG_TABLE = `${PROJECT_ID}.Client_audits.7_organicSearch_Jobs`;

// Set this in Cloud Run env vars if you want to avoid hardcoding
const ORGANIC_WEBHOOK_URL =
  process.env.ORGANIC_WEBHOOK_URL ||
  'https://n8n.srv974379.hstgr.cloud/webhook/07_organicSearch';

async function loadJob(jobId) {
  const [rows] = await bigquery.query({
    query: `
      SELECT
        jobId,
        location,
        businessName,
        services,
        createdAt,
        \`1_demographics_Status\` AS demographicsStatus,
        \`7_organicSearch_Status\` AS organicSearchStatus,
        \`8_paidAds_Status\` AS paidAdsStatus,
        status
      FROM \`${JOBS_TABLE}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId },
  });

  return rows;
}

/**
 * Called from worker.js when stage === '7_organicSearch'
 * - Marks 7_organicSearch_Status = 'pending'
 * - Sends payload to n8n webhook
 * - Inserts initial row in 7_organicSearch_Jobs with status='pending'
 */
async function handleOrganicSearchSegment(jobRow) {
  const jobId = jobRow.jobId;
  const location = jobRow.location;
  const services = jobRow.services || [];
  const businessName = jobRow.businessName || '';
  const createdAt =
    jobRow.createdAt?.value || jobRow.createdAt || new Date().toISOString();

  console.log(
    `ℹ️ [ORG] Job ${jobId} location = "${location}", organicSearchStatus = ${jobRow.organicSearchStatus}, services = ${JSON.stringify(
      services
    )}`
  );

  // 1) Mark segment status = pending in main jobs table
  console.log(`➡️ [ORG] Mark main job 7_organicSearch_Status = 'pending'`);
  await markSegmentStatus(jobId, '7_organicSearch_Status', 'pending');

  // 2) Call n8n webhook
  const payload = {
    jobId,
    location,
    services,
  };

  console.log(
    `ℹ️ [ORG] Sending payload to n8n webhook: ${ORGANIC_WEBHOOK_URL} → ${JSON.stringify(
      payload
    )}`
  );

  const response = await axios.post(ORGANIC_WEBHOOK_URL, payload, {
    timeout: 30000,
  });

  console.log(
    `✅ [ORG] n8n webhook call succeeded for job ${jobId}. Response: ${JSON.stringify(
      response.data
    )}`
  );

  // 3) INSERT initial row into 7_organicSearch_Jobs with status = 'pending'
  console.log(
    `ℹ️ [ORG] Inserting initial row into ${ORG_TABLE} for job ${jobId} with status='pending'`
  );

  await bigquery.query({
    query: `
      INSERT INTO \`${ORG_TABLE}\` (
        jobId,
        businessName,
        date,
        status,
        createdAt
      )
      VALUES (
        @jobId,
        @businessName,
        TIMESTAMP(@date),
        'pending',
        CURRENT_TIMESTAMP()
      )
    `,
    params: {
      jobId,
      businessName,
      date: createdAt,
    },
  });

  console.log(
    `✅ [ORG] Initial organic row inserted into ${ORG_TABLE} for job ${jobId}`
  );
}

/**
 * Express handler for /organic-result
 * - Receives the rank1..rank10 data from n8n
 * - Updates the existing row in 7_organicSearch_Jobs (matched ONLY by jobId)
 * - Sets table.status = 'completed' if all rank fields are filled, otherwise 'failed'
 * - Sets client_audits_jobs.7_organicSearch_Status = 'completed' or 'failed'
 */
async function handleOrganicResultCallback(req, res) {
  try {
    const items = req.body;

    if (!Array.isArray(items) || items.length === 0) {
      console.error('❌ [/organic-result] Invalid payload, expected non-empty array');
      res.status(400).json({ error: 'Invalid payload' });
      return;
    }

    console.log(
      `📥 [/organic-result] Received ${items.length} organic result item(s)`
    );

    for (const item of items) {
      const jobId = item.jobId;
      if (!jobId) {
        console.warn('⚠️ [ORG-CB] Skipping item with no jobId', item);
        continue;
      }

      const jobRows = await loadJob(jobId);
      if (!jobRows || jobRows.length === 0) {
        console.warn(
          `⚠️ [ORG-CB] No job row found in ${JOBS_TABLE} for job ${jobId}; marking segment failed`
        );
        await markSegmentStatus(jobId, '7_organicSearch_Status', 'failed');
        continue;
      }

      const jobRow = jobRows[0];
      const location = jobRow.location;
      const services = jobRow.services;

      console.log(
        `▶️ [ORG-CB] Processing organic callback for job ${jobId} (location=${location}, services=${JSON.stringify(
          services
        )})`
      );

      // We IGNORE location + services from the payload, we only use jobId + rank fields
      const safe = (v) =>
        v === undefined || v === null ? '' : String(v);

      const params = {
        jobId,
        rank1Name: safe(item.rank1Name),
        rank1Url: safe(item.rank1Url),
        rank2Name: safe(item.rank2Name),
        rank2Url: safe(item.rank2Url),
        rank3Name: safe(item.rank3Name),
        rank3Url: safe(item.rank3Url),
        rank4Name: safe(item.rank4Name),
        rank4Url: safe(item.rank4Url),
        rank5Name: safe(item.rank5Name),
        rank5Url: safe(item.rank5Url),
        rank6Name: safe(item.rank6Name),
        rank6Url: safe(item.rank6Url),
        rank7Name: safe(item.rank7Name),
        rank7Url: safe(item.rank7Url),
        rank8Name: safe(item.rank8Name),
        rank8Url: safe(item.rank8Url),
        rank9Name: safe(item.rank9Name),
        rank9Url: safe(item.rank9Url),
        rank10Name: safe(item.rank10Name),
        rank10Url: safe(item.rank10Url),
      };

      // Decide table.status based on whether all rank fields are filled
      const requiredFields = [
        'rank1Name', 'rank1Url',
        'rank2Name', 'rank2Url',
        'rank3Name', 'rank3Url',
        'rank4Name', 'rank4Url',
        'rank5Name', 'rank5Url',
        'rank6Name', 'rank6Url',
        'rank7Name', 'rank7Url',
        'rank8Name', 'rank8Url',
        'rank9Name', 'rank9Url',
        'rank10Name', 'rank10Url',
      ];

      const allFilled = requiredFields.every((key) => {
        const val = params[key];
        return val !== undefined && val !== null && String(val).trim() !== '';
      });

      const tableStatus = allFilled ? 'completed' : 'failed';
      const segmentStatus = tableStatus; // same mapping

      console.log(
        `ℹ️ [ORG-CB] Updating ${ORG_TABLE} for job ${jobId} with status=${tableStatus}`
      );

      await bigquery.query({
        query: `
          UPDATE \`${ORG_TABLE}\`
          SET
            rank1Name  = @rank1Name,
            rank1Url   = @rank1Url,
            rank2Name  = @rank2Name,
            rank2Url   = @rank2Url,
            rank3Name  = @rank3Name,
            rank3Url   = @rank3Url,
            rank4Name  = @rank4Name,
            rank4Url   = @rank4Url,
            rank5Name  = @rank5Name,
            rank5Url   = @rank5Url,
            rank6Name  = @rank6Name,
            rank6Url   = @rank6Url,
            rank7Name  = @rank7Name,
            rank7Url   = @rank7Url,
            rank8Name  = @rank8Name,
            rank8Url   = @rank8Url,
            rank9Name  = @rank9Name,
            rank9Url   = @rank9Url,
            rank10Name = @rank10Name,
            rank10Url  = @rank10Url,
            status     = @statusInTable,
            updatedAt  = CURRENT_TIMESTAMP()
          WHERE jobId = @jobId
        `,
        params: {
          ...params,
          statusInTable: tableStatus,
        },
      });

      console.log(
        `✅ [ORG-CB] UPDATE completed for job ${jobId} in ${ORG_TABLE} with status=${tableStatus}`
      );

      // Update segment status in main jobs table
      await markSegmentStatus(jobId, '7_organicSearch_Status', segmentStatus);
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('❌ [ORG-CB] Error in /organic-result handler:', err);
    res.status(500).json({ error: 'Internal error' });
  }
}

module.exports = {
  handleOrganicSearchSegment,
  handleOrganicResultCallback,
};
