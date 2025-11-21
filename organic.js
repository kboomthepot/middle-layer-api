// organic.js
'use strict';

const axios = require('axios');
const {
  bigquery,
  PROJECT_ID,
  bqTimestampToIso,
  safeStr,
  loadJob,
  markSegmentStatus,
  updateOverallStatus
} = require('./jobHelpers');

const ORGANIC_JOBS_TABLE = `${PROJECT_ID}.Client_audits.7_organicSearch_Jobs`;
const ORGANIC_WEBHOOK_URL =
  'https://n8n.srv974379.hstgr.cloud/webhook/07_organicSearch';

const MERGE_ORGANIC_SQL = `
  MERGE \`${ORGANIC_JOBS_TABLE}\` T
  USING (
    SELECT
      @jobId AS jobId,
      TIMESTAMP(@dateIso) AS date,
      @businessName AS businessName,
      @website AS website,
      @rank1Name AS rank1Name, @rank1Url AS rank1Url,
      @rank2Name AS rank2Name, @rank2Url AS rank2Url,
      @rank3Name AS rank3Name, @rank3Url AS rank3Url,
      @rank4Name AS rank4Name, @rank4Url AS rank4Url,
      @rank5Name AS rank5Name, @rank5Url AS rank5Url,
      @rank6Name AS rank6Name, @rank6Url AS rank6Url,
      @rank7Name AS rank7Name, @rank7Url AS rank7Url,
      @rank8Name AS rank8Name, @rank8Url AS rank8Url,
      @rank9Name AS rank9Name, @rank9Url AS rank9Url,
      @rank10Name AS rank10Name, @rank10Url AS rank10Url
  ) S
  ON T.jobId = S.jobId
  WHEN MATCHED THEN UPDATE SET
    T.date = S.date,
    T.businessName = S.businessName,
    T.website = S.website,
    T.rank1Name = S.rank1Name, T.rank1Url = S.rank1Url,
    T.rank2Name = S.rank2Name, T.rank2Url = S.rank2Url,
    T.rank3Name = S.rank3Name, T.rank3Url = S.rank3Url,
    T.rank4Name = S.rank4Name, T.rank4Url = S.rank4Url,
    T.rank5Name = S.rank5Name, T.rank5Url = S.rank5Url,
    T.rank6Name = S.rank6Name, T.rank6Url = S.rank6Url,
    T.rank7Name = S.rank7Name, T.rank7Url = S.rank7Url,
    T.rank8Name = S.rank8Name, T.rank8Url = S.rank8Url,
    T.rank9Name = S.rank9Name, T.rank9Url = S.rank9Url,
    T.rank10Name = S.rank10Name, T.rank10Url = S.rank10Url
  WHEN NOT MATCHED THEN
    INSERT (
      jobId, date, businessName, website,
      rank1Name, rank1Url,
      rank2Name, rank2Url,
      rank3Name, rank3Url,
      rank4Name, rank4Url,
      rank5Name, rank5Url,
      rank6Name, rank6Url,
      rank7Name, rank7Url,
      rank8Name, rank8Url,
      rank9Name, rank9Url,
      rank10Name, rank10Url
    )
    VALUES (
      S.jobId, S.date, S.businessName, S.website,
      S.rank1Name, S.rank1Url,
      S.rank2Name, S.rank2Url,
      S.rank3Name, S.rank3Url,
      S.rank4Name, S.rank4Url,
      S.rank5Name, S.rank5Url,
      S.rank6Name, S.rank6Url,
      S.rank7Name, S.rank7Url,
      S.rank8Name, S.rank8Url,
      S.rank9Name, S.rank9Url,
      S.rank10Name, S.rank10Url
    )
`;

/**
 * Stage handler: called from Pub/Sub (stage = '7_organicSearch')
 * - Marks segment pending
 * - Sends webhook to n8n
 * - Seeds an empty row in 7_organicSearch_Jobs
 */
async function handleOrganicStage(jobId) {
  console.log(`▶️ [ORG] Starting organic search processing for job ${jobId}`);

  const job = await loadJob(jobId);
  if (!job) {
    console.warn(`[ORG] No job row found for jobId ${jobId}`);
    return;
  }

  const servicesRaw = job.services || '';
  let servicesArray = [];
  try {
    const parsed = JSON.parse(servicesRaw);
    if (Array.isArray(parsed)) servicesArray = parsed;
    else if (parsed) servicesArray = [String(parsed)];
  } catch {
    if (servicesRaw) servicesArray = [servicesRaw];
  }

  console.log(
    `ℹ️ [ORG] Job ${jobId} location = "${job.location}", organicSearchStatus = ${job.organicSearchStatus}, services = ${JSON.stringify(
      servicesArray
    )}`
  );

  // Mark segment pending and bump overall status
  console.log("➡️ [ORG] Mark main job 7_organicSearch_Status = 'pending'");
  await markSegmentStatus(jobId, '7_organicSearch_Status', 'pending');
  await updateOverallStatus(jobId);

  const payload = {
    jobId,
    location: job.location,
    services: servicesArray
  };

  console.log(
    `ℹ️ [ORG] Sending payload to n8n webhook: ${ORGANIC_WEBHOOK_URL} → ${JSON.stringify(
      payload
    )}`
  );

  try {
    const resp = await axios.post(ORGANIC_WEBHOOK_URL, payload, {
      timeout: 15000
    });
    console.log(
      `✅ [ORG] n8n webhook call succeeded for job ${jobId}. Response: ${JSON.stringify(
        resp.data
      )}`
    );

    // Seed empty row in 7_organicSearch_Jobs
    const dateIso = bqTimestampToIso(job.createdAt);
    const website = safeStr(job.website);
    const businessName = safeStr(job.businessName);

    console.log(
      `ℹ️ [ORG] Seeding empty row in ${ORGANIC_JOBS_TABLE} for job ${jobId}`
    );
    await bigquery.query({
      query: `
        MERGE \`${ORGANIC_JOBS_TABLE}\` T
        USING (
          SELECT
            @jobId AS jobId,
            TIMESTAMP(@dateIso) AS date,
            @businessName AS businessName,
            @website AS website
        ) S
        ON T.jobId = S.jobId
        WHEN MATCHED THEN
          UPDATE SET
            T.date = S.date,
            T.businessName = S.businessName,
            T.website = S.website
        WHEN NOT MATCHED THEN
          INSERT (jobId, date, businessName, website)
          VALUES (S.jobId, S.date, S.businessName, S.website)
      `,
      params: { jobId, dateIso, businessName, website }
    });
  } catch (err) {
    console.error(
      `❌ [ORG] n8n webhook call FAILED for job ${jobId}:`,
      err.message || err
    );
    await markSegmentStatus(jobId, '7_organicSearch_Status', 'failed');
    await updateOverallStatus(jobId);
  }
}

/**
 * Callback handler: called from /organic-result route when n8n returns data
 */
async function handleOrganicCallback(rawBody) {
  const items = Array.isArray(rawBody) ? rawBody : [rawBody];
  console.log(
    `📥 [ORG-CB] Received ${items.length} organic result item(s) in callback`
  );

  for (const item of items) {
    const {
      jobId,
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
      rank10Url
    } = item;

    if (!jobId) {
      console.warn(
        '[ORG-CB] Missing jobId in organic result payload, skipping item'
      );
      continue;
    }

    const job = await loadJob(jobId);
    if (!job) {
      console.warn(
        `[ORG-CB] No job row found for jobId ${jobId}, marking segment failed`
      );
      await markSegmentStatus(jobId, '7_organicSearch_Status', 'failed');
      await updateOverallStatus(jobId);
      continue;
    }

    const dateIso = bqTimestampToIso(job.createdAt);
    const website = safeStr(job.website);
    const businessName = safeStr(job.businessName);

    console.log(
      `▶️ [ORG-CB] Processing organic callback for job ${jobId} (location=${job.location}, services=${job.services})`
    );

    try {
      await bigquery.query({
        query: MERGE_ORGANIC_SQL,
        params: {
          jobId,
          dateIso,
          businessName,
          website,
          rank1Name: safeStr(rank1Name),
          rank1Url: safeStr(rank1Url),
          rank2Name: safeStr(rank2Name),
          rank2Url: safeStr(rank2Url),
          rank3Name: safeStr(rank3Name),
          rank3Url: safeStr(rank3Url),
          rank4Name: safeStr(rank4Name),
          rank4Url: safeStr(rank4Url),
          rank5Name: safeStr(rank5Name),
          rank5Url: safeStr(rank5Url),
          rank6Name: safeStr(rank6Name),
          rank6Url: safeStr(rank6Url),
          rank7Name: safeStr(rank7Name),
          rank7Url: safeStr(rank7Url),
          rank8Name: safeStr(rank8Name),
          rank8Url: safeStr(rank8Url),
          rank9Name: safeStr(rank9Name),
          rank9Url: safeStr(rank9Url),
          rank10Name: safeStr(rank10Name),
          rank10Url: safeStr(rank10Url)
        }
      });
      console.log(
        `✅ [ORG-CB] MERGE completed for job ${jobId} into ${ORGANIC_JOBS_TABLE}`
      );

      await markSegmentStatus(jobId, '7_organicSearch_Status', 'completed');
      await updateOverallStatus(jobId);
    } catch (err) {
      console.error(
        `❌ [ORG-CB] MERGE FAILED for job ${jobId}:`,
        err && err.errors ? err.errors : err
      );
      await markSegmentStatus(jobId, '7_organicSearch_Status', 'failed');
      await updateOverallStatus(jobId);
    }
  }
}

module.exports = {
  handleOrganicStage,
  handleOrganicCallback
};
