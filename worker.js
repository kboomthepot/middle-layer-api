// worker.js
'use strict';

const express = require('express');
const bodyParser = require('body-parser');
const { BigQuery } = require('@google-cloud/bigquery');
const axios = require('axios');

const app = express();
app.use(bodyParser.json());

const bigquery = new BigQuery();

const PROJECT_ID = 'ghs-construction-1734441714520';

// TABLES
const JOBS_TABLE = `${PROJECT_ID}.Client_audits.client_audits_jobs`;
const DEMOS_SOURCE_TABLE = `${PROJECT_ID}.Client_audits_data.1_demographics`;
const DEMOS_JOBS_TABLE = `${PROJECT_ID}.Client_audits.1_demographicJobs`;
const ORGANIC_JOBS_TABLE = `${PROJECT_ID}.Client_audits.7_organicSearch_Jobs`;

// N8N WEBHOOK
const ORGANIC_WEBHOOK_URL = 'https://n8n.srv974379.hstgr.cloud/webhook/07_organicSearch';

/**
 * HELPERS
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
        1_demographics_Status,
        7_organicSearch_Status,
        8_paidAds_Status,
        website
      FROM \`${JOBS_TABLE}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId },
  });

  console.log(`ℹ️ loadJob result rows: ${rows.length}`);
  return rows[0];
}

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
 * Update a specific "segment status" column (e.g. 1_demographics_Status).
 */
async function markSegmentStatus(jobId, statusColumn, newStatus) {
  console.log(
    `✅ markSegmentStatus: set ${statusColumn} = '${newStatus}' for job ${jobId}`
  );
  await bigquery.query({
    query: `
      UPDATE \`${JOBS_TABLE}\`
      SET ${statusColumn} = @newStatus
      WHERE jobId = @jobId
    `,
    params: { jobId, newStatus },
  });
}

/**
 * Optionally mark the whole job as completed if all key segments are done.
 * For now we require:
 *   1_demographics_Status = 'completed'
 *   7_organicSearch_Status = 'completed'
 *   8_paidAds_Status      = 'completed'
 */
async function maybeMarkJobCompleted(jobId) {
  console.log('➡️ Load job row from client_audits_jobs');
  const [rows] = await bigquery.query({
    query: `
      SELECT
        jobId,
        status,
        1_demographics_Status,
        7_organicSearch_Status,
        8_paidAds_Status
      FROM \`${JOBS_TABLE}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId },
  });

  console.log(`ℹ️ loadJob result rows: ${rows.length}`);
  if (!rows.length) return;

  const job = rows[0];

  const demo = job['1_demographics_Status'] || 'queued';
  const organic = job['7_organicSearch_Status'] || 'queued';
  const paid = job['8_paidAds_Status'] || 'queued';

  console.log(
    `ℹ️ maybeMarkJobCompleted: current segments for ${jobId} => demo=${demo}, organic=${organic}, paid=${paid}`
  );

  if (demo === 'completed' && organic === 'completed' && paid === 'completed') {
    await bigquery.query({
      query: `
        UPDATE \`${JOBS_TABLE}\`
        SET status = 'completed'
        WHERE jobId = @jobId
      `,
      params: { jobId },
    });
    console.log(
      `ℹ️ maybeMarkJobCompleted: marked job ${jobId} status='completed'.`
    );
  } else {
    console.log(
      `ℹ️ maybeMarkJobCompleted: job ${jobId} not fully completed yet (required segment statuses=completed,completed,completed).`
    );
  }
}

/**
 * MERGE SQL for demographics result table.
 */
const MERGE_DEMOGRAPHICS_SQL = `
  MERGE \`${DEMOS_JOBS_TABLE}\` T
  USING (
    SELECT
      @jobId AS jobId,
      TIMESTAMP(@dateIso) AS date,
      @status AS status,
      @businessName AS businessName,
      CAST(@population_no AS NUMERIC) AS population_no,
      CAST(@households_no AS NUMERIC) AS households_no,
      CAST(@median_age AS FLOAT64) AS median_age,
      CAST(@median_income_households AS NUMERIC) AS median_income_households,
      CAST(@median_income_families AS NUMERIC) AS median_income_families,
      CAST(@male_percentage AS FLOAT64) AS male_percentage,
      CAST(@female_percentage AS FLOAT64) AS female_percentage
  ) S
  ON T.jobId = S.jobId
  WHEN MATCHED THEN
    UPDATE SET
      T.date = S.date,
      T.status = S.status,
      T.businessName = S.businessName,
      T.population_no = S.population_no,
      T.households_no = S.households_no,
      T.median_age = S.median_age,
      T.median_income_households = S.median_income_households,
      T.median_income_families = S.median_income_families,
      T.male_percentage = S.male_percentage,
      T.female_percentage = S.female_percentage
  WHEN NOT MATCHED THEN
    INSERT (
      jobId,
      date,
      status,
      businessName,
      population_no,
      households_no,
      median_age,
      median_income_households,
      median_income_families,
      male_percentage,
      female_percentage
    )
    VALUES (
      S.jobId,
      S.date,
      S.status,
      S.businessName,
      S.population_no,
      S.households_no,
      S.median_age,
      S.median_income_households,
      S.median_income_families,
      S.male_percentage,
      S.female_percentage
    )
`;

/**
 * MERGE SQL for organic search results table.
 */
const MERGE_ORGANIC_SQL = `
  MERGE \`${ORGANIC_JOBS_TABLE}\` T
  USING (
    SELECT
      @jobId AS jobId,
      TIMESTAMP(@dateIso) AS date,
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
      jobId, date, website,
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
      S.jobId, S.date, S.website,
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
 * DEMOGRAPHICS STAGE HANDLER
 */
async function handleDemographicsStage(jobId) {
  console.log(`▶️ [DEMOS] Starting demographics processing for job ${jobId}`);

  const job = await loadJob(jobId);
  if (!job) {
    console.warn(`[DEMOS] No job row found for jobId ${jobId}`);
    return;
  }

  const createdAtIso = bqTimestampToIso(job.createdAt);

  console.log(
    `ℹ️ [DEMOS] Job ${jobId} location = "${job.location}", demographicsStatus = ${job['1_demographics_Status']}, organicSearchStatus = ${job['7_organicSearch_Status']}, paidAdsStatus = ${job['8_paidAds_Status']}, status = ${job.status}, createdAt = ${JSON.stringify(
      job.createdAt
    )}`
  );

  // Step 2: mark demographics segment as pending
  console.log(
    "➡️ [DEMOS] Step 2: Mark main job 1_demographics_Status = 'pending'"
  );
  await markSegmentStatus(jobId, '1_demographics_Status', 'pending');

  // Step 3: Load demographics from source table
  console.log(
    '➡️ [DEMOS] Step 3: Load demographics from Client_audits_data.1_demographics'
  );
  const [demoRows] = await bigquery.query({
    query: `
      SELECT
        population_no,
        median_age,
        households_no,
        median_income_households,
        median_income_families,
        male_percentage,
        female_percentage
      FROM \`${DEMOS_SOURCE_TABLE}\`
      WHERE location = @location
      LIMIT 1
    `,
    params: { location: job.location },
  });

  console.log(
    `ℹ️ [DEMOS] Step 3 result rows (demographics): ${demoRows.length}`
  );
  if (!demoRows.length) {
    console.warn(
      `[DEMOS] No demographics row found for location "${job.location}", marking failed.`
    );
    await markSegmentStatus(jobId, '1_demographics_Status', 'failed');
    return;
  }

  const d = demoRows[0];
  console.log(
    `ℹ️ [DEMOS] Found demographics for "${job.location}": ${JSON.stringify(d)}`
  );

  console.log(
    '➡️ [DEMOS] Step 4: MERGE into 1_demographicJobs with demographics values'
  );
  console.log(
    `ℹ️ [DEMOS] Step 4 storing demographics for job ${jobId}: pop=${d.population_no}, age=${d.median_age}, households=${d.households_no}, income_hh=${d.median_income_households}, income_fam=${d.median_income_families}, male=${d.male_percentage}, female=${d.female_percentage}, status=completed, date=${createdAtIso}, businessName=${job.businessName}`
  );

  try {
    await bigquery.query({
      query: MERGE_DEMOGRAPHICS_SQL,
      params: {
        jobId,
        dateIso: createdAtIso,
        status: 'completed',
        businessName: job.businessName || null,
        population_no: d.population_no,
        households_no: d.households_no,
        median_age: d.median_age,
        median_income_households: d.median_income_households,
        median_income_families: d.median_income_families,
        male_percentage: d.male_percentage,
        female_percentage: d.female_percentage,
      },
    });
    console.log(
      `✅ [DEMOS] MERGE completed for job ${jobId} into ${DEMOS_JOBS_TABLE}`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] MERGE FAILED for job ${jobId} into 1_demographicJobs:`,
      err && err.errors ? err.errors : err
    );
    await markSegmentStatus(jobId, '1_demographics_Status', 'failed');
    return;
  }

  console.log('➡️ [DEMOS] Step 5: Update client_audits_jobs.1_demographics_Status');
  await markSegmentStatus(jobId, '1_demographics_Status', 'completed');

  console.log(
    '➡️ [DEMOS] Step 6: Optionally mark main job status = completed if all segments done'
  );
  await maybeMarkJobCompleted(jobId);
}

/**
 * ORGANIC SEARCH STAGE (trigger N8N)
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
    `ℹ️ [ORG] Job ${jobId} location = "${job.location}", organicSearchStatus = ${job['7_organicSearch_Status']}, services = ${JSON.stringify(
      servicesArray
    )}`
  );

  console.log("➡️ [ORG] Mark main job 7_organicSearch_Status = 'pending'");
  await markSegmentStatus(jobId, '7_organicSearch_Status', 'pending');

  const payload = {
    jobId,
    location: job.location,
    services: servicesArray,
  };

  console.log(
    `ℹ️ [ORG] Sending payload to n8n webhook: ${ORGANIC_WEBHOOK_URL} → ${JSON.stringify(
      payload
    )}`
  );

  try {
    const resp = await axios.post(ORGANIC_WEBHOOK_URL, payload, {
      timeout: 15000,
    });
    console.log(
      `✅ [ORG] n8n webhook call succeeded for job ${jobId}. Response: ${JSON.stringify(
        resp.data
      )}`
    );
  } catch (err) {
    console.error(
      `❌ [ORG] n8n webhook call FAILED for job ${jobId}:`,
      err.message || err
    );
    await markSegmentStatus(jobId, '7_organicSearch_Status', 'failed');
  }
}

/**
 * ORGANIC SEARCH CALLBACK - receives results from N8N and writes to BigQuery
 */
app.post('/organic-result', async (req, res) => {
  try {
    const items = Array.isArray(req.body) ? req.body : [req.body];
    console.log(
      `📥 [/organic-result] Received ${items.length} organic result item(s)`
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
        rank10Url,
      } = item;

      if (!jobId) {
        console.warn('[ORG-CB] Missing jobId in organic result payload, skip');
        continue;
      }

      // Load job for date & website
      const job = await loadJob(jobId);
      if (!job) {
        console.warn(
          `[ORG-CB] No job row found for jobId ${jobId}, skipping merge`
        );
        continue;
      }

      const dateIso = bqTimestampToIso(job.createdAt);
      const website = job.website || null;

      console.log(
        `▶️ [ORG-CB] Processing organic callback for job ${jobId} (location=${job.location}, services=${job.services})`
      );

      try {
        await bigquery.query({
          query: MERGE_ORGANIC_SQL,
          params: {
            jobId,
            dateIso,
            website,
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
          },
        });
        console.log(
          `✅ [ORG-CB] MERGE completed for job ${jobId} into ${ORGANIC_JOBS_TABLE}`
        );
      } catch (err) {
        console.error(
          `❌ [ORG-CB] MERGE FAILED for job ${jobId}:`,
          err && err.errors ? err.errors : err
        );
      }

      // Mark organic segment complete regardless of merge outcome for now
      await markSegmentStatus(jobId, '7_organicSearch_Status', 'completed');
      await maybeMarkJobCompleted(jobId);
    }

    res.status(200).json({ ok: true });
  } catch (err) {
    console.error('❌ Error in /organic-result handler:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * PUB/SUB PUSH ENDPOINT
 * Google Pub/Sub will POST here with base64-encoded data.
 */
app.post('/', async (req, res) => {
  try {
    const message = req.body && req.body.message;
    if (!message || !message.data) {
      console.error('❌ Invalid Pub/Sub message format', req.body);
      res.status(400).send('Bad Request: no message.data');
      return;
    }

    const dataBuffer = Buffer.from(message.data, 'base64');
    const payload = JSON.parse(dataBuffer.toString('utf8'));

    const { jobId, location, createdAt, stage } = payload;

    console.log('📩 Received job message:', payload);

    if (!jobId || !stage) {
      console.error('❌ Missing jobId or stage in Pub/Sub payload');
      res.status(400).send('Bad Request');
      return;
    }

    console.log(
      `✅ Worker received job ${jobId} (stage=${stage}, location=${location})`
    );

    if (stage === 'demographics') {
      await handleDemographicsStage(jobId);
    } else if (stage === '7_organicSearch') {
      await handleOrganicStage(jobId);
    } else {
      console.log(`ℹ️ Unknown stage "${stage}" - nothing to do yet.`);
    }

    res.status(204).send(); // Pub/Sub requires a 2xx
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    res.status(500).send();
  }
});

/**
 * Simple GET health check for Cloud Run.
 */
app.get('/', (req, res) => {
  res.status(200).send('OK');
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`🚀 client-audits-worker listening on port ${PORT}`);
});
