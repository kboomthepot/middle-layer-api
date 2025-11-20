const express = require('express');
const bodyParser = require('body-parser');
const { BigQuery } = require('@google-cloud/bigquery');
const axios = require('axios');

// ---------- CONFIG ----------
const PROJECT_ID = 'ghs-construction-1734441714520';

// BigQuery datasets & tables
const DATASET_ID = 'Client_audits';
const JOBS_TABLE_ID = 'client_audits_jobs';

// Segment tables
const DEMOS_JOBS_TABLE_ID = '1_demographicJobs';
const ORGANIC_JOBS_TABLE_ID = '7_organicSearch_Jobs';

// Demographics source dataset
const DEMOS_DATASET_ID = 'Client_audits_data';
const DEMOS_SOURCE_TABLE_ID = '1_demographics';

// n8n webhook URL for organic search
const ORGANIC_WEBHOOK_URL = 'https://n8n.srv974379.hstgr.cloud/webhook/07_organicSearch';

const bigquery = new BigQuery({ projectId: PROJECT_ID });

const app = express();
app.use(bodyParser.json());

// ---------- HEALTH CHECK ----------
app.get('/', (req, res) => {
  res.send('Worker service listening on port 8080');
});

// ---------- PUBSUB ENTRYPOINT ----------
app.post('/', async (req, res) => {
  try {
    const envelope = req.body;
    if (!envelope || !envelope.message || !envelope.message.data) {
      console.error('❌ Invalid Pub/Sub message format:', JSON.stringify(envelope));
      return res.status(204).send();
    }

    const payload = JSON.parse(
      Buffer.from(envelope.message.data, 'base64').toString()
    );

    console.log('📩 Received job message:', payload);

    const { jobId, location: locationFromMessage, stage } = payload;
    const stageName = stage || 'demographics';

    console.log(
      `✅ Worker received job ${jobId} (stage=${stageName}, location=${locationFromMessage || 'N/A'})`
    );

    if (!jobId) {
      console.error('❌ Missing jobId in message payload. Skipping.');
      return res.status(204).send();
    }

    if (stageName === 'demographics') {
      await processDemographics(jobId, locationFromMessage || null);
    } else if (stageName === '7_organicSearch') {
      await processOrganic(jobId, locationFromMessage || null);
    } else {
      console.warn(`⚠️ Unknown stage "${stageName}" for job ${jobId}, skipping.`);
    }

    res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    res.status(204).send();
  }
});

// ---------- ORGANIC CALLBACK ENDPOINT ----------
app.post('/organic-result', async (req, res) => {
  try {
    const body = req.body;

    let items = [];
    if (Array.isArray(body)) {
      items = body;
    } else if (body && Array.isArray(body.items)) {
      items = body.items;
    } else if (body && body.jobId) {
      items = [body];
    }

    console.log(`📥 [/organic-result] Received ${items.length} organic result item(s)`);

    if (!items.length) {
      return res.json({ ok: true, message: 'No items to process' });
    }

    const item = items[0];
    const jobId = item.jobId;

    if (!jobId) {
      console.error('❌ [/organic-result] Missing jobId in payload, cannot process');
      return res.status(400).json({ error: 'Missing jobId' });
    }

    const job = await loadJob(jobId);
    if (!job) {
      console.error(
        `❌ [/organic-result] Job ${jobId} not found in ${DATASET_ID}.${JOBS_TABLE_ID}`
      );
      return res.status(404).json({ error: 'Job not found' });
    }

    console.log(
      `▶️ [ORG-CB] Processing organic callback for job ${jobId} (location=${job.location}, services=${job.services})`
    );

    await upsertOrganicResults(jobId, job.location || null, item);

    await markSegmentStatus(jobId, '7_organicSearch_Status', 'completed');

    await maybeMarkJobCompleted(jobId);

    res.json({ ok: true });
  } catch (err) {
    console.error('❌ [/organic-result] Error processing callback:', err);
    res.status(500).json({ error: 'Internal error' });
  }
});

// ---------- CORE HELPERS ----------
async function loadJob(jobId) {
  console.log('➡️ Load job row from client_audits_jobs');
  const [rows] = await bigquery.query({
    query: `
      SELECT
        jobId,
        location,
        businessName,
        services,
        status,
        createdAt,
        1_demographics_Status,
        7_organicSearch_Status,
        8_paidAds_Status
      FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId }
  });

  console.log(`ℹ️ loadJob result rows: ${rows.length}`);
  return rows[0] || null;
}

function toNumberOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  if (Number.isNaN(n)) return null;
  return n;
}

function getSafeJobDateIso(createdAt) {
  const nowIso = new Date().toISOString();
  if (!createdAt) {
    console.warn(
      '⚠️ Job createdAt is NULL/undefined; using now() as date for jobs table.'
    );
    return nowIso;
  }

  const raw =
    typeof createdAt === 'object' && createdAt.value
      ? createdAt.value
      : createdAt;

  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) {
    console.warn(
      `⚠️ Invalid createdAt value "${createdAt}"; using now() as date for jobs table.`
    );
    return nowIso;
  }
  return d.toISOString();
}

async function markSegmentStatus(jobId, segmentColumn, segmentStatus) {
  try {
    const [job] = await bigquery.createQueryJob({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET \`${segmentColumn}\` = @segmentStatus
        WHERE jobId = @jobId
      `,
      params: { jobId, segmentStatus }
    });
    await job.getQueryResults();
    console.log(
      `✅ markSegmentStatus: set ${segmentColumn} = '${segmentStatus}' for job ${jobId}`
    );
  } catch (err) {
    console.error(
      `❌ markSegmentStatus: error setting ${segmentColumn} = '${segmentStatus}' for job ${jobId}:`,
      err
    );
  }
}

async function maybeMarkJobCompleted(jobId) {
  try {
    const job = await loadJob(jobId);
    if (!job) return;

    const demoStatus = job['1_demographics_Status'] || 'queued';
    const organicStatus = job['7_organicSearch_Status'] || 'queued';
    const paidAdsStatus = job['8_paidAds_Status'] || 'queued';

    if (
      demoStatus === 'completed' &&
      organicStatus === 'completed' &&
      paidAdsStatus === 'completed'
    ) {
      const [updateJob] = await bigquery.createQueryJob({
        query: `
          UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
          SET status = 'completed'
          WHERE jobId = @jobId
        `,
        params: { jobId }
      });
      await updateJob.getQueryResults();
      console.log(
        `ℹ️ maybeMarkJobCompleted: marked job ${jobId} status='completed'.`
      );
    } else {
      console.log(
        `ℹ️ maybeMarkJobCompleted: job ${jobId} not fully completed yet (required segment statuses=completed,completed,completed).`
      );
    }
  } catch (err) {
    console.error(
      `❌ maybeMarkJobCompleted: error checking/updating job ${jobId}:`,
      err
    );
  }
}

// ---------- DEMOGRAPHICS SEGMENT ----------
async function processDemographics(jobId, locationFromMessage) {
  console.log(`▶️ [DEMOS] Starting demographics processing for job ${jobId}`);

  const job = await loadJob(jobId);
  if (!job) {
    console.warn(
      `⚠️ [DEMOS] Job ${jobId} not found in ${DATASET_ID}.${JOBS_TABLE_ID}.`
    );
    return;
  }

  const location = job.location || locationFromMessage || null;
  const jobDateIso = getSafeJobDateIso(job.createdAt);

  console.log(
    `ℹ️ [DEMOS] Job ${jobId} location = "${location}", demographicsStatus = ${job['1_demographics_Status']}, organicSearchStatus = ${job['7_organicSearch_Status']}, paidAdsStatus = ${job['8_paidAds_Status']}, status = ${job.status}, createdAt = ${JSON.stringify(job.createdAt)}`
  );

  if (!location) {
    console.warn(
      `⚠️ [DEMOS] Job ${jobId} has no location; cannot process demographics.`
    );
    await markSegmentStatus(jobId, '1_demographics_Status', 'failed');
    return;
  }

  console.log("➡️ [DEMOS] Step 2: Mark main job 1_demographics_Status = 'pending'");
  await markSegmentStatus(jobId, '1_demographics_Status', 'pending');

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
      FROM \`${PROJECT_ID}.${DEMOS_DATASET_ID}.${DEMOS_SOURCE_TABLE_ID}\`
      WHERE location = @location
      LIMIT 1
    `,
    params: { location }
  });

  console.log(`ℹ️ [DEMOS] Step 3 result rows (demographics): ${demoRows.length}`);

  if (!demoRows.length) {
    console.warn(
      `⚠️ [DEMOS] No demographics found in ${DEMOS_DATASET_ID}.${DEMOS_SOURCE_TABLE_ID} for location "${location}". Marking as failed.`
    );

    await upsertDemographicsJobRow(jobId, {
      jobId,
      location,
      date: jobDateIso,
      population_no: null,
      median_age: null,
      households_no: null,
      median_income_households: null,
      median_income_families: null,
      male_percentage: null,
      female_percentage: null,
      status: 'failed',
      businessName: job.businessName || null
    });

    await markSegmentStatus(jobId, '1_demographics_Status', 'failed');
    await maybeMarkJobCompleted(jobId);
    return;
  }

  const demo = demoRows[0];

  console.log(
    `ℹ️ [DEMOS] Found demographics for "${location}": ${JSON.stringify(demo)}`
  );

  console.log(
    '➡️ [DEMOS] Step 4: MERGE into 1_demographicJobs with demographics values'
  );

  const parsed = {
    population_no: toNumberOrNull(demo.population_no),
    median_age: toNumberOrNull(demo.median_age),
    households_no: toNumberOrNull(demo.households_no),
    median_income_households: toNumberOrNull(demo.median_income_households),
    median_income_families: toNumberOrNull(demo.median_income_families),
    male_percentage: toNumberOrNull(demo.male_percentage),
    female_percentage: toNumberOrNull(demo.female_percentage)
  };

  const metricsArray = [
    parsed.population_no,
    parsed.median_age,
    parsed.households_no,
    parsed.median_income_households,
    parsed.median_income_families,
    parsed.male_percentage,
    parsed.female_percentage
  ];

  const allNull = metricsArray.every((v) => v === null);
  const allNonNull = metricsArray.every((v) => v !== null);

  let newDemoStatus;
  if (allNull) newDemoStatus = 'failed';
  else if (allNonNull) newDemoStatus = 'completed';
  else newDemoStatus = 'partial';

  console.log(
    `ℹ️ [DEMOS] Step 4 storing demographics for job ${jobId}: ` +
      `pop=${parsed.population_no}, age=${parsed.median_age}, households=${parsed.households_no}, ` +
      `income_hh=${parsed.median_income_households}, income_fam=${parsed.median_income_families}, ` +
      `male=${parsed.male_percentage}, female=${parsed.female_percentage}, ` +
      `status=${newDemoStatus}, date=${jobDateIso}, businessName=${job.businessName || null}`
  );

  await upsertDemographicsJobRow(jobId, {
    ...parsed,
    jobId,
    location,
    status: newDemoStatus,
    date: jobDateIso,
    businessName: job.businessName || null
  });

  console.log('➡️ [DEMOS] Step 5: Update client_audits_jobs.1_demographics_Status');
  await markSegmentStatus(jobId, '1_demographics_Status', newDemoStatus);

  console.log(
    '➡️ [DEMOS] Step 6: Optionally mark main job status = completed if all segments done'
  );
  await maybeMarkJobCompleted(jobId);
}

async function upsertDemographicsJobRow(jobId, data) {
  const nowIso = new Date().toISOString();

  const params = {
    jobId,
    location: data.location || null,
    population_no: data.population_no,
    median_age: data.median_age,
    households_no: data.households_no,
    median_income_households: data.median_income_households,
    median_income_families: data.median_income_families,
    male_percentage: data.male_percentage,
    female_percentage: data.female_percentage,
    status: data.status || 'failed',
    date: data.date,
    createdAt: nowIso,
    updatedAt: nowIso,
    businessName: data.businessName || null
  };

  const mergeQuery = `
    MERGE \`${PROJECT_ID}.${DATASET_ID}.${DEMOS_JOBS_TABLE_ID}\` T
    USING (
      SELECT
        @jobId AS jobId,
        @location AS location,
        @population_no AS population_no,
        @median_age AS median_age,
        @households_no AS households_no,
        @median_income_households AS median_income_households,
        @median_income_families AS median_income_families,
        @male_percentage AS male_percentage,
        @female_percentage AS female_percentage,
        @status AS status,
        @date AS date,
        @createdAt AS createdAt,
        @updatedAt AS updatedAt,
        @businessName AS businessName
    ) S
    ON T.jobId = S.jobId
    WHEN MATCHED THEN
      UPDATE SET
        T.location = S.location,
        T.population_no = S.population_no,
        T.median_age = S.median_age,
        T.households_no = S.households_no,
        T.median_income_households = S.median_income_households,
        T.median_income_families = S.median_income_families,
        T.male_percentage = S.male_percentage,
        T.female_percentage = S.female_percentage,
        T.status = S.status,
        T.date = S.date,
        T.updatedAt = S.updatedAt,
        T.businessName = S.businessName
    WHEN NOT MATCHED THEN
      INSERT (
        jobId,
        location,
        population_no,
        median_age,
        households_no,
        median_income_households,
        median_income_families,
        male_percentage,
        female_percentage,
        status,
        date,
        createdAt,
        updatedAt,
        businessName
      )
      VALUES (
        S.jobId,
        S.location,
        S.population_no,
        S.median_age,
        S.households_no,
        S.median_income_households,
        S.median_income_families,
        S.male_percentage,
        S.female_percentage,
        S.status,
        S.date,
        S.createdAt,
        S.updatedAt,
        S.businessName
      )
  `;

  try {
    const [job] = await bigquery.createQueryJob({
      query: mergeQuery,
      params
    });
    await job.getQueryResults();
    console.log(
      `✅ [DEMOS] MERGE completed for job ${jobId} into ${PROJECT_ID}.${DATASET_ID}.${DEMOS_JOBS_TABLE_ID}`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] MERGE FAILED for job ${jobId} into ${DEMOS_JOBS_TABLE_ID}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
  }
}

// ---------- ORGANIC SEGMENT ----------
async function processOrganic(jobId, locationFromMessage) {
  console.log(`▶️ [ORG] Starting organic search processing for job ${jobId}`);

  const job = await loadJob(jobId);
  if (!job) {
    console.warn(
      `⚠️ [ORG] Job ${jobId} not found in ${DATASET_ID}.${JOBS_TABLE_ID}.`
    );
    return;
  }

  const location = job.location || locationFromMessage || null;

  console.log(
    `ℹ️ [ORG] Job ${jobId} location = "${location}", organicSearchStatus = ${job['7_organicSearch_Status']}, services = ${job.services}`
  );

  console.log("➡️ [ORG] Mark main job 7_organicSearch_Status = 'pending'");
  await markSegmentStatus(jobId, '7_organicSearch_Status', 'pending');

  if (!location) {
    console.warn(
      `⚠️ [ORG] Job ${jobId} has no location; cannot process organic search.`
    );
    await markSegmentStatus(jobId, '7_organicSearch_Status', 'failed');
    await maybeMarkJobCompleted(jobId);
    return;
  }

  let servicesArray = [];
  try {
    if (Array.isArray(job.services)) {
      servicesArray = job.services;
    } else if (typeof job.services === 'string' && job.services.trim() !== '') {
      servicesArray = JSON.parse(job.services);
    }
  } catch (err) {
    console.warn(
      `⚠️ [ORG] Could not parse services JSON for job ${jobId}, using raw string.`
    );
  }

  const requestBody = {
    jobId,
    location,
    services: servicesArray.length ? servicesArray : [job.services || '']
  };

  console.log(
    `ℹ️ [ORG] Sending payload to n8n webhook: ${ORGANIC_WEBHOOK_URL} → ${JSON.stringify(
      requestBody
    )}`
  );

  try {
    const response = await axios.post(ORGANIC_WEBHOOK_URL, requestBody, {
      timeout: 15000,
      headers: { 'Content-Type': 'application/json' }
    });
    console.log(
      `✅ [ORG] n8n webhook call succeeded for job ${jobId}. Response: ${JSON.stringify(
        response.data
      )}`
    );
  } catch (err) {
    console.error(
      `❌ [ORG] Error calling n8n webhook for job ${jobId}:`,
      err.response ? err.response.data : err.message
    );
    await markSegmentStatus(jobId, '7_organicSearch_Status', 'failed');
    await maybeMarkJobCompleted(jobId);
  }
}

async function upsertOrganicResults(jobId, location, item) {
  const nowIso = new Date().toISOString();

  const params = {
    jobId,
    location: location || null,
    services: item.services || null,
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
    status: 'completed',
    createdAt: nowIso,
    updatedAt: nowIso
  };

  const mergeQuery = `
    MERGE \`${PROJECT_ID}.${DATASET_ID}.${ORGANIC_JOBS_TABLE_ID}\` T
    USING (
      SELECT
        @jobId AS jobId,
        @location AS location,
        @services AS services,
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
        @status AS status,
        @createdAt AS createdAt,
        @updatedAt AS updatedAt
    ) S
    ON T.jobId = S.jobId
    WHEN MATCHED THEN
      UPDATE SET
        T.location = S.location,
        T.services = S.services,
        T.rank1Name = S.rank1Name,
        T.rank1Url = S.rank1Url,
        T.rank2Name = S.rank2Name,
        T.rank2Url = S.rank2Url,
        T.rank3Name = S.rank3Name,
        T.rank3Url = S.rank3Url,
        T.rank4Name = S.rank4Name,
        T.rank4Url = S.rank4Url,
        T.rank5Name = S.rank5Name,
        T.rank5Url = S.rank5Url,
        T.rank6Name = S.rank6Name,
        T.rank6Url = S.rank6Url,
        T.rank7Name = S.rank7Name,
        T.rank7Url = S.rank7Url,
        T.rank8Name = S.rank8Name,
        T.rank8Url = S.rank8Url,
        T.rank9Name = S.rank9Name,
        T.rank9Url = S.rank9Url,
        T.rank10Name = S.rank10Name,
        T.rank10Url = S.rank10Url,
        T.status = S.status,
        T.updatedAt = S.updatedAt
    WHEN NOT MATCHED THEN
      INSERT (
        jobId,
        location,
        services,
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
        status,
        createdAt,
        updatedAt
      )
      VALUES (
        S.jobId,
        S.location,
        S.services,
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
        S.status,
        S.createdAt,
        S.updatedAt
      )
  `;

  try {
    const [job] = await bigquery.createQueryJob({
      query: mergeQuery,
      params
    });
    await job.getQueryResults();
    console.log(
      `✅ [ORG-CB] MERGE completed for job ${jobId} into ${PROJECT_ID}.${DATASET_ID}.${ORGANIC_JOBS_TABLE_ID}`
    );
  } catch (err) {
    console.error(
      `❌ [ORG-CB] MERGE FAILED for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
  }
}

// ---------- START SERVER ----------
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Worker service listening on port ${port}`);
});
