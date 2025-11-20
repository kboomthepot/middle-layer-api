// worker.js
const express = require('express');
const bodyParser = require('body-parser');
const { BigQuery } = require('@google-cloud/bigquery');
const axios = require('axios');

// ---------- CONFIG ----------
const PROJECT_ID = 'ghs-construction-1734441714520';

// Main jobs table (source)
const DATASET_ID = 'Client_audits';
const JOBS_TABLE_ID = 'client_audits_jobs';

// Demographics data source
const DEMOS_DATASET_ID = 'Client_audits_data';
const DEMOS_SOURCE_TABLE_ID = '1_demographics';

// Demographics jobs table (target)
const DEMOS_TARGET_TABLE_ID = '1_demographicJobs';

// Organic search jobs table (target)
const ORG_TARGET_TABLE_ID = '7_organicSearch_Jobs';

// n8n webhook for organic jobs
const N8N_WEBHOOK_URL =
  'https://n8n.srv974379.hstgr.cloud/webhook/07_organicSearch';

const bigquery = new BigQuery({ projectId: PROJECT_ID });

const app = express();
app.use(bodyParser.json());

// ---------- HEALTH CHECK ----------
app.get('/', (req, res) => {
  res.send('Worker service listening on port 8080');
});

// ---------- PUB/SUB ENDPOINT ----------
app.post('/', async (req, res) => {
  try {
    const envelope = req.body;
    if (!envelope || !envelope.message || !envelope.message.data) {
      console.error('❌ Invalid Pub/Sub message format:', JSON.stringify(envelope));
      // ACK anyway so Pub/Sub doesn’t retry forever
      return res.status(204).send();
    }

    const payload = JSON.parse(
      Buffer.from(envelope.message.data, 'base64').toString()
    );

    console.log('📩 Received job message:', payload);

    const { jobId, location: locationFromMessage, stage } = payload;

    console.log(
      `✅ Worker received job ${jobId} (stage=${stage || 'N/A'}, location=${
        locationFromMessage || 'N/A'
      })`
    );

    if (!jobId) {
      console.error('❌ Missing jobId in message payload. Skipping.');
      return res.status(204).send();
    }

    // Route by stage
    if (!stage || stage === 'demographics') {
      await processDemographicsStage(jobId, locationFromMessage || null);
    } else if (stage === '7_organicSearch') {
      await processOrganicSearchStage(jobId, locationFromMessage || null);
    } else {
      console.log(`ℹ️ Unknown stage "${stage}" for job ${jobId}, skipping.`);
    }

    // Always ACK so Pub/Sub does not retry this message
    res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    // ACK even on error to avoid infinite retry loop
    res.status(204).send();
  }
});

// ======================================================
// =============== DEMOGRAPHICS STAGE ===================
// ======================================================

async function processDemographicsStage(jobId, locationFromMessage = null) {
  console.log(`▶️ [DEMOS] Starting demographics processing for job ${jobId}`);

  // ---- Step 1: Load job row from client_audits_jobs ----
  console.log('➡️ Load job row from client_audits_jobs');
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
    `ℹ️ [DEMOS] Job ${jobId} location = "${location}", ` +
      `1_demographics_Status = ${job['1_demographics_Status']}, ` +
      `7_organicSearch_Status = ${job['7_organicSearch_Status']}, ` +
      `8_paidAds_Status = ${job['8_paidAds_Status']}, ` +
      `status = ${job.status}, createdAt = ${JSON.stringify(job.createdAt)}`
  );

  if (!location) {
    console.warn(
      `⚠️ [DEMOS] Job ${jobId} has no location; cannot process demographics.`
    );
    // Mark main job as failed for this segment
    await markSegmentStatus(
      jobId,
      '1_demographics_Status',
      'failed',
      'demographicsStatus'
    );
    return;
  }

  // ---- Step 2: Mark main job demographics segment = pending ----
  console.log('➡️ [DEMOS] Step 2: Mark main job 1_demographics_Status = pending');

  await markSegmentStatus(
    jobId,
    '1_demographics_Status',
    'pending',
    'demographicsStatus'
  );

  // ---- Step 3: Load demographics source row ----
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
    params: { location },
  });

  console.log(`ℹ️ [DEMOS] Step 3 result rows (demographics): ${demoRows.length}`);

  if (!demoRows.length) {
    console.warn(
      `⚠️ [DEMOS] No demographics found in ${DEMOS_DATASET_ID}.${DEMOS_SOURCE_TABLE_ID} for location "${location}". Marking as failed.`
    );

    await upsertDemographicsJobRow(job, {
      population_no: null,
      median_age: null,
      households_no: null,
      median_income_households: null,
      median_income_families: null,
      male_percentage: null,
      female_percentage: null,
      status: 'failed',
      dateIso: jobDateIso,
    });

    await markSegmentStatus(
      jobId,
      '1_demographics_Status',
      'failed',
      'demographicsStatus'
    );
    await maybeMarkJobCompleted(jobId);
    return;
  }

  const demo = demoRows[0];

  console.log(
    `ℹ️ [DEMOS] Found demographics for "${location}": ${JSON.stringify(demo)}`
  );

  // ---- Step 4: Compute metrics + status; MERGE into 1_demographicJobs ----
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
    female_percentage: toNumberOrNull(demo.female_percentage),
  };

  const metricsArray = [
    parsed.population_no,
    parsed.median_age,
    parsed.households_no,
    parsed.median_income_households,
    parsed.median_income_families,
    parsed.male_percentage,
    parsed.female_percentage,
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
      `status=${newDemoStatus}, date=${jobDateIso}, businessName=${job.businessName}`
  );

  try {
    await upsertDemographicsJobRow(job, {
      ...parsed,
      status: newDemoStatus,
      dateIso: jobDateIso,
    });

    console.log(
      `✅ [DEMOS] MERGE completed for job ${jobId} into ${DATASET_ID}.${DEMOS_TARGET_TABLE_ID}`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] MERGE FAILED for job ${jobId} into ${DEMOS_TARGET_TABLE_ID}:`,
      err && err.errors ? err.errors : err
    );

    await markSegmentStatus(
      jobId,
      '1_demographics_Status',
      'failed',
      'demographicsStatus'
    );
    await maybeMarkJobCompleted(jobId);
    return;
  }

  // ---- Step 5: Update main job's demographics segment status ----
  console.log('➡️ [DEMOS] Step 5: Update client_audits_jobs.1_demographics_Status');

  await markSegmentStatus(
    jobId,
    '1_demographics_Status',
    newDemoStatus,
    'demographicsStatus'
  );

  // ---- Step 6: Optionally update main job.status if all segments done ----
  console.log(
    '➡️ [DEMOS] Step 6: Optionally mark main job status = completed if all segments done'
  );
  await maybeMarkJobCompleted(jobId);
}

// ======================================================
// ============ ORGANIC SEARCH STAGE ====================
// ======================================================

async function processOrganicSearchStage(jobId, locationFromMessage = null) {
  console.log(`▶️ [ORG] Starting organic search processing for job ${jobId}`);

  const job = await loadJob(jobId);
  if (!job) {
    console.warn(
      `[ORG] Job ${jobId} not found in ${DATASET_ID}.${JOBS_TABLE_ID}. Skipping.`
    );
    return;
  }

  const location = job.location || locationFromMessage || null;

  let servicesArray = [];
  try {
    if (job.services) {
      servicesArray = Array.isArray(job.services)
        ? job.services
        : JSON.parse(job.services);
    }
  } catch (e) {
    console.warn(
      `[ORG] Could not parse services JSON for job ${jobId}, raw=`,
      job.services
    );
  }
  if (!Array.isArray(servicesArray)) servicesArray = [];

  // 1) Mark 7_organicSearch segment as pending
  await markSegmentStatus(
    jobId,
    '7_organicSearch_Status',
    'pending',
    'organicSearchStatus'
  );

  // 2) Ensure a base row exists in 7_organicSearch_Jobs
  await ensureOrganicJobRow(job);

  // 3) Send payload to n8n
  const payload = {
    jobId,
    location,
    services: servicesArray,
  };

  console.log(
    `ℹ️ [ORG] Sending payload to n8n webhook: ${N8N_WEBHOOK_URL} → ${JSON.stringify(
      payload
    )}`
  );

  try {
    const resp = await axios.post(N8N_WEBHOOK_URL, payload, {
      timeout: 30000,
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
    // You might want to mark this segment as failed here, or leave as pending and retry externally
    await markSegmentStatus(
      jobId,
      '7_organicSearch_Status',
      'failed',
      'organicSearchStatus'
    );
    await maybeMarkJobCompleted(jobId);
  }
}

// ---------- ORGANIC RESULT CALLBACK (from n8n) ----------

app.post('/organic-result', async (req, res) => {
  try {
    const body = req.body;
    const items = Array.isArray(body) ? body : [body];

    console.log(
      `📥 [/organic-result] Received ${items.length} organic result item(s)`
    );

    if (!items.length) {
      return res.status(400).json({ error: 'No organic results provided' });
    }

    const item = items[0];

    const {
      jobId,
      services,
      location,
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
      console.error('❌ [/organic-result] Missing jobId in payload');
      return res.status(400).json({ error: 'jobId is required' });
    }

    console.log(
      `▶️ [ORG-CB] Processing organic callback for job ${jobId} (location=${location}, services=${services})`
    );

    const job = await loadJob(jobId);
    if (!job) {
      console.warn(
        `[ORG-CB] Job ${jobId} not found in client_audits_jobs. Still attempting to update ${ORG_TARGET_TABLE_ID}.`
      );
    }

    // UPDATE existing row in 7_organicSearch_Jobs
    const updateQuery = `
      UPDATE \`${PROJECT_ID}.${DATASET_ID}.${ORG_TARGET_TABLE_ID}\`
      SET
        rank1Name = @rank1Name,
        rank1Url  = @rank1Url,
        rank2Name = @rank2Name,
        rank2Url  = @rank2Url,
        rank3Name = @rank3Name,
        rank3Url  = @rank3Url,
        rank4Name = @rank4Name,
        rank4Url  = @rank4Url,
        rank5Name = @rank5Name,
        rank5Url  = @rank5Url,
        rank6Name = @rank6Name,
        rank6Url  = @rank6Url,
        rank7Name = @rank7Name,
        rank7Url  = @rank7Url,
        rank8Name = @rank8Name,
        rank8Url  = @rank8Url,
        rank9Name = @rank9Name,
        rank9Url  = @rank9Url,
        rank10Name = @rank10Name,
        rank10Url  = @rank10Url,
        status    = 'completed'
      WHERE jobId = @jobId
    `;

    const [updateJob] = await bigquery.createQueryJob({
      query: updateQuery,
      params: {
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
      },
    });

    const [updateResult] = await updateJob.getQueryResults();
    console.log(
      `ℹ️ [ORG-CB] UPDATE 7_organicSearch_Jobs result for job ${jobId}:`,
      updateResult && updateResult[0] ? JSON.stringify(updateResult[0]) : 'no DML stats row'
    );

    await markSegmentStatus(
      jobId,
      '7_organicSearch_Status',
      'completed',
      'organicSearchStatus'
    );
    await maybeMarkJobCompleted(jobId);

    res.json({ ok: true });
  } catch (err) {
    console.error(
      '❌ [/organic-result] Error processing organic callback:',
      err.message || err
    );
    res.status(500).json({ error: 'Failed to process organic results' });
  }
});

// ======================================================
// =================== HELPERS ==========================
// ======================================================

async function loadJob(jobId) {
  const [rows] = await bigquery.query({
    query: `
      SELECT *
      FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId },
  });
  console.log(`ℹ️ loadJob result rows: ${rows.length}`);
  return rows[0] || null;
}

function toNumberOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  // Strip commas and plus if present (e.g. "250,000+")
  const cleaned = String(value).replace(/[,+]/g, '');
  const n = Number(cleaned);
  if (Number.isNaN(n)) return null;
  return n;
}

function getSafeJobDateIso(createdAt) {
  const nowIso = new Date().toISOString();
  if (!createdAt) {
    console.warn(
      `⚠️ Job createdAt is NULL/undefined; using now() as date for demographic/organic tables.`
    );
    return nowIso;
  }
  const raw = createdAt.value || createdAt;
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) {
    console.warn(
      `⚠️ Invalid createdAt value "${raw}"; using now() as date for demographic/organic tables.`
    );
    return nowIso;
  }
  return d.toISOString();
}

async function markSegmentStatus(jobId, columnName, newStatus, segmentName) {
  // segmentName is just for logs, e.g. 'demographicsStatus' or 'organicSearchStatus'
  try {
    const [job] = await bigquery.createQueryJob({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET \`${columnName}\` = @status
        WHERE jobId = @jobId
      `,
      params: { jobId, status: newStatus },
    });
    await job.getQueryResults();
    console.log(
      `✅ markSegmentStatus: set ${segmentName} (column ${columnName}) = '${newStatus}' for job ${jobId}`
    );
  } catch (err) {
    console.error(
      `❌ markSegmentStatus: error setting ${segmentName} (column ${columnName}) = '${newStatus}' for job ${jobId}:`,
      err
    );
  }
}

// Upsert row into 1_demographicJobs
async function upsertDemographicsJobRow(job, data) {
  const jobId = job.jobId;
  const jobDateIso = data.dateIso;
  const businessName = job.businessName || null;
  const location = job.location || null;

  const query = `
    MERGE \`${PROJECT_ID}.${DATASET_ID}.${DEMOS_TARGET_TABLE_ID}\` AS t
    USING (
      SELECT
        @jobId AS jobId,
        @date AS date,
        @businessName AS businessName,
        @location AS location,
        @population_no AS population_no,
        @median_age AS median_age,
        @households_no AS households_no,
        @median_income_households AS median_income_households,
        @median_income_families AS median_income_families,
        @male_percentage AS male_percentage,
        @female_percentage AS female_percentage,
        @status AS status
    ) AS s
    ON t.jobId = s.jobId
    WHEN MATCHED THEN
      UPDATE SET
        t.date = TIMESTAMP(s.date),
        t.businessName = s.businessName,
        t.location = s.location,
        t.population_no = s.population_no,
        t.median_age = s.median_age,
        t.households_no = s.households_no,
        t.median_income_households = s.median_income_households,
        t.median_income_families = s.median_income_families,
        t.male_percentage = s.male_percentage,
        t.female_percentage = s.female_percentage,
        t.status = s.status
    WHEN NOT MATCHED THEN
      INSERT (
        jobId,
        date,
        businessName,
        location,
        population_no,
        median_age,
        households_no,
        median_income_households,
        median_income_families,
        male_percentage,
        female_percentage,
        status
      )
      VALUES (
        s.jobId,
        TIMESTAMP(s.date),
        s.businessName,
        s.location,
        s.population_no,
        s.median_age,
        s.households_no,
        s.median_income_households,
        s.median_income_families,
        s.male_percentage,
        s.female_percentage,
        s.status
      )
  `;

  await bigquery.query({
    query,
    params: {
      jobId,
      date: jobDateIso,
      businessName,
      location,
      population_no: data.population_no,
      median_age: data.median_age,
      households_no: data.households_no,
      median_income_households: data.median_income_households,
      median_income_families: data.median_income_families,
      male_percentage: data.male_percentage,
      female_percentage: data.female_percentage,
      status: data.status,
    },
  });
}

// Ensure base row for 7_organicSearch_Jobs
async function ensureOrganicJobRow(job) {
  const jobId = job.jobId;
  const jobDateIso = getSafeJobDateIso(job.createdAt);
  const businessName = job.businessName || null;

  const query = `
    MERGE \`${PROJECT_ID}.${DATASET_ID}.${ORG_TARGET_TABLE_ID}\` AS t
    USING (
      SELECT
        @jobId AS jobId,
        @date AS date,
        @businessName AS businessName,
        @status AS status
    ) AS s
    ON t.jobId = s.jobId
    WHEN MATCHED THEN
      UPDATE SET
        t.date = TIMESTAMP(s.date),
        t.businessName = s.businessName,
        t.status = s.status
    WHEN NOT MATCHED THEN
      INSERT (jobId, date, businessName, status)
      VALUES (s.jobId, TIMESTAMP(s.date), s.businessName, s.status)
  `;

  try {
    await bigquery.query({
      query,
      params: {
        jobId,
        date: jobDateIso,
        businessName,
        status: 'pending',
      },
    });
    console.log(
      `ℹ️ [ORG] ensureOrganicJobRow: upserted base row in ${ORG_TARGET_TABLE_ID} for job ${jobId}`
    );
  } catch (err) {
    console.error(
      `❌ [ORG] ensureOrganicJobRow MERGE failed for job ${jobId}:`,
      err.message || err
    );
  }
}

// Decide if the overall job.status can be marked completed
async function maybeMarkJobCompleted(jobId) {
  const job = await loadJob(jobId);
  if (!job) {
    console.warn(`[maybeMarkJobCompleted] Job ${jobId} not found.`);
    return;
  }

  const demoStatus = job['1_demographics_Status'];
  const paidAdsStatus = job['8_paidAds_Status'];
  const organicStatus = job['7_organicSearch_Status'];

  // Adjust this logic as you bring more modules online.
  const requiredStatuses = [demoStatus, paidAdsStatus, organicStatus];

  console.log(
    `ℹ️ maybeMarkJobCompleted: job ${jobId} statuses=` +
      `demographics=${demoStatus}, paidAds=${paidAdsStatus}, organic=${organicStatus}`
  );

  // For now: only mark completed if demographics + organic + paidAds are all 'completed'
  const allCompleted = requiredStatuses.every((s) => s === 'completed');

  if (!allCompleted) {
    console.log(
      `ℹ️ maybeMarkJobCompleted: job ${jobId} not fully completed yet (required all segments 'completed').`
    );
    return;
  }

  try {
    const [jobResp] = await bigquery.createQueryJob({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET status = 'completed'
        WHERE jobId = @jobId
      `,
      params: { jobId },
    });
    await jobResp.getQueryResults();
    console.log(
      `ℹ️ maybeMarkJobCompleted: marked job ${jobId} status='completed'.`
    );
  } catch (err) {
    console.error(
      `❌ maybeMarkJobCompleted: error updating status for job ${jobId}:`,
      err
    );
  }
}

// ---------- START SERVER ----------
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Worker service listening on port ${port}`);
});
