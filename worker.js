// worker.js
const express = require('express');
const bodyParser = require('body-parser');
const { BigQuery } = require('@google-cloud/bigquery');

// ---------- CONFIG ----------
const PROJECT_ID = 'ghs-construction-1734441714520';

// Main jobs table (source)
const DATASET_ID = 'Client_audits';
const JOBS_TABLE_ID = 'client_audits_jobs';

// Demographics data source
const DEMOS_DATASET_ID = 'Client_audits_data';
const DEMOS_SOURCE_TABLE_ID = '1_demographics';

// Demographics jobs table (target)
const DEMO_JOBS_TABLE_ID = '1_demographicJobs';

// Organic search jobs table (target)
const ORGANIC_JOBS_TABLE_ID = '7_organicSearch_Jobs';

// n8n webhook for organic search (used when TRIGGERING n8n)
const ORGANIC_WEBHOOK_URL =
  'https://n8n.srv974379.hstgr.cloud/webhook/07_organicSearch';

const bigquery = new BigQuery({ projectId: PROJECT_ID });

const app = express();
app.use(bodyParser.json());

// ---------- HEALTH CHECK ----------
app.get('/', (req, res) => {
  res.send('Worker service listening on port 8080');
});

// ======================================================================
//                   1) PUB/SUB ENTRYPOINT  (DEMOS + ORGANIC TRIGGER)
// ======================================================================
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

    const {
      jobId,
      location: locationFromMessage,
      stage = 'demographics',
    } = payload;

    console.log(
      `✅ Worker received job ${jobId} (stage=${stage}, location=${
        locationFromMessage || 'N/A'
      })`
    );

    if (!jobId) {
      console.error('❌ Missing jobId in message payload. Skipping.');
      return res.status(204).send();
    }

    await handleJobMessage({
      jobId,
      locationFromMessage: locationFromMessage || null,
      stage,
    });

    res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    res.status(204).send();
  }
});

// ======================================================================
//           2) HTTP CALLBACK ENTRYPOINT FOR ORGANIC RESULTS (n8n)
// ======================================================================
// n8n will POST the array you showed here.
app.post('/organic-result', async (req, res) => {
  try {
    const body = req.body;

    const items = Array.isArray(body) ? body : [body];

    console.log(
      `📥 [/organic-result] Received ${items.length} organic result item(s)`
    );

    for (const item of items) {
      await processOrganicResultCallback(item);
    }

    res.json({ ok: true, processed: items.length });
  } catch (err) {
    console.error('❌ Error in /organic-result handler:', err);
    res.status(500).json({ ok: false, error: err.message || String(err) });
  }
});

// ======================================================================
//                       MESSAGE ROUTER (MULTI-STAGE)
// ======================================================================

async function handleJobMessage({ jobId, locationFromMessage, stage }) {
  try {
    switch (stage) {
      case 'demographics':
        await processJobDemographics(jobId, locationFromMessage);
      	break;

      case '7_organicSearch':
      case 'organicSearch':
        await processOrganicSearchStage(jobId);
        break;

      default:
        console.warn(
          `⚠️ Unknown stage "${stage}" for job ${jobId}. Skipping processing.`
        );
    }
  } catch (err) {
    console.error(
      `❌ Error in handleJobMessage for job ${jobId}, stage="${stage}":`,
      err
    );
  }
}

// ======================================================================
//                ORGANIC SEARCH STAGE → TRIGGER n8n
// ======================================================================

async function processOrganicSearchStage(jobId) {
  console.log(`▶️ [ORG] Starting organic search processing for job ${jobId}`);

  const job = await loadJob(jobId);
  if (!job) {
    console.warn(
      `⚠️ [ORG] Job ${jobId} not found in ${DATASET_ID}.${JOBS_TABLE_ID}.`
    );
    return;
  }

  const location = job.location || null;

  let servicesArray = [];
  if (job.services) {
    try {
      servicesArray = JSON.parse(job.services);
      if (!Array.isArray(servicesArray)) {
        servicesArray = [servicesArray];
      }
    } catch (err) {
      console.warn(
        `⚠️ [ORG] Could not parse services JSON for job ${jobId}:`,
        err
      );
      servicesArray = [];
    }
  }

  const body = {
    jobId,
    location,
    services: servicesArray,
  };

  console.log(
    `ℹ️ [ORG] Sending payload to n8n webhook: ${ORGANIC_WEBHOOK_URL} →`,
    JSON.stringify(body)
  );

  try {
    const response = await fetch(ORGANIC_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    const text = await response.text();

    if (!response.ok) {
      console.error(
        `❌ [ORG] n8n webhook responded with status ${response.status}:`,
        text
      );
    } else {
      console.log(
        `✅ [ORG] n8n webhook call succeeded for job ${jobId}. Response:`,
        text
      );
    }
  } catch (err) {
    console.error(`❌ [ORG] Error calling n8n webhook for job ${jobId}:`, err);
  }

  // The actual write to 7_organicSearch_Jobs happens when n8n calls /organic-result
}

// ======================================================================
//       ORGANIC SEARCH CALLBACK: WRITE 7_organicSearch_Jobs + STATUS
// ======================================================================

async function processOrganicResultCallback(result) {
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
  } = result;

  if (!jobId) {
    console.warn('⚠️ [/organic-result] Missing jobId in payload. Skipping row.');
    return;
  }

  console.log(
    `▶️ [ORG-CB] Processing organic callback for job ${jobId} (location=${location}, services=${services})`
  );

  // Load job to get createdAt (so we can store a consistent date)
  const job = await loadJob(jobId);
  if (!job) {
    console.warn(
      `⚠️ [ORG-CB] Job ${jobId} not found when writing organic results.`
    );
    return;
  }

  const jobDateIso = getSafeJobDateIso(job.createdAt);

  // Upsert the organic results row
  await upsertOrganicRow({
    jobId,
    dateIso: jobDateIso,
    status: 'completed', // you can adjust later if you add error cases
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
  });

  // Update main job segment status → 7_organicSearch_Status
  await markSegmentStatus(jobId, 'organicSearchStatus', 'completed');

  // Check if all required segments are now completed
  await maybeMarkJobCompleted(jobId);
}

async function upsertOrganicRow(data) {
  const {
    jobId,
    dateIso,
    status,
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
  } = data;

  console.log(
    `ℹ️ [ORG-CB] Upserting organic results for job ${jobId} into ${DATASET_ID}.${ORGANIC_JOBS_TABLE_ID}`
  );

  const mergeQuery = `
    MERGE \`${PROJECT_ID}.${DATASET_ID}.${ORGANIC_JOBS_TABLE_ID}\` T
    USING (
      SELECT
        @jobId       AS jobId,
        TIMESTAMP(@dateIso) AS date,
        @status      AS status,
        @rank1Name   AS rank1Name,
        @rank1Url    AS rank1Url,
        @rank2Name   AS rank2Name,
        @rank2Url    AS rank2Url,
        @rank3Name   AS rank3Name,
        @rank3Url    AS rank3Url,
        @rank4Name   AS rank4Name,
        @rank4Url    AS rank4Url,
        @rank5Name   AS rank5Name,
        @rank5Url    AS rank5Url,
        @rank6Name   AS rank6Name,
        @rank6Url    AS rank6Url,
        @rank7Name   AS rank7Name,
        @rank7Url    AS rank7Url,
        @rank8Name   AS rank8Name,
        @rank8Url    AS rank8Url,
        @rank9Name   AS rank9Name,
        @rank9Url    AS rank9Url,
        @rank10Name  AS rank10Name,
        @rank10Url   AS rank10Url
    ) S
    ON T.jobId = S.jobId
    WHEN MATCHED THEN
      UPDATE SET
        date       = S.date,
        status     = S.status,
        rank1Name  = S.rank1Name,
        rank1Url   = S.rank1Url,
        rank2Name  = S.rank2Name,
        rank2Url   = S.rank2Url,
        rank3Name  = S.rank3Name,
        rank3Url   = S.rank3Url,
        rank4Name  = S.rank4Name,
        rank4Url   = S.rank4Url,
        rank5Name  = S.rank5Name,
        rank5Url   = S.rank5Url,
        rank6Name  = S.rank6Name,
        rank6Url   = S.rank6Url,
        rank7Name  = S.rank7Name,
        rank7Url   = S.rank7Url,
        rank8Name  = S.rank8Name,
        rank8Url   = S.rank8Url,
        rank9Name  = S.rank9Name,
        rank9Url   = S.rank9Url,
        rank10Name = S.rank10Name,
        rank10Url  = S.rank10Url
    WHEN NOT MATCHED THEN
      INSERT (
        jobId, date, status,
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
        S.jobId, S.date, S.status,
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

  try {
    const [mergeJob] = await bigquery.createQueryJob({
      query: mergeQuery,
      params: {
        jobId,
        dateIso,
        status,
        rank1Name: rank1Name || null,
        rank1Url: rank1Url || null,
        rank2Name: rank2Name || null,
        rank2Url: rank2Url || null,
        rank3Name: rank3Name || null,
        rank3Url: rank3Url || null,
        rank4Name: rank4Name || null,
        rank4Url: rank4Url || null,
        rank5Name: rank5Name || null,
        rank5Url: rank5Url || null,
        rank6Name: rank6Name || null,
        rank6Url: rank6Url || null,
        rank7Name: rank7Name || null,
        rank7Url: rank7Url || null,
        rank8Name: rank8Name || null,
        rank8Url: rank8Url || null,
        rank9Name: rank9Name || null,
        rank9Url: rank9Url || null,
        rank10Name: rank10Name || null,
        rank10Url: rank10Url || null,
      },
    });
    await mergeJob.getQueryResults();
    console.log(
      `✅ [ORG-CB] MERGE completed for job ${jobId} into ${DATASET_ID}.${ORGANIC_JOBS_TABLE_ID}`
    );
  } catch (err) {
    console.error(
      `❌ [ORG-CB] MERGE FAILED for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
  }
}

// ======================================================================
//                     MAIN DEMOGRAPHICS ENTRYPOINT
// ======================================================================

async function processJobDemographics(jobId, locationFromMessage = null) {
  console.log(`▶️ [DEMOS] Starting demographics processing for job ${jobId}`);

  const job = await loadJob(jobId);

  if (!job) {
    console.warn(
      `⚠️ [DEMOS] Job ${jobId} not found in ${DATASET_ID}.${JOBS_TABLE_ID}.`
    );
    return;
  }

  await processDemographicsStage(job, locationFromMessage);
}

// ======================================================================
//                     DEMOGRAPHICS STAGE PROCESSOR
// ======================================================================

async function processDemographicsStage(job, locationOverride = null) {
  const jobId = job.jobId;
  const paidAdsStatus = job.paidAdsStatus || null;
  const demographicsStatus = job.demographicsStatus || null;
  const organicSearchStatus = job.organicSearchStatus || null;
  const location = job.location || locationOverride || null;
  const businessName = job.businessName || null;

  const jobDateIso = getSafeJobDateIso(job.createdAt);

  console.log(
    `ℹ️ [DEMOS] Job ${jobId} location = "${location}", demographicsStatus = ${
      job.demographicsStatus
    }, paidAdsStatus = ${paidAdsStatus}, organicSearchStatus = ${organicSearchStatus}, status = ${
      job.status
    }, createdAt = ${JSON.stringify(job.createdAt) || 'NULL'}`
  );

  if (demographicsStatus === 'completed') {
    console.log(
      `ℹ️ [DEMOS] Job ${jobId} demographics already 'completed'; skipping re-processing.`
    );
    return;
  }

  if (!location) {
    console.warn(
      `⚠️ [DEMOS] Job ${jobId} has no location; cannot process demographics.`
    );

    await markSegmentStatus(jobId, 'demographicsStatus', 'failed');
    return;
  }

  console.log('➡️ [DEMOS] Step 2: Mark main job demographicsStatus = pending');
  await markSegmentStatus(jobId, 'demographicsStatus', 'pending');

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

    await overwriteJobsDemographicsRow(jobId, {
      jobId,
      businessName,
      date: jobDateIso,
      population_no: null,
      median_age: null,
      households_no: null,
      median_income_households: null,
      median_income_families: null,
      male_percentage: null,
      female_percentage: null,
      status: 'failed',
    });

    await markSegmentStatus(jobId, 'demographicsStatus', 'failed');
    return;
  }

  const demo = demoRows[0];

  console.log(
    `ℹ️ [DEMOS] Found demographics for "${location}": ` +
      JSON.stringify(demo)
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
      `status=${newDemoStatus}, date=${jobDateIso}, businessName=${businessName}`
  );

  try {
    const mergeQuery = `
      MERGE \`${PROJECT_ID}.${DATASET_ID}.${DEMO_JOBS_TABLE_ID}\` T
      USING (
        SELECT
          @jobId AS jobId,
          @population_no AS population_no,
          @median_age AS median_age,
          @households_no AS households_no,
          @median_income_households AS median_income_households,
          @median_income_families AS median_income_families,
          @male_percentage AS male_percentage,
          @female_percentage AS female_percentage,
          @status AS status,
          TIMESTAMP(@date) AS date,
          @businessName AS businessName
      ) S
      ON T.jobId = S.jobId
      WHEN MATCHED THEN
        UPDATE SET
          population_no = S.population_no,
          median_age = S.median_age,
          households_no = S.households_no,
          median_income_households = S.median_income_households,
          median_income_families = S.median_income_families,
          male_percentage = S.male_percentage,
          female_percentage = S.female_percentage,
          status = S.status,
          date = S.date,
          businessName = S.businessName
      WHEN NOT MATCHED THEN
        INSERT (jobId, date, status, businessName,
                population_no, households_no, median_age,
                median_income_households, median_income_families,
                male_percentage, female_percentage)
        VALUES (S.jobId, S.date, S.status, S.businessName,
                S.population_no, S.households_no, S.median_age,
                S.median_income_households, S.median_income_families,
                S.male_percentage, S.female_percentage)
    `;

    const [mergeJob] = await bigquery.createQueryJob({
      query: mergeQuery,
      params: {
        jobId,
        population_no: parsed.population_no,
        median_age: parsed.median_age,
        households_no: parsed.households_no,
        median_income_households: parsed.median_income_households,
        median_income_families: parsed.median_income_families,
        male_percentage: parsed.male_percentage,
        female_percentage: parsed.female_percentage,
        status: newDemoStatus,
        date: jobDateIso,
        businessName,
      },
    });
    await mergeJob.getQueryResults();

    console.log(
      `✅ [DEMOS] MERGE completed for job ${jobId} into ${DATASET_ID}.${DEMO_JOBS_TABLE_ID}`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] MERGE FAILED for job ${jobId} into 1_demographicJobs:`,
      JSON.stringify(err.errors || err, null, 2)
    );

    await markSegmentStatus(jobId, 'demographicsStatus', 'failed');
    return;
  }

  console.log('➡️ [DEMOS] Step 5: Update client_audits_jobs.demographicsStatus');
  await markSegmentStatus(jobId, 'demographicsStatus', newDemoStatus);

  console.log(
    '➡️ [DEMOS] Step 6: Optionally mark main job status = completed if all segments done'
  );

  await maybeMarkJobCompleted(jobId);
}

// ======================================================================
//                             HELPERS
// ======================================================================

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
      `⚠️ Job createdAt is NULL/undefined; using now() as date.`
    );
    return nowIso;
  }

  let raw = createdAt;
  if (typeof createdAt === 'object' && createdAt.value) {
    raw = createdAt.value;
  }

  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) {
    console.warn(
      `⚠️ Invalid createdAt value "${raw}"; using now() as date.`
    );
    return nowIso;
  }
  return d.toISOString();
}

// Load a job row by jobId
async function loadJob(jobId) {
  console.log('➡️ Load job row from client_audits_jobs');

  const [jobRows] = await bigquery.query({
    query: `
      SELECT
        jobId,
        businessName,
        location,
        services,
        \`1_demographics_Status\` AS demographicsStatus,
        \`8_paidAds_Status\` AS paidAdsStatus,
        \`7_organicSearch_Status\` AS organicSearchStatus,
        status,
        createdAt
      FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId },
  });

  console.log(`ℹ️ loadJob result rows: ${jobRows.length}`);
  if (!jobRows.length) return null;
  return jobRows[0];
}

// Mark any segment status on the main jobs table
async function markSegmentStatus(jobId, segmentField, status) {
  const fieldMap = {
    demographicsStatus: '1_demographics_Status',
    paidAdsStatus: '8_paidAds_Status',
    organicSearchStatus: '7_organicSearch_Status',
  };

  const columnName = fieldMap[segmentField];
  if (!columnName) {
    console.error(
      `❌ markSegmentStatus: segmentField "${segmentField}" is not allowed`
    );
    return;
  }

  try {
    const [job] = await bigquery.createQueryJob({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET \`${columnName}\` = @status
        WHERE jobId = @jobId
      `,
      params: { jobId, status },
    });
    await job.getQueryResults();
    console.log(
      `✅ markSegmentStatus: set ${segmentField} (column ${columnName}) = '${status}' for job ${jobId}`
    );
  } catch (err) {
    console.error(
      `❌ Error setting ${segmentField}='${status}' for job ${jobId}:`,
      err
    );
  }
}

// If all required segments are completed, mark overall job.status = 'completed'
async function maybeMarkJobCompleted(jobId) {
  try {
    const job = await loadJob(jobId);
    if (!job) {
      console.warn(`⚠️ maybeMarkJobCompleted: job ${jobId} not found.`);
      return;
    }

    const segmentStatusMap = {
      demographicsStatus: job.demographicsStatus,
      paidAdsStatus: job.paidAdsStatus,
      organicSearchStatus: job.organicSearchStatus,
    };

    const requiredSegments = [
      'demographicsStatus',
      'paidAdsStatus',
      'organicSearchStatus',
    ];

    const statuses = requiredSegments.map(
      (key) => segmentStatusMap[key] || 'queued'
    );

    const allDone = statuses.every(
      (s) => s && s !== 'queued' && s !== 'pending'
    );
    const allCompleted = statuses.every((s) => s === 'completed');

    if (!allDone || !allCompleted) {
      console.log(
        `ℹ️ maybeMarkJobCompleted: job ${jobId} not fully completed yet (required segment statuses=${statuses.join(
          ','
        )}).`
      );
      return;
    }

    const [statusJob] = await bigquery.createQueryJob({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET status = 'completed'
        WHERE jobId = @jobId
      `,
      params: { jobId },
    });
    await statusJob.getQueryResults();
    console.log(
      `ℹ️ maybeMarkJobCompleted: marked job ${jobId} status='completed'.`
    );
  } catch (err) {
    console.error(
      `❌ maybeMarkJobCompleted: error updating main job status for job ${jobId}:`,
      err
    );
  }
}

async function overwriteJobsDemographicsRow(jobId, data) {
  const rowDateIso = data.date || new Date().toISOString();
  const businessName = data.businessName || null;

  try {
    const mergeQuery = `
      MERGE \`${PROJECT_ID}.${DATASET_ID}.${DEMO_JOBS_TABLE_ID}\` T
      USING (
        SELECT
          @jobId AS jobId,
          @population_no AS population_no,
          @median_age AS median_age,
          @households_no AS households_no,
          @median_income_households AS median_income_households,
          @median_income_families AS median_income_families,
          @male_percentage AS male_percentage,
          @female_percentage AS female_percentage,
          @status AS status,
          TIMESTAMP(@date) AS date,
          @businessName AS businessName
      ) S
      ON T.jobId = S.jobId
      WHEN MATCHED THEN
        UPDATE SET
          population_no = S.population_no,
          median_age = S.median_age,
          households_no = S.households_no,
          median_income_households = S.median_income_households,
          median_income_families = S.median_income_families,
          male_percentage = S.male_percentage,
          female_percentage = S.female_percentage,
          status = S.status,
          date = S.date,
          businessName = S.businessName
      WHEN NOT MATCHED THEN
        INSERT (jobId, date, status, businessName,
                population_no, households_no, median_age,
                median_income_households, median_income_families,
                male_percentage, female_percentage)
        VALUES (S.jobId, S.date, S.status, S.businessName,
                S.population_no, S.households_no, S.median_age,
                S.median_income_households, S.median_income_families,
                S.male_percentage, S.female_percentage)
    `;

    const [mergeJob] = await bigquery.createQueryJob({
      query: mergeQuery,
      params: {
        jobId,
        population_no: data.population_no ?? null,
        median_age: data.median_age ?? null,
        households_no: data.households_no ?? null,
        median_income_households: data.median_income_households ?? null,
        median_income_families: data.median_income_families ?? null,
        male_percentage: data.male_percentage ?? null,
        female_percentage: data.female_percentage ?? null,
        status: data.status || 'failed',
        date: rowDateIso,
        businessName,
      },
    });
    await mergeJob.getQueryResults();

    console.log(
      `✅ overwriteJobsDemographicsRow: MERGE row for job ${jobId} with status=${data.status ||
        'failed'}`
    );
  } catch (err) {
    console.error(
      `❌ overwriteJobsDemographicsRow: MERGE failed for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
  }
}

// ---------- START SERVER ----------
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Worker service listening on port ${port}`);
});
