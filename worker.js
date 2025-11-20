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
const JOBS_DEMOS_TABLE_ID = 'jobs_demographics';

// n8n webhook for organic search
const ORGANIC_WEBHOOK_URL =
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

    const {
      jobId,
      location: locationFromMessage,
      stage = 'demographics', // default so old messages still work
    } = payload;

    console.log(
      `✅ Worker received job ${jobId} (stage=${stage}, location=${locationFromMessage || 'N/A'})`
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

    // Always ACK so Pub/Sub does not retry this message
    res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    // ACK even on error to avoid infinite retry loop
    res.status(204).send();
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
//                ORGANIC SEARCH STAGE → CALL n8n WEBHOOK
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
      headers: {
        'Content-Type': 'application/json',
      },
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

  // Later: when 7_organicJobs is written + callback is wired,
  // we'll mark 7_organicSearchStatus here.
}

// ======================================================================
//                     DEMOGRAPHICS STAGE PROCESSOR
// ======================================================================

async function processDemographicsStage(job, locationOverride = null) {
  const jobId = job.jobId;
  const paidAdsStatus = job.paidAdsStatus || null;
  const location = job.location || locationOverride || null;

  const jobDateIso = getSafeJobDateIso(job.createdAt);

  console.log(
    `ℹ️ [DEMOS] Job ${jobId} location = "${location}", demographicsStatus = ${
      job.demographicsStatus
    }, paidAdsStatus = ${paidAdsStatus}, organicSearchStatus = ${
      job.organicSearchStatus
    }, status = ${job.status}, createdAt = ${
      JSON.stringify(job.createdAt) || 'NULL'
    }`
  );

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
    '➡️ [DEMOS] Step 4: MERGE into jobs_demographics with demographics values'
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
      `status=${newDemoStatus}, date=${jobDateIso}`
  );

  const nowIso = new Date().toISOString();

  try {
    const mergeQuery = `
      MERGE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\` T
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
          @updatedAt AS updatedAt
      ) S
      ON T.jobId = S.jobId
      WHEN MATCHED THEN
        UPDATE SET
          location = S.location,
          population_no = S.population_no,
          median_age = S.median_age,
          households_no = S.households_no,
          median_income_households = S.median_income_households,
          median_income_families = S.median_income_families,
          male_percentage = S.male_percentage,
          female_percentage = S.female_percentage,
          status = S.status,
          date = S.date,
          createdAt = S.createdAt,
          updatedAt = S.updatedAt
      WHEN NOT MATCHED THEN
        INSERT (jobId, location, population_no, median_age, households_no,
                median_income_households, median_income_families,
                male_percentage, female_percentage, status, date, createdAt, updatedAt)
        VALUES (S.jobId, S.location, S.population_no, S.median_age, S.households_no,
                S.median_income_households, S.median_income_families,
                S.male_percentage, S.female_percentage, S.status, S.date, S.createdAt, S.updatedAt)
    `;

    const [mergeJob] = await bigquery.createQueryJob({
      query: mergeQuery,
      params: {
        jobId,
        location,
        population_no: parsed.population_no,
        median_age: parsed.median_age,
        households_no: parsed.households_no,
        median_income_households: parsed.median_income_households,
        median_income_families: parsed.median_income_families,
        male_percentage: parsed.male_percentage,
        female_percentage: parsed.female_percentage,
        status: newDemoStatus,
        date: jobDateIso,
        createdAt: nowIso,
        updatedAt: nowIso,
      },
    });
    await mergeJob.getQueryResults();

    console.log(
      `✅ [DEMOS] MERGE completed for job ${jobId} into ${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] MERGE FAILED for job ${jobId} into jobs_demographics:`,
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
      `⚠️ [DEMOS] Job createdAt is NULL/undefined; using now() as date for jobs_demographics.`
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
      `⚠️ [DEMOS] Invalid createdAt value "${raw}"; using now() as date for jobs_demographics.`
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
        location,
        services,
        demographicsStatus,
        paidAdsStatus,
        \`7_organicSearchStatus\` AS organicSearchStatus,
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
    demographicsStatus: 'demographicsStatus',
    paidAdsStatus: 'paidAdsStatus',
    organicSearchStatus: '7_organicSearchStatus',
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

// If all processed segments are completed, mark overall job.status = 'completed'
async function maybeMarkJobCompleted(jobId) {
  try {
    const job = await loadJob(jobId);
    if (!job) {
      console.warn(
        `⚠️ maybeMarkJobCompleted: job ${jobId} not found.`
      );
      return;
    }

    const rawSegments = {
      demographicsStatus: job.demographicsStatus,
      paidAdsStatus: job.paidAdsStatus,
      organicSearchStatus: job.organicSearchStatus,
    };

    const segments = Object.values(rawSegments).filter(
      (s) => s && s !== 'queued'
    );

    if (!segments.length) {
      console.log(
        `ℹ️ maybeMarkJobCompleted: no processed segment statuses yet for job ${jobId}.`
      );
      return;
    }

    const allCompleted = segments.every((s) => s === 'completed');
    if (!allCompleted) {
      console.log(
        `ℹ️ maybeMarkJobCompleted: not all processed segments completed for job ${jobId} (segments=${segments.join(
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
  const nowIso = new Date().toISOString();
  const row = {
    jobId: data.jobId,
    location: data.location || null,
    population_no: data.population_no ?? null,
    median_age: data.median_age ?? null,
    households_no: data.households_no ?? null,
    median_income_households: data.median_income_households ?? null,
    median_income_families: data.median_income_families ?? null,
    male_percentage: data.male_percentage ?? null,
    female_percentage: data.female_percentage ?? null,
    status: data.status || 'failed',
    date: data.date || nowIso,
    createdAt: nowIso,
    updatedAt: nowIso,
  };

  try {
    const mergeQuery = `
      MERGE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\` T
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
          @updatedAt AS updatedAt
      ) S
      ON T.jobId = S.jobId
      WHEN MATCHED THEN
        UPDATE SET
          location = S.location,
          population_no = S.population_no,
          median_age = S.median_age,
          households_no = S.households_no,
          median_income_households = S.median_income_households,
          median_income_families = S.median_income_families,
          male_percentage = S.male_percentage,
          female_percentage = S.female_percentage,
          status = S.status,
          date = S.date,
          createdAt = S.createdAt,
          updatedAt = S.updatedAt
      WHEN NOT MATCHED THEN
        INSERT (jobId, location, population_no, median_age, households_no,
                median_income_households, median_income_families,
                male_percentage, female_percentage, status, date, createdAt, updatedAt)
        VALUES (S.jobId, S.location, S.population_no, S.median_age, S.households_no,
                S.median_income_households, S.median_income_families,
                S.male_percentage, S.female_percentage, S.status, S.date, S.createdAt, S.updatedAt)
    `;

    const [mergeJob] = await bigquery.createQueryJob({
      query: mergeQuery,
      params: {
        jobId: row.jobId,
        location: row.location,
        population_no: row.population_no,
        median_age: row.median_age,
        households_no: row.households_no,
        median_income_households: row.median_income_households,
        median_income_families: row.median_income_families,
        male_percentage: row.male_percentage,
        female_percentage: row.female_percentage,
        status: row.status,
        date: row.date,
        createdAt: row.createdAt,
        updatedAt: row.updatedAt,
      },
    });
    await mergeJob.getQueryResults();

    console.log(
      `✅ overwriteJobsDemographicsRow: MERGE row for job ${jobId} with status=${row.status}`
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
