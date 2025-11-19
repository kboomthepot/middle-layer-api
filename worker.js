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

const bigquery = new BigQuery({ projectId: PROJECT_ID });

const app = express();
app.use(bodyParser.json());

// ---------- Small helpers ----------
function toIsoStringOrNull(val) {
  if (!val) return null;
  if (val instanceof Date) return val.toISOString();
  if (typeof val === 'string') return val;
  // BigQuery can also return { value: '...' } for TIMESTAMP
  if (typeof val === 'object' && val.value) return String(val.value);
  return null;
}

function computeDemoStatusFromMetrics(metrics) {
  const values = [
    metrics.population_no,
    metrics.median_age,
    metrics.households_no,
    metrics.median_income_households,
    metrics.median_income_families,
    metrics.male_percentage,
    metrics.female_percentage,
  ];

  const nonNullCount = values.filter(v => v !== null && v !== undefined).length;
  if (nonNullCount === 0) return 'failed';
  if (nonNullCount < values.length) return 'partial';
  return 'completed';
}

// Recompute overall main job `status` from sub-status fields
async function recomputeMainStatus(jobId) {
  const [rows] = await bigquery.query({
    query: `
      SELECT
        demographicsStatus,
        paidAdsStatus
      FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId },
  });

  if (!rows.length) {
    console.warn(`⚠️ [DEMOS] recomputeMainStatus: Job ${jobId} not found.`);
    return;
  }

  const { demographicsStatus, paidAdsStatus } = rows[0];
  const subStatuses = [demographicsStatus, paidAdsStatus].filter(Boolean);

  let newStatus = 'pending';

  if (subStatuses.some(s => s === 'failed')) {
    newStatus = 'failed';
  } else if (subStatuses.length > 0 && subStatuses.every(s => s === 'completed')) {
    newStatus = 'completed';
  } else if (subStatuses.some(s => s === 'queued' || s === 'pending')) {
    newStatus = 'pending';
  } else {
    newStatus = 'pending';
  }

  console.log(
    `ℹ️ [DEMOS] Recomputed main status for job ${jobId}: ${newStatus} ` +
    `(from sub-statuses=${JSON.stringify(subStatuses)})`
  );

  await bigquery.query({
    query: `
      UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
      SET status = @status, updatedAt = CURRENT_TIMESTAMP()
      WHERE jobId = @jobId
    `,
    params: { jobId, status: newStatus },
  });
}

// ---------- HEALTH CHECK ----------
app.get('/', (req, res) => {
  res.send('Worker service listening on port 8080');
});

// ---------- Pub/Sub push endpoint ----------
app.post('/', async (req, res) => {
  try {
    const envelope = req.body;
    if (!envelope || !envelope.message || !envelope.message.data) {
      console.error('❌ Invalid Pub/Sub message format:', JSON.stringify(envelope));
      return res.status(204).send(); // ACK anyway
    }

    const payload = JSON.parse(
      Buffer.from(envelope.message.data, 'base64').toString()
    );

    console.log('📩 Received job message:', payload);

    const { jobId } = payload;

    console.log(
      `✅ Worker received job ${jobId} (location=${payload.location || 'N/A'})`
    );

    if (!jobId) {
      console.error('❌ [DEMOS] Missing jobId in message payload. Skipping.');
      return res.status(204).send();
    }

    await processJobDemographics(jobId);

    // Always ACK so Pub/Sub does not retry this message forever
    res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    // Still ACK to avoid infinite retry
    res.status(204).send();
  }
});

// -------- DEMOGRAPHICS PROCESSOR --------

async function processJobDemographics(jobId) {
  console.log(`▶️ [DEMOS] Starting demographics processing for job ${jobId}`);

  // ---- Step 1: Load job row from client_audits_jobs ----
  console.log('➡️ [DEMOS] Step 1: Load job row from client_audits_jobs');

  const [jobRows] = await bigquery.query({
    query: `
      SELECT
        jobId,
        location,
        demographicsStatus,
        paidAdsStatus,
        status,
        createdAt
      FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId },
  });

  console.log(`ℹ️ [DEMOS] Step 1 result rows: ${jobRows.length}`);

  if (!jobRows.length) {
    console.warn(
      `⚠️ [DEMOS] Job ${jobId} not found in ${DATASET_ID}.${JOBS_TABLE_ID}.`
    );
    return;
  }

  const job = jobRows[0];
  const location = job.location || null;
  const currentDemoStatus = job.demographicsStatus || 'queued';
  const jobCreatedAtIso = toIsoStringOrNull(job.createdAt);

  console.log(
    `ℹ️ [DEMOS] Job ${jobId} location = "${location}", ` +
    `demographicsStatus = ${currentDemoStatus}, paidAdsStatus = ${job.paidAdsStatus}, ` +
    `status = ${job.status}, createdAt=${jobCreatedAtIso}`
  );

  if (!location) {
    console.warn(
      `⚠️ [DEMOS] Job ${jobId} has no location; cannot process demographics.`
    );
    return;
  }

  // ---- Idempotency guard: if demographicsStatus already completed/failed AND jobs_demographics has data, skip ----
  if (currentDemoStatus === 'completed' || currentDemoStatus === 'failed') {
    console.log(
      `ℹ️ [DEMOS] Job ${jobId} already has demographicsStatus="${currentDemoStatus}". ` +
      `Checking jobs_demographics for populated row to decide whether to skip.`
    );

    const [demoCheckRows] = await bigquery.query({
      query: `
        SELECT
          jobId,
          population_no,
          median_age,
          households_no,
          median_income_households,
          median_income_families,
          male_percentage,
          female_percentage,
          status
        FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        WHERE jobId = @jobId
        LIMIT 1
      `,
      params: { jobId },
    });

    if (demoCheckRows.length) {
      const r = demoCheckRows[0];
      const metrics = {
        population_no: r.population_no ?? null,
        median_age: r.median_age ?? null,
        households_no: r.households_no ?? null,
        median_income_households: r.median_income_households ?? null,
        median_income_families: r.median_income_families ?? null,
        male_percentage: r.male_percentage ?? null,
        female_percentage: r.female_percentage ?? null,
      };
      const derived = computeDemoStatusFromMetrics(metrics);

      console.log(
        `ℹ️ [DEMOS] Idempotency check for job ${jobId}: ` +
        `existing jobs_demographics.status=${r.status}, derivedFromMetrics=${derived}`
      );

      const hasAnyMetric =
        Object.values(metrics).filter(v => v !== null && v !== undefined).length > 0;

      if (hasAnyMetric && (r.status === 'completed' || r.status === 'partial' || derived === 'completed')) {
        console.log(
          `ℹ️ [DEMOS] Job ${jobId} already has populated demographics row; skipping re-processing.`
        );
        await recomputeMainStatus(jobId); // keep main status in sync if other segments changed
        return;
      }
    }

    console.log(
      `ℹ️ [DEMOS] Idempotency guard: demographicsStatus=${currentDemoStatus} but ` +
      `jobs_demographics missing or empty. Proceeding with re-processing.`
    );
  }

  // ---- Step 2: Mark demographicsStatus = pending on main job ----
  console.log('➡️ [DEMOS] Step 2: Mark main job demographicsStatus = pending');

  await bigquery.query({
    query: `
      UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
      SET demographicsStatus = 'pending', updatedAt = CURRENT_TIMESTAMP()
      WHERE jobId = @jobId
    `,
    params: { jobId },
  });

  console.log(`✅ [DEMOS] Marked demographicsStatus = 'pending' for job ${jobId}`);

  await recomputeMainStatus(jobId);

  // ---- Step 3: Load demographics source row from Client_audits_data.1_demographics ----
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
      `⚠️ [DEMOS] No demographics found in ${DEMOS_DATASET_ID}.${DEMOS_SOURCE_TABLE_ID} ` +
      `for location "${location}". Marking as failed.`
    );

    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET demographicsStatus = 'failed', updatedAt = CURRENT_TIMESTAMP()
        WHERE jobId = @jobId
      `,
      params: { jobId },
    });

    // Optionally, insert a "failed" row or leave jobs_demographics empty.
    await recomputeMainStatus(jobId);
    return;
  }

  const demo = demoRows[0];

  console.log(
    `ℹ️ [DEMOS] Found demographics for "${location}": ` +
    JSON.stringify(demo)
  );

  // ---- Step 4: Overwrite jobs_demographics with demographics values via streaming insert ----
  console.log(
    '➡️ [DEMOS] Step 4: Overwrite jobs_demographics with demographics values via streaming insert'
  );

  const metrics = {
    population_no: demo.population_no != null && demo.population_no !== '' ? Number(demo.population_no) : null,
    median_age: demo.median_age != null && demo.median_age !== '' ? Number(demo.median_age) : null,
    households_no: demo.households_no != null && demo.households_no !== '' ? Number(demo.households_no) : null,
    median_income_households:
      demo.median_income_households != null && demo.median_income_households !== ''
        ? Number(demo.median_income_households)
        : null,
    median_income_families:
      demo.median_income_families != null && demo.median_income_families !== ''
        ? Number(demo.median_income_families)
        : null,
    male_percentage: demo.male_percentage != null && demo.male_percentage !== '' ? Number(demo.male_percentage) : null,
    female_percentage: demo.female_percentage != null && demo.female_percentage !== '' ? Number(demo.female_percentage) : null,
  };

  const newDemoStatus = computeDemoStatusFromMetrics(metrics);

  console.log(
    `ℹ️ [DEMOS] Step 4 computed metrics for job ${jobId}: ` +
    `${JSON.stringify(metrics)} => newDemoStatus="${newDemoStatus}"`
  );

  // Clean remove any existing row for this jobId
  await bigquery.query({
    query: `
      DELETE FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
      WHERE jobId = @jobId
    `,
    params: { jobId },
  });

  console.log(
    `ℹ️ [DEMOS] Step 4: Deleted any existing jobs_demographics row for job ${jobId} before streaming insert.`
  );

  const nowIso = new Date().toISOString();
  const timestampIso = jobCreatedAtIso || nowIso;

  const rowToInsert = {
    jobId,
    location,
    households_no: metrics.households_no,
    population_no: metrics.population_no,
    median_age: metrics.median_age,
    median_income_households: metrics.median_income_households,
    median_income_families: metrics.median_income_families,
    male_percentage: metrics.male_percentage,
    female_percentage: metrics.female_percentage,
    status: newDemoStatus,
    timestamp: timestampIso,      // <-- must match TIMESTAMP column name
    createdAt: nowIso,
    updatedAt: nowIso,
  };

  console.log(
    `ℹ️ [DEMOS] Step 4 rowToInsert for job ${jobId}: ` +
    JSON.stringify(rowToInsert)
  );

  // Streaming insert
  await bigquery
    .dataset(DATASET_ID)
    .table(JOBS_DEMOS_TABLE_ID)
    .insert([rowToInsert]);

  console.log(
    `✅ [DEMOS] Streaming insert completed for job ${jobId} into ${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}`
  );

  // Optional verification read-back
  const [verifyRows] = await bigquery.query({
    query: `
      SELECT
        jobId,
        location,
        households_no,
        population_no,
        median_age,
        median_income_households,
        median_income_families,
        male_percentage,
        female_percentage,
        status,
        timestamp
      FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId },
  });

  console.log(
    `ℹ️ [DEMOS] Step 4 check row for job ${jobId}: ` +
    JSON.stringify(verifyRows[0] || null)
  );

  // ---- Step 5: Update job's demographicsStatus in main table ----
  console.log('➡️ [DEMOS] Step 5: Update client_audits_jobs.demographicsStatus');

  await bigquery.query({
    query: `
      UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
      SET demographicsStatus = @demoStatus, updatedAt = CURRENT_TIMESTAMP()
      WHERE jobId = @jobId
    `,
    params: { jobId, demoStatus: newDemoStatus },
  });

  console.log(
    `✅ [DEMOS] Marked demographicsStatus = '${newDemoStatus}' for job ${jobId}`
  );

  // Recompute main status using updated sub-status
  await recomputeMainStatus(jobId);
}

// ---------- START SERVER ----------
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Worker service listening on port ${port}`);
});
