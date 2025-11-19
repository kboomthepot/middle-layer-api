// index.js
const express = require('express');
const bodyParser = require('body-parser');
const { BigQuery } = require('@google-cloud/bigquery');

const bigquery = new BigQuery();

const PROJECT_ID = 'ghs-construction-1734441714520';
const DATASET_ID = 'Client_audits';
const JOBS_TABLE = `${PROJECT_ID}.Client_audits.client_audits_jobs`;
const JOBS_DEMOS_TABLE = `${PROJECT_ID}.Client_audits.jobs_demographics`;
const DEMOS_SOURCE_TABLE = `${PROJECT_ID}.Client_audits_data.1_demographics`;

const app = express();
app.use(bodyParser.json());

/**
 * Small helper: recompute main `status` on client_audits_jobs
 * based on demographicsStatus + paidAdsStatus (and any future fields).
 *
 * Rules:
 * - if any = 'failed'         => status = 'failed'
 * - else if any in (pending, queued, partial) => status = 'pending'
 * - else if all = 'completed' => status = 'completed'
 * - else                      => status = 'queued'
 */
async function recomputeJobMainStatus(jobId) {
  const [rows] = await bigquery.query({
    query: `
      SELECT demographicsStatus, paidAdsStatus
      FROM \`${JOBS_TABLE}\`
      WHERE jobId = @jobId
    `,
    params: { jobId },
  });

  if (!rows.length) {
    console.log(`⚠️ [DEMOS] recomputeJobMainStatus: job ${jobId} not found.`);
    return;
  }

  const { demographicsStatus, paidAdsStatus } = rows[0];
  const subStatuses = [demographicsStatus, paidAdsStatus].filter(Boolean);

  if (!subStatuses.length) {
    console.log(
      `ℹ️ [DEMOS] recomputeJobMainStatus: no sub-statuses for job ${jobId}, skipping.`
    );
    return;
  }

  let newStatus = 'queued';
  if (subStatuses.includes('failed')) {
    newStatus = 'failed';
  } else if (
    subStatuses.some((s) =>
      ['pending', 'queued', 'partial'].includes(String(s).toLowerCase())
    )
  ) {
    newStatus = 'pending';
  } else if (subStatuses.every((s) => s === 'completed')) {
    newStatus = 'completed';
  }

  console.log(
    `ℹ️ [DEMOS] Recomputed main status for job ${jobId}: ${newStatus} (from sub-statuses=${JSON.stringify(
      subStatuses
    )})`
  );

  await bigquery.query({
    query: `
      UPDATE \`${JOBS_TABLE}\`
      SET status = @status
      WHERE jobId = @jobId
    `,
    params: { jobId, status: newStatus },
  });
}

/**
 * Main demographics worker
 */
async function processJobDemographics(jobId, locationFromMessage, createdAtFromMessage) {
  console.log('📩 Received job message:', {
    jobId,
    location: locationFromMessage,
    createdAt: createdAtFromMessage,
  });
  console.log(
    `✅ Worker received job ${jobId} (location=${locationFromMessage})`
  );
  console.log(
    `▶️ [DEMOS] Starting demographics processing for job ${jobId}`
  );

  // ---- STEP 1: Load job row from client_audits_jobs ----
  console.log('➡️ [DEMOS] Step 1: Load job row from client_audits_jobs');
  const [jobRows] = await bigquery.query({
    query: `
      SELECT jobId,
             location,
             demographicsStatus,
             paidAdsStatus,
             status,
             createdAt
      FROM \`${JOBS_TABLE}\`
      WHERE jobId = @jobId
    `,
    params: { jobId },
  });

  console.log(
    `ℹ️ [DEMOS] Step 1 result rows: ${jobRows.length}`
  );

  if (!jobRows.length) {
    console.log(
      `⚠️ [DEMOS] No client_audits_jobs row found for jobId=${jobId}, aborting.`
    );
    return;
  }

  const jobRow = jobRows[0];
  const jobLocation = jobRow.location || locationFromMessage;
  const demographicsStatus = jobRow.demographicsStatus;
  const paidAdsStatus = jobRow.paidAdsStatus;
  const mainStatus = jobRow.status;
  const jobCreatedAt =
    jobRow.createdAt || createdAtFromMessage || new Date().toISOString();

  console.log(
    `ℹ️ [DEMOS] Job ${jobId} location = "${jobLocation}", demographicsStatus = ${demographicsStatus}, paidAdsStatus = ${paidAdsStatus}, status = ${mainStatus}, createdAt=${jobCreatedAt}`
  );

  // Idempotency guard: if this job already has demographics processed (completed/failed),
  // just recompute the main status once and exit.
  if (demographicsStatus === 'completed' || demographicsStatus === 'failed') {
    console.log(
      `ℹ️ [DEMOS] Job ${jobId} already has demographicsStatus="${demographicsStatus}". Skipping re-processing.`
    );
    await recomputeJobMainStatus(jobId);
    return;
  }

  // ---- STEP 2: Mark demographicsStatus = pending on main job ----
  console.log(
    '➡️ [DEMOS] Step 2: Mark main job demographicsStatus = pending'
  );
  await bigquery.query({
    query: `
      UPDATE \`${JOBS_TABLE}\`
      SET demographicsStatus = 'pending'
      WHERE jobId = @jobId
    `,
    params: { jobId },
  });
  console.log(
    `✅ [DEMOS] Marked demographicsStatus = 'pending' for job ${jobId}`
  );

  // ---- STEP 3: Load demographics from Client_audits_data.1_demographics ----
  console.log(
    '➡️ [DEMOS] Step 3: Load demographics from Client_audits_data.1_demographics'
  );
  const [demoRows] = await bigquery.query({
    query: `
      SELECT
        population_no,
        median_age,
        median_income_households,
        median_income_families,
        male_percentage,
        female_percentage
      FROM \`${DEMOS_SOURCE_TABLE}\`
      WHERE location = @location
      LIMIT 1
    `,
    params: { location: jobLocation },
  });

  console.log(
    `ℹ️ [DEMOS] Step 3 result rows (demographics): ${demoRows.length}`
  );

  if (!demoRows.length) {
    console.log(
      `⚠️ [DEMOS] No demographics found in ${DEMOS_SOURCE_TABLE} for location="${jobLocation}". Marking failed.`
    );

    // Set demographicsStatus = failed on main job
    await bigquery.query({
      query: `
        UPDATE \`${JOBS_TABLE}\`
        SET demographicsStatus = 'failed'
        WHERE jobId = @jobId
      `,
      params: { jobId },
    });

    // We DO NOT write a row to jobs_demographics if we couldn't find demographics.
    await recomputeJobMainStatus(jobId);
    return;
  }

  const demo = demoRows[0];
  console.log(
    `ℹ️ [DEMOS] Found demographics for "${jobLocation}": ${JSON.stringify(
      demo
    )}`
  );

  // ---- STEP 4: Delete any existing jobs_demographics row and streaming insert new one ----
  console.log(
    '➡️ [DEMOS] Step 4: Overwrite jobs_demographics with demographics values via streaming insert'
  );

  // Parse numbers safely (handle strings like "70992")
  const population_no =
    demo.population_no != null ? Number(demo.population_no) : null;
  const median_age =
    demo.median_age != null ? Number(demo.median_age) : null;
  const median_income_households =
    demo.median_income_households != null
      ? Number(demo.median_income_households)
      : null;
  const median_income_families =
    demo.median_income_families != null
      ? Number(demo.median_income_families)
      : null;
  const male_percentage =
    demo.male_percentage != null ? Number(demo.male_percentage) : null;
  const female_percentage =
    demo.female_percentage != null ? Number(demo.female_percentage) : null;

  const metrics = {
    population_no,
    median_age,
    median_income_households,
    median_income_families,
    male_percentage,
    female_percentage,
  };

  // Determine new demographics status based on metrics
  const metricValues = Object.values(metrics);
  const nonNullCount = metricValues.filter((v) => v !== null).length;
  let newDemoStatus = 'failed';
  if (nonNullCount === 0) {
    newDemoStatus = 'failed';
  } else if (nonNullCount < metricValues.length) {
    newDemoStatus = 'partial';
  } else {
    newDemoStatus = 'completed';
  }

  console.log(
    `ℹ️ [DEMOS] Step 4 computed metrics for job ${jobId}: ${JSON.stringify(
      metrics
    )} => newDemoStatus="${newDemoStatus}"`
  );

  // Delete existing row (if any) before streaming insert
  await bigquery.query({
    query: `
      DELETE FROM \`${JOBS_DEMOS_TABLE}\`
      WHERE jobId = @jobId
    `,
    params: { jobId },
  });
  console.log(
    `ℹ️ [DEMOS] Step 4: Deleted any existing jobs_demographics row for job ${jobId} before streaming insert.`
  );

  // Build row for streaming insert
  const nowIso = new Date().toISOString();

  const rowToInsert = {
    jobId,
    location: jobLocation,
    population_no,
    median_age,
    median_income_households,
    median_income_families,
    male_percentage,
    female_percentage,
    status: newDemoStatus,
    // IMPORTANT: "date" field = createdAt from client_audits_jobs
    date: jobCreatedAt,
    createdAt: nowIso,
    updatedAt: nowIso,
  };

  console.log(
    `ℹ️ [DEMOS] Step 4 rowToInsert for job ${jobId}: ${JSON.stringify(
      rowToInsert
    )}`
  );

  const dataset = bigquery.dataset(DATASET_ID);
  const table = dataset.table('jobs_demographics');

  await table.insert(rowToInsert);
  console.log(
    `✅ [DEMOS] Streaming insert completed for job ${jobId} into Client_audits.jobs_demographics`
  );

  // ---- STEP 5: Update client_audits_jobs.demographicsStatus with newDemoStatus ----
  console.log(
    '➡️ [DEMOS] Step 5: Update client_audits_jobs.demographicsStatus'
  );
  await bigquery.query({
    query: `
      UPDATE \`${JOBS_TABLE}\`
      SET demographicsStatus = @status
      WHERE jobId = @jobId
    `,
    params: {
      jobId,
      status: newDemoStatus,
    },
  });
  console.log(
    `✅ [DEMOS] Marked demographicsStatus = '${newDemoStatus}' for job ${jobId}`
  );

  // ---- STEP 6: Recompute main status once ----
  await recomputeJobMainStatus(jobId);
}

// HTTP endpoint for Cloud Run / n8n / PubSub push
app.post('/demographics-job', async (req, res) => {
  try {
    // Support both raw body and Pub/Sub envelope
    let payload = req.body;
    if (payload && payload.message && payload.message.data) {
      const dataStr = Buffer.from(payload.message.data, 'base64').toString();
      payload = JSON.parse(dataStr);
    }

    const { jobId, location, createdAt } = payload || {};

    if (!jobId) {
      console.error('❌ [DEMOS] Missing jobId in request payload', payload);
      res.status(400).json({ error: 'Missing jobId' });
      return;
    }

    await processJobDemographics(jobId, location, createdAt);
    res.status(200).json({ ok: true });
  } catch (err) {
    console.error('❌ [DEMOS] Error in /demographics-job handler:', err);
    res.status(500).json({ error: err.message || 'Unknown error' });
  }
});

// Root endpoint (optional) – can also accept the same payload
app.post('/', async (req, res) => {
  return app._router.handle(req, res, () => {});
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Worker service listening on port ${PORT}`);
});
