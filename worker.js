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

    const { jobId, location: locationFromMessage } = payload;

    console.log(
      `✅ Worker received job ${jobId} (location=${locationFromMessage || 'N/A'})`
    );

    if (!jobId) {
      console.error('❌ [DEMOS] Missing jobId in message payload. Skipping.');
      return res.status(204).send();
    }

    await processJobDemographics(jobId, locationFromMessage || null);

    // Always ACK so Pub/Sub does not retry this message
    res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    // ACK even on error to avoid infinite retry loop
    res.status(204).send();
  }
});

// ---------- DEMOGRAPHICS PROCESSOR ----------

async function processJobDemographics(jobId, locationFromMessage = null) {
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
  const currentDemoStatus = job.demographicsStatus || 'queued';
  const paidAdsStatus = job.paidAdsStatus || null;
  const location = job.location || locationFromMessage || null;

  // createdAt is a BigQuery TIMESTAMP object; normalise to ISO string
  const jobTimestamp =
    job.createdAt && job.createdAt.value
      ? job.createdAt.value
      : job.createdAt || null;

  console.log(
    `ℹ️ [DEMOS] Job ${jobId} location = "${location}", demographicsStatus = ${currentDemoStatus}, paidAdsStatus = ${paidAdsStatus}, status = ${job.status}, createdAt=${jobTimestamp}`
  );

  // ✅ Idempotency guard: if this job is already terminal, don't reprocess
  if (currentDemoStatus === 'completed' || currentDemoStatus === 'failed') {
    console.log(
      `ℹ️ [DEMOS] Job ${jobId} already has demographicsStatus="${currentDemoStatus}". Skipping re-processing.`
    );
    return;
  }

  if (!location) {
    console.warn(
      `⚠️ [DEMOS] Job ${jobId} has no location; cannot process demographics.`
    );

    // Mark main job as failed for demographics
    await markJobDemographicsStatus(jobId, 'failed');
    return;
  }

  // ---- Step 2: Mark main job demographicsStatus = pending ----
  console.log('➡️ [DEMOS] Step 2: Mark main job demographicsStatus = pending');
  await markJobDemographicsStatus(jobId, 'pending');

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

    // Insert a "failed" row with nulls into jobs_demographics
    await overwriteJobsDemographicsRow(jobId, {
      jobId,
      location,
      households_no: null,
      population_no: null,
      median_age: null,
      median_income_households: null,
      median_income_families: null,
      male_percentage: null,
      female_percentage: null,
      status: 'failed',
      timestamp: jobTimestamp,
    });

    await markJobDemographicsStatus(jobId, 'failed');
    return;
  }

  const demo = demoRows[0];

  console.log(
    `ℹ️ [DEMOS] Found demographics for "${location}": ` +
      JSON.stringify(demo)
  );

  // ---- Step 4: Compute metrics + status; overwrite jobs_demographics row via streaming insert ----
  console.log(
    '➡️ [DEMOS] Step 4: Overwrite jobs_demographics with demographics values via streaming insert'
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
    `ℹ️ [DEMOS] Step 4 computed metrics for job ${jobId}:`,
    JSON.stringify(parsed),
    `=> newDemoStatus="${newDemoStatus}"`
  );

  // Delete any existing row for this jobId (safe DML – affects only committed rows)
  try {
    const [deleteJob] = await bigquery.createQueryJob({
      query: `
        DELETE FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        WHERE jobId = @jobId
      `,
      params: { jobId },
    });
    await deleteJob.getQueryResults();
    console.log(
      `ℹ️ [DEMOS] Step 4: Deleted any existing jobs_demographics row for job ${jobId} before streaming insert.`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error deleting existing jobs_demographics row for job ${jobId}:`,
      err
    );
    // continue anyway; insert will just create a fresh row
  }

  // Streaming insert full row with metrics + status + timestamp
  const nowIso = new Date().toISOString();
  const rowToInsert = {
    jobId,
    location,
    households_no: parsed.households_no,
    population_no: parsed.population_no,
    median_age: parsed.median_age,
    median_income_households: parsed.median_income_households,
    median_income_families: parsed.median_income_families,
    male_percentage: parsed.male_percentage,
    female_percentage: parsed.female_percentage,
    status: newDemoStatus,
    timestamp: jobTimestamp || null, // match job date from main table
    createdAt: nowIso,
    updatedAt: nowIso,
  };

  console.log(
    `ℹ️ [DEMOS] Step 4 rowToInsert for job ${jobId}:`,
    JSON.stringify(rowToInsert)
  );

  try {
    await bigquery
      .dataset(DATASET_ID)
      .table(JOBS_DEMOS_TABLE_ID)
      .insert([rowToInsert], { ignoreUnknownValues: true });

    console.log(
      `✅ [DEMOS] Streaming insert completed for job ${jobId} into ${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] Streaming insert FAILED for job ${jobId} into jobs_demographics:`,
      JSON.stringify(err.errors || err, null, 2)
    );

    // If insert fails, mark main job as failed
    await markJobDemographicsStatus(jobId, 'failed');
    return;
  }

  // Quick verification
  try {
    const [checkRows] = await bigquery.query({
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
      `ℹ️ [DEMOS] Step 4 check row for job ${jobId}:`,
      checkRows[0] || null
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error verifying jobs_demographics row for job ${jobId}:`,
      err
    );
  }

  // ---- Step 5: Update main job's demographicsStatus based on newDemoStatus ----
  console.log('➡️ [DEMOS] Step 5: Update client_audits_jobs.demographicsStatus');

  await markJobDemographicsStatus(jobId, newDemoStatus);

  // No separate Step 6 needed: markJobDemographicsStatus now also recomputes main status
}

// ---------- HELPERS ----------

function toNumberOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  if (Number.isNaN(n)) return null;
  return n;
}

/**
 * Updates demographicsStatus for a job and then recomputes the main `status`
 * for that job based on ALL known section statuses (demographics, paidAds, etc.).
 *
 * Rules:
 * - If ANY sub-status = 'failed'           => main status = 'failed'
 * - Else if ANY sub-status in ('pending','queued') => main status = 'pending'
 * - Else if ANY sub-status = 'partial'     => main status = 'partial'
 * - Else if ALL sub-statuses = 'completed' => main status = 'completed'
 * - Fallback                               => 'pending'
 */
async function markJobDemographicsStatus(jobId, demoStatus) {
  try {
    // 1) Update demographicsStatus itself
    const [job] = await bigquery.createQueryJob({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET demographicsStatus = @demoStatus
        WHERE jobId = @jobId
      `,
      params: { jobId, demoStatus },
    });
    await job.getQueryResults();
    console.log(
      `✅ [DEMOS] Marked demographicsStatus = '${demoStatus}' for job ${jobId}`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error marking demographicsStatus='${demoStatus}' for job ${jobId}:`,
      err
    );
    // still try to recompute main status with whatever is currently stored
  }

  // 2) Recompute main status based on all sub-statuses
  try {
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
      console.warn(
        `⚠️ [DEMOS] markJobDemographicsStatus: job ${jobId} not found when recomputing main status.`
      );
      return;
    }

    const row = rows[0];
    const statuses = [
      row.demographicsStatus,
      row.paidAdsStatus,
    ].filter(Boolean);

    let mainStatus = 'pending';

    if (statuses.some((s) => s === 'failed')) {
      mainStatus = 'failed';
    } else if (statuses.some((s) => s === 'pending' || s === 'queued')) {
      mainStatus = 'pending';
    } else if (statuses.some((s) => s === 'partial')) {
      mainStatus = 'partial';
    } else if (statuses.length && statuses.every((s) => s === 'completed')) {
      mainStatus = 'completed';
    } else {
      mainStatus = 'pending';
    }

    const [updateMain] = await bigquery.createQueryJob({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET status = @mainStatus
        WHERE jobId = @jobId
      `,
      params: { jobId, mainStatus },
    });
    await updateMain.getQueryResults();

    console.log(
      `ℹ️ [DEMOS] Recomputed main status for job ${jobId}: ${mainStatus} (from sub-statuses=${JSON.stringify(
        statuses
      )})`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error recomputing main status for job ${jobId}:`,
      err
    );
  }
}

/**
 * Used in the "no data" path to ensure there is a row in jobs_demographics
 * even when we have no metrics. It deletes existing row(s) and then streams a new one.
 */
async function overwriteJobsDemographicsRow(jobId, data) {
  const nowIso = new Date().toISOString();

  const row = {
    jobId: data.jobId,
    location: data.location || null,
    households_no: data.households_no ?? null,
    population_no: data.population_no ?? null,
    median_age: data.median_age ?? null,
    median_income_households: data.median_income_households ?? null,
    median_income_families: data.median_income_families ?? null,
    male_percentage: data.male_percentage ?? null,
    female_percentage: data.female_percentage ?? null,
    status: data.status || 'failed',
    timestamp:
      data.timestamp && data.timestamp.value
        ? data.timestamp.value
        : data.timestamp || null,
    createdAt: nowIso,
    updatedAt: nowIso,
  };

  try {
    const [deleteJob] = await bigquery.createQueryJob({
      query: `
        DELETE FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        WHERE jobId = @jobId
      `,
      params: { jobId },
    });
    await deleteJob.getQueryResults();
    console.log(
      `ℹ️ [DEMOS] overwriteJobsDemographicsRow: deleted existing row(s) for job ${jobId}`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] overwriteJobsDemographicsRow: error deleting existing rows for job ${jobId}:`,
      err
    );
  }

  try {
    await bigquery
      .dataset(DATASET_ID)
      .table(JOBS_DEMOS_TABLE_ID)
      .insert([row], { ignoreUnknownValues: true });
    console.log(
      `✅ [DEMOS] overwriteJobsDemographicsRow: inserted row for job ${jobId} with status=${row.status}`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] overwriteJobsDemographicsRow: streaming insert failed for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
  }
}

// ---------- START SERVER ----------
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Worker service listening on port ${port}`);
});
