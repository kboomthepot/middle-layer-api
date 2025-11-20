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
  const paidAdsStatus = job.paidAdsStatus || null;
  const location = job.location || locationFromMessage || null;

  // Safely derive the "date" we want to store in jobs_demographics
  const jobDateIso = getSafeJobDateIso(job.createdAt);

  console.log(
    `ℹ️ [DEMOS] Job ${jobId} location = "${location}", demographicsStatus = ${job.demographicsStatus}, paidAdsStatus = ${paidAdsStatus}, status = ${job.status}, createdAt = ${job.createdAt || 'NULL'}`
  );

  if (!location) {
    console.warn(
      `⚠️ [DEMOS] Job ${jobId} has no location; cannot process demographics.`
    );

    // Mark main job as failed for demographics
    await markJobDemographicsStatus(jobId, 'failed');
    return;
  }

  // ---- Step 2: Mark main job demographicsStatus = pending ----
  console.log('➡️ [DEMOS] Step 2: Mark main job status = pending');

  try {
    const [updateJob] = await bigquery.createQueryJob({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET demographicsStatus = 'pending'
        WHERE jobId = @jobId
      `,
      params: { jobId },
    });
    const [updateResult] = await updateJob.getQueryResults();
    console.log(
      `ℹ️ [DEMOS] Step 2: Updated main job demographicsStatus to 'pending' for job ${jobId} (dmlStats= ${
        updateResult && updateResult[0] ? JSON.stringify(updateResult[0]) : 'null'
      }).`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error updating main job to pending for job ${jobId}:`,
      err
    );
    // don't hard-fail, continue to try demographics
  }

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

    // Delete any existing row, then insert a "failed" row with nulls
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

  // Clean, human-readable summary instead of giant Big objects
  console.log(
    `ℹ️ [DEMOS] Step 4 storing demographics for job ${jobId}: ` +
      `pop=${parsed.population_no}, age=${parsed.median_age}, households=${parsed.households_no}, ` +
      `income_hh=${parsed.median_income_households}, income_fam=${parsed.median_income_families}, ` +
      `male=${parsed.male_percentage}, female=${parsed.female_percentage}, ` +
      `status=${newDemoStatus}, date=${jobDateIso}`
  );

  // Delete any existing row for this jobId (safe DML – doesn't touch streaming rows)
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

  // Streaming insert full row with metrics + status
  const nowIso = new Date().toISOString();
  const rowToInsert = {
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
    date: jobDateIso,        // <- date from client_audits_jobs.createdAt
    createdAt: nowIso,       // worker insert time
    updatedAt: nowIso,
  };

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

  // ---- Step 5: Update main job's demographicsStatus based on newDemoStatus ----
  console.log('➡️ [DEMOS] Step 5: Update client_audits_jobs.demographicsStatus');

  await markJobDemographicsStatus(jobId, newDemoStatus);

  // ---- Step 6: Optionally update main job.status if all segments done ----
  console.log(
    '➡️ [DEMOS] Step 6: Optionally mark main job status = completed if all segments done'
  );

  try {
    if (newDemoStatus === 'completed' && paidAdsStatus === 'completed') {
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
        `ℹ️ [DEMOS] Step 6: Marked main job status='completed' for job ${jobId} (both demographics + paidAds completed).`
      );
    } else {
      console.log(
        `ℹ️ [DEMOS] Step 6: Main job status NOT set to completed for job ${jobId} (demoStatus=${newDemoStatus}, paidAdsStatus=${paidAdsStatus}).`
      );
    }
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error updating main job status for job ${jobId}:`,
      err
    );
  }
}

// ---------- HELPERS ----------

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
  const d = new Date(createdAt);
  if (Number.isNaN(d.getTime())) {
    console.warn(
      `⚠️ [DEMOS] Invalid createdAt value "${createdAt}"; using now() as date for jobs_demographics.`
    );
    return nowIso;
  }
  return d.toISOString();
}

async function markJobDemographicsStatus(jobId, demoStatus) {
  // demoStatus is expected: 'pending' | 'completed' | 'partial' | 'failed'
  try {
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
  }
}

async function overwriteJobsDemographicsRow(jobId, data) {
  // Convenience for "no data" case
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
    date: data.date || nowIso, // use provided job date or now
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
