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

// Pub/Sub sends base64-encoded JSON in { message: { data: "..." } }
app.use(bodyParser.json());

// Simple health check
app.get('/', (req, res) => {
  res.send('Worker service listening on port 8080');
});

// Pub/Sub push endpoint
app.post('/', async (req, res) => {
  try {
    const envelope = req.body;
    if (!envelope || !envelope.message || !envelope.message.data) {
      console.error('❌ Invalid Pub/Sub message format:', JSON.stringify(envelope));
      // Always 204 so Pub/Sub doesn’t retry forever
      return res.status(204).send();
    }

    const payload = JSON.parse(
      Buffer.from(envelope.message.data, 'base64').toString()
    );

    console.log('📩 Received job message:', payload);

    const { jobId, location, createdAt } = payload;

    console.log(
      `✅ Worker received job ${jobId} (location=${location || 'N/A'})`
    );

    if (!jobId) {
      console.error('❌ [DEMOS] Missing jobId in message payload. Skipping.');
      return res.status(204).send();
    }

    await processJobDemographics(jobId);

    // Always ACK so Pub/Sub does not retry
    res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    // Still ACK to avoid infinite retry loops
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
        paidAdsStatus
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

  console.log(
    `ℹ️ [DEMOS] Job ${jobId} location = "${location}", demographicsStatus = ${job.demographicsStatus}, paidAdsStatus = ${job.paidAdsStatus}`
  );

  if (!location) {
    console.warn(
      `⚠️ [DEMOS] Job ${jobId} has no location; cannot process demographics.`
    );
    return;
  }

  // ---- Step 2: Upsert pending row into jobs_demographics ----
  console.log(
    '➡️ [DEMOS] Step 2: Upsert pending row into jobs_demographics'
  );

  const [existingRows] = await bigquery.query({
    query: `
      SELECT jobId
      FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId },
  });

  if (existingRows.length > 0) {
    console.log(
      `ℹ️ [DEMOS] jobs_demographics already has row for job ${jobId}; will not insert duplicate.`
    );
  } else {
    console.log(
      `ℹ️ [DEMOS] No jobs_demographics row for job ${jobId}, inserting pending row.`
    );

    const pendingRow = {
      jobId,
      status: 'pending',
      location,
      population_no: null,
      median_age: null,
      // households_no: null,          // ❌ REMOVE: column does not exist in table
      median_income_households: null,
      median_income_families: null,
      male_percentage: null,
      female_percentage: null,
      createdAt: new Date().toISOString(),
    };

    try {
      await bigquery
        .dataset(DATASET_ID)
        .table(JOBS_DEMOS_TABLE_ID)
        .insert([pendingRow], {
          // Safety: ignore any extra fields that might not exist in schema
          ignoreUnknownValues: true,
        });

      console.log(
        `✅ [DEMOS] Inserted pending demographics row for job ${jobId} into ${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}`
      );
    } catch (err) {
      console.error(
        `❌ [DEMOS] Error inserting pending row for job ${jobId} into jobs_demographics:`,
        JSON.stringify(err.errors || err, null, 2)
      );
      // Don’t throw; we don’t want Pub/Sub retries
      return;
    }
  }

  // ---- Step 3: (NEXT) Fetch actual demographics from Client_audits_data.1_demographics ----
  // We’ll implement this after we confirm rows are being created in jobs_demographics.
  console.log(
    `ℹ️ [DEMOS] Step 2 finished for job ${jobId}. Next step is to wire reading from Client_audits_data.1_demographics.`
  );
}

// ---------- START SERVER ----------
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Worker service listening on port ${port}`);
});
