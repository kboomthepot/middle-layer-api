// worker.js
// Event-driven worker for client audit jobs (demographics segment)

'use strict';

const express = require('express');
const bodyParser = require('body-parser');
const { BigQuery } = require('@google-cloud/bigquery');

const bigquery = new BigQuery();

// ---- CONFIG ----
const PROJECT_ID = process.env.GCP_PROJECT || 'ghs-construction-1734441714520';

// Main jobs table
const JOBS_DATASET_ID = 'Client_audits';
const JOBS_TABLE_ID = 'client_audits_jobs';

// Demographics jobs table (output)
const JOBS_DEMOS_DATASET_ID = 'Client_audits';
const JOBS_DEMOS_TABLE_ID = 'jobs_demographics';

// Source demographics data
const DEMOS_DATASET_ID = 'Client_audits_data';
const DEMOS_TABLE_ID = '1_demographics';

const app = express();
app.use(bodyParser.json());

// -------------------- utils --------------------

function parseJobMessage(req) {
  // Pub/Sub push format
  if (req.body && req.body.message && req.body.message.data) {
    const decoded = Buffer.from(req.body.message.data, 'base64').toString('utf8');
    const msg = JSON.parse(decoded);
    console.log('📩 Received job message:', msg);
    return msg;
  }

  // Direct POST (Postman) – treat body as the message
  console.log('📩 Received non-Pub/Sub body:', req.body);
  return req.body;
}

// ----------------- DEMOGRAPHICS ----------------

async function processJobDemographics(jobId) {
  console.log(`▶️ [DEMOS] Starting demographics processing for job ${jobId}`);

  // 🔹 STEP 1: Load job row from client_audits_jobs
  console.log('➡️ [DEMOS] Step 1: Load job row from client_audits_jobs');

  const [jobRows] = await bigquery.query({
    query: `
      SELECT jobId, location
      FROM \`${PROJECT_ID}.${JOBS_DATASET_ID}.${JOBS_TABLE_ID}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId },
  });

  console.log(`ℹ️ [DEMOS] Step 1 result rows: ${jobRows.length}`);

  if (!jobRows.length) {
    console.warn(`⚠️ [DEMOS] No job row found for jobId=${jobId}`);
    return;
  }

  const job = jobRows[0];
  console.log(
    `ℹ️ [DEMOS] Job ${jobId} location = "${job.location}"`
  );

  // 🔹 STEP 2: Insert / reset pending row in jobs_demographics
  console.log('➡️ [DEMOS] Step 2: Upsert pending row into jobs_demographics');

  const upsertPendingQuery = `
    BEGIN TRANSACTION;

    DELETE FROM \`${PROJECT_ID}.${JOBS_DEMOS_DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
    WHERE jobId = @jobId;

    INSERT \`${PROJECT_ID}.${JOBS_DEMOS_DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\` (
      jobId,
      status,
      location,
      population_no,
      households_no,
      median_age,
      median_income_households,
      median_income_families,
      male_percentage,
      female_percentage
    )
    VALUES (
      @jobId,
      'pending',
      @location,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    );

    COMMIT TRANSACTION;
  `;

  await bigquery.query({
    query: upsertPendingQuery,
    params: {
      jobId,
      location: job.location,
    },
  });

  console.log(`✅ [DEMOS] Pending row written for job ${jobId}`);

  // 🔹 STEP 3: Lookup demographics row for the job location
  console.log('➡️ [DEMOS] Step 3: Lookup demographics row');

  const [demoRows] = await bigquery.query({
    query: `
      SELECT
        location,
        population_no,
        households_no,
        median_age,
        median_income_households,
        median_income_families,
        male_percentage,
        female_percentage
      FROM \`${PROJECT_ID}.${DEMOS_DATASET_ID}.${DEMOS_TABLE_ID}\`
      WHERE location = @location
      LIMIT 1
    `,
    params: { location: job.location },
  });

  console.log(`ℹ️ [DEMOS] Step 3 result rows: ${demoRows.length}`);

  if (!demoRows.length) {
    console.warn(
      `⚠️ [DEMOS] No demographics row found for location="${job.location}"`
    );

    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${JOBS_DEMOS_DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        SET status = 'not_found'
        WHERE jobId = @jobId
      `,
      params: { jobId },
    });

    console.log(
      `⚠️ [DEMOS] Marked job ${jobId} as not_found in jobs_demographics`
    );
    return;
  }

  const d = demoRows[0];

  // 🔹 STEP 4: Update jobs_demographics with real values
  console.log(
    '➡️ [DEMOS] Step 4: Update jobs_demographics with demographics values'
  );

  await bigquery.query({
    query: `
      UPDATE \`${PROJECT_ID}.${JOBS_DEMOS_DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
      SET
        status = 'completed',
        population_no = @population_no,
        households_no = @households_no,
        median_age = @median_age,
        median_income_households = @median_income_households,
        median_income_families = @median_income_families,
        male_percentage = @male_percentage,
        female_percentage = @female_percentage
      WHERE jobId = @jobId
    `,
    params: {
      jobId,
      population_no: d.population_no ?? null,
      households_no: d.households_no ?? null,
      median_age: d.median_age ?? null,
      median_income_households: d.median_income_households ?? null,
      median_income_families: d.median_income_families ?? null,
      male_percentage: d.male_percentage ?? null,
      female_percentage: d.female_percentage ?? null,
    },
  });

  console.log(`✅ [DEMOS] Completed demographics for job ${jobId}`);
}

// ---------------- HTTP handler (Pub/Sub push) ----------------

app.post('/', async (req, res) => {
  try {
    const msg = parseJobMessage(req);

    if (!msg || !msg.jobId) {
      console.error('❌ Invalid message, missing jobId', msg);
      res.status(400).send('Bad Request: missing jobId');
      return;
    }

    console.log(
      `✅ Worker received job ${msg.jobId} (location=${msg.location})`
    );

    await processJobDemographics(msg.jobId);

    // Ack success
    res.status(204).end();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    // 500 => Pub/Sub will retry later
    res.status(500).send('Internal Server Error');
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Worker service listening on port ${PORT}`);
});
