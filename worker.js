// worker.js

const express = require('express');
const bodyParser = require('body-parser');
const { BigQuery } = require('@google-cloud/bigquery');

const app = express();
app.use(bodyParser.json());

// ---- GCP / BigQuery setup ----
const PROJECT_ID = 'ghs-construction-1734441714520';
const bigquery = new BigQuery({ projectId: PROJECT_ID });

const JOBS_TABLE = `${PROJECT_ID}.Client_audits.client_audits_jobs`;
const JOBS_DEMOS_TABLE = `${PROJECT_ID}.Client_audits.jobs_demographics`;
const DEMOS_SOURCE_TABLE = `${PROJECT_ID}.Client_audits_data.1_demographics`;

// ============================================================================
//  MAIN DEMOGRAPHICS WORKER FUNCTION (your existing logic)
// ============================================================================

async function processJobDemographics(jobId, locationFromMessage) {
  console.log(`▶️ [DEMOS] Starting demographics processing for job ${jobId}`);

  //
  // STEP 1: Load job row from client_audits_jobs
  //
  console.log('➡️ [DEMOS] Step 1: Load job row from client_audits_jobs');
  const [jobRows] = await bigquery.query({
    query: `
      SELECT jobId, location, demographicsStatus, paidAdsStatus
      FROM \`${JOBS_TABLE}\`
      WHERE jobId = @jobId
    `,
    params: { jobId }
  });

  console.log(`ℹ️ [DEMOS] Step 1 result rows: ${jobRows.length}`);
  if (!jobRows.length) {
    console.log(`⚠️ [DEMOS] No job row found in client_audits_jobs for jobId=${jobId}`);
    return;
  }

  const jobRow = jobRows[0];
  const jobLocation = jobRow.location || locationFromMessage;

  console.log(
    `ℹ️ [DEMOS] Job ${jobId} location = "${jobLocation}", ` +
    `demographicsStatus = ${jobRow.demographicsStatus}, paidAdsStatus = ${jobRow.paidAdsStatus}`
  );

  //
  // STEP 2: Ensure pending row exists in jobs_demographics
  //
  console.log('➡️ [DEMOS] Step 2: Upsert pending row into jobs_demographics');
  const [existingDemoRows] = await bigquery.query({
    query: `
      SELECT jobId
      FROM \`${JOBS_DEMOS_TABLE}\`
      WHERE jobId = @jobId
    `,
    params: { jobId }
  });

  if (!existingDemoRows.length) {
    console.log(
      `ℹ️ [DEMOS] No jobs_demographics row for job ${jobId}, inserting pending row.`
    );

    const nowIso = new Date().toISOString();

    try {
      await bigquery.dataset('Client_audits')
        .table('jobs_demographics')
        .insert([{
          jobId,
          status: 'pending',
          location: jobLocation,
          population_no: null,
          median_age: null,
          households_no: null,
          median_income_households: null,
          median_income_families: null,
          male_percentage: null,
          female_percentage: null,
          createdAt: nowIso,
          updatedAt: nowIso
        }]);

      console.log(
        `✅ [DEMOS] Inserted pending demographics row for job ${jobId} into Client_audits.jobs_demographics`
      );
    } catch (err) {
      console.error(
        `❌ [DEMOS] Error inserting pending row for job ${jobId} into jobs_demographics:`,
        JSON.stringify(err.errors || err, null, 2)
      );
      // Don't crash container, just stop this run
      return;
    }
  } else {
    console.log(
      `ℹ️ [DEMOS] jobs_demographics row already exists for job ${jobId}, will update it.`
    );
  }

  //
  // STEP 3: Load demographics from Client_audits_data.1_demographics
  //
  console.log('➡️ [DEMOS] Step 3: Load demographics from Client_audits_data.1_demographics');

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
      FROM \`${DEMOS_SOURCE_TABLE}\`
      WHERE location = @location
      LIMIT 1
    `,
    params: { location: jobLocation }
  });

  console.log(`ℹ️ [DEMOS] Step 3 result rows (demographics): ${demoRows.length}`);

  if (!demoRows.length) {
    console.log(
      `⚠️ [DEMOS] No demographics row found for "${jobLocation}" in 1_demographics. ` +
      `Marking jobs_demographics + job as no_data.`
    );

    await bigquery.query({
      query: `
        UPDATE \`${JOBS_DEMOS_TABLE}\`
        SET status = 'no_data', updatedAt = CURRENT_TIMESTAMP()
        WHERE jobId = @jobId
      `,
      params: { jobId }
    });

    await bigquery.query({
      query: `
        UPDATE \`${JOBS_TABLE}\`
        SET demographicsStatus = 'no_data', updatedAt = CURRENT_TIMESTAMP()
        WHERE jobId = @jobId
      `,
      params: { jobId }
    });

    return;
  }

  const demo = demoRows[0];

  console.log(
    `ℹ️ [DEMOS] Found demographics for "${jobLocation}": ` +
    JSON.stringify(demo)
  );

  //
  // STEP 4: Update jobs_demographics with demographics values
  //
  console.log('➡️ [DEMOS] Step 4: Update jobs_demographics with demographics values');

  const updateParams = {
    jobId,
    population_no: demo.population_no != null && demo.population_no !== ''
      ? Number(demo.population_no)
      : null,
    median_age: demo.median_age != null && demo.median_age !== ''
      ? Number(demo.median_age)
      : null,
    households_no: demo.households_no != null && demo.households_no !== ''
      ? Number(demo.households_no)
      : null,
    median_income_households:
      demo.median_income_households != null && demo.median_income_households !== ''
        ? Number(demo.median_income_households)
        : null,
    median_income_families:
      demo.median_income_families != null && demo.median_income_families !== ''
        ? Number(demo.median_income_families)
        : null,
    male_percentage: demo.male_percentage != null && demo.male_percentage !== ''
      ? Number(demo.male_percentage)
      : null,
    female_percentage: demo.female_percentage != null && demo.female_percentage !== ''
      ? Number(demo.female_percentage)
      : null
  };

  console.log('ℹ️ [DEMOS] Step 4 params:', JSON.stringify(updateParams, null, 2));

  try {
    const [updateJob] = await bigquery.createQueryJob({
      query: `
        UPDATE \`${JOBS_DEMOS_TABLE}\`
        SET
          population_no = @population_no,
          median_age = @median_age,
          households_no = @households_no,
          median_income_households = @median_income_households,
          median_income_families = @median_income_families,
          male_percentage = @male_percentage,
          female_percentage = @female_percentage,
          status = 'completed',
          updatedAt = CURRENT_TIMESTAMP()
        WHERE jobId = @jobId
      `,
      params: updateParams
    });

    const [updateResult] = await updateJob.getQueryResults();
    console.log(
      `✅ [DEMOS] Updated jobs_demographics for job ${jobId}. ` +
      `DML result row count (usually 0 for UPDATE): ${updateResult.length}`
    );

    //
    // STEP 5: Mark demographicsStatus = completed on main jobs table
    //
    await bigquery.createQueryJob({
      query: `
        UPDATE \`${JOBS_TABLE}\`
        SET demographicsStatus = 'completed', updatedAt = CURRENT_TIMESTAMP()
        WHERE jobId = @jobId
      `,
      params: { jobId }
    });

    console.log(`✅ [DEMOS] Marked demographicsStatus=completed for job ${jobId} in client_audits_jobs`);
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error updating jobs_demographics for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
    // Let Pub/Sub retry if needed, but don't crash the whole container here
    throw err;
  }
}

// ============================================================================
//  PUB/SUB PUSH ENDPOINT
// ============================================================================

// Simple health endpoint for Cloud Run
app.get('/', (req, res) => {
  res.send('client-audits-worker is running');
});

// Pub/Sub push handler
app.post('/pubsub/push', async (req, res) => {
  try {
    const message = req.body.message;

    if (!message || !message.data) {
      console.warn('⚠️ [PUBSUB] No message.data in request body:', JSON.stringify(req.body));
      return res.status(400).send('No message data');
    }

    const decoded = Buffer.from(message.data, 'base64').toString('utf8');
    console.log('📨 [PUBSUB] Decoded message data:', decoded);

    let payload;
    try {
      payload = JSON.parse(decoded);
    } catch (e) {
      console.error('❌ [PUBSUB] Failed to parse JSON from message data:', e);
      return res.status(400).send('Invalid JSON');
    }

    const { jobId, location } = payload;
    if (!jobId) {
      console.error('❌ [PUBSUB] jobId missing from payload:', payload);
      return res.status(400).send('jobId is required');
    }

    await processJobDemographics(jobId, location || null);

    // Pub/Sub expects 204 on success
    res.status(204).send();
  } catch (err) {
    console.error('❌ [PUBSUB] Error in /pubsub/push handler:', err);
    // 500 → Pub/Sub will retry according to subscription settings
    res.status(500).send('Internal error');
  }
});

// ============================================================================
//  START SERVER (this is what Cloud Run cares about)
// ============================================================================

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`🚀 client-audits-worker listening on port ${PORT}`);
});
