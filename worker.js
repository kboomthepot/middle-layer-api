// worker.js
const express = require('express');
const bodyParser = require('body-parser');
const { BigQuery } = require('@google-cloud/bigquery');

const app = express();
app.use(bodyParser.json());

// --- BigQuery setup ---
const bigquery = new BigQuery();

const PROJECT_ID = process.env.GCP_PROJECT || 'ghs-construction-1734441714520';

// Main jobs table
const JOBS_DATASET_ID = 'Client_audits';
const JOBS_TABLE_ID = 'client_audits_jobs';

// Demographics source table
const DEMOS_DATASET_ID = 'Client_audits_data';
const DEMOS_SOURCE_TABLE_ID = '1_demographics';

// Demographics jobs table (target)
const JOBS_DEMOS_TABLE_ID = 'jobs_demographics';

// =============== Pub/Sub HTTP endpoint ===============
app.post('/', async (req, res) => {
  try {
    const pubsubMessage = req.body.message;
    if (!pubsubMessage || !pubsubMessage.data) {
      console.warn('Received message without data, ignoring.');
      return res.status(204).send();
    }

    const payloadJson = Buffer.from(pubsubMessage.data, 'base64').toString('utf8');
    let payload;
    try {
      payload = JSON.parse(payloadJson);
    } catch (e) {
      console.error('Failed to parse message data JSON:', payloadJson, e);
      // ACK anyway so Pub/Sub doesn’t retry forever
      return res.status(204).send();
    }

    console.log('📩 Received job message:', payload);

    const { jobId } = payload;
    if (!jobId) {
      console.warn('Message missing jobId, ignoring.');
      return res.status(204).send();
    }

    console.log(`✅ Worker received job ${jobId} (location=${payload.location})`);

    // Run demographics processing. Any errors are caught/logged inside.
    await processJobDemographics(jobId);

    // Always ACK so Pub/Sub does not retry the same message in a loop
    return res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message (but acking to stop retries):', err);
    // Still ACK the message – we don’t want infinite retries
    return res.status(204).send();
  }
});

// =============== Demographics pipeline ===============
async function processJobDemographics(jobId) {
  console.log(`▶️ [DEMOS] Starting demographics processing for job ${jobId}`);

  // ---- Step 1: load job row from client_audits_jobs ----
  console.log('➡️ [DEMOS] Step 1: Load job row from client_audits_jobs');
  const [jobRows] = await bigquery.query({
    query: `
      SELECT jobId, location, demographicsStatus, paidAdsStatus, status
      FROM \`${PROJECT_ID}.${JOBS_DATASET_ID}.${JOBS_TABLE_ID}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId },
  });

  console.log(`ℹ️ [DEMOS] Step 1 result rows: ${jobRows.length}`);
  if (!jobRows.length) {
    console.warn(`[DEMOS] Job ${jobId} not found; skipping.`);
    return;
  }

  const job = jobRows[0];
  const location = job.location;

  console.log(
    `ℹ️ [DEMOS] Job ${jobId} location = ${JSON.stringify(location)}, demographicsStatus = ${job.demographicsStatus}, paidAdsStatus = ${job.paidAdsStatus}`
  );

  if (!location) {
    console.warn(`[DEMOS] Job ${jobId} has no location; marking demographicsStatus = 'no_location'.`);
    await safeUpdateJobDemographicsStatus(jobId, 'no_location');
    return;
  }

  // ---- Step 2: ensure a pending row exists in jobs_demographics ----
  console.log('➡️ [DEMOS] Step 2: Upsert pending row into jobs_demographics');

  const [existingRows] = await bigquery.query({
    query: `
      SELECT jobId, status
      FROM \`${PROJECT_ID}.${JOBS_DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId },
  });

  if (!existingRows.length) {
    console.log(
      `ℹ️ [DEMOS] No jobs_demographics row for job ${jobId}, inserting pending row.`
    );

    const pendingRow = {
      jobId,
      status: 'pending',
      location,
      population_no: null,
      median_age: null,
      households_no: null,
      median_income_households: null,
      median_income_families: null,
      male_percentage: null,
      female_percentage: null,
      createdAt: new Date().toISOString(),
    };

    try {
      await bigquery.dataset(JOBS_DATASET_ID).table(JOBS_DEMOS_TABLE_ID).insert([pendingRow]);
      console.log(`✅ [DEMOS] Pending row written for job ${jobId}`);
    } catch (err) {
      console.error(
        `❌ [DEMOS] Error inserting pending row for job ${jobId} into jobs_demographics:`,
        JSON.stringify(err.errors || err, null, 2)
      );
      // Stop here for this job – but don’t throw, caller already ACK’d message.
      return;
    }
  } else {
    console.log(
      `ℹ️ [DEMOS] jobs_demographics row already exists for job ${jobId} with status=${existingRows[0].status}`
    );
  }

  // ---- Step 3: load demographics for this location ----
  console.log(
    `➡️ [DEMOS] Step 3: Load demographics source row for location "${location}" from 1_demographics`
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

  if (!demoRows.length) {
    console.warn(
      `[DEMOS] No demographics found for location "${location}" (jobId: ${jobId}). Marking status = no_data.`
    );
    await safeUpdateJobsDemographicsRow(jobId, { status: 'no_data' });
    await safeUpdateJobDemographicsStatus(jobId, 'no_data');
    return;
  }

  const d = demoRows[0];

  // ---- Step 4: update jobs_demographics with actual values ----
  console.log(
    `➡️ [DEMOS] Step 4: Update jobs_demographics row with demographics values for job ${jobId}`
  );

  try {
    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${JOBS_DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        SET
          status = 'completed',
          population_no = @population_no,
          median_age = @median_age,
          households_no = @households_no,
          median_income_households = @median_income_households,
          median_income_families = @median_income_families,
          male_percentage = @male_percentage,
          female_percentage = @female_percentage
        WHERE jobId = @jobId
      `,
      params: {
        jobId,
        population_no: d.population_no ?? null,
        median_age: d.median_age ?? null,
        households_no: d.households_no ?? null,
        median_income_households: d.median_income_households ?? null,
        median_income_families: d.median_income_families ?? null,
        male_percentage: d.male_percentage ?? null,
        female_percentage: d.female_percentage ?? null,
      },
    });

    console.log(`✅ [DEMOS] Completed demographics for job ${jobId}`);
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error updating jobs_demographics for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
    return; // don’t throw – caller will have ACK’d the message already
  }

  // ---- Step 5: update main job table statuses ----
  console.log(
    `➡️ [DEMOS] Step 5: Update client_audits_jobs.demographicsStatus and overall status for job ${jobId}`
  );

  await safeUpdateJobDemographicsStatus(jobId, 'completed');

  console.log(`🎉 [DEMOS] Finished processing demographics for job ${jobId}`);
}

// =============== Helper: update jobs_demographics row ===============
async function safeUpdateJobsDemographicsRow(jobId, fields) {
  const setParts = [];
  const params = { jobId };
  let i = 0;

  for (const [key, value] of Object.entries(fields)) {
    const paramName = `p${i++}`;
    setParts.push(`${key} = @${paramName}`);
    params[paramName] = value;
  }

  if (!setParts.length) return;

  const setClause = setParts.join(', ');

  try {
    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${JOBS_DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        SET ${setClause}
        WHERE jobId = @jobId
      `,
      params,
    });

    console.log(
      `✅ [DEMOS] Updated jobs_demographics for job ${jobId} with fields: ${Object.keys(
        fields
      ).join(', ')}`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error in safeUpdateJobsDemographicsRow for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
  }
}

// =============== Helper: update main jobs table status ===============
async function safeUpdateJobDemographicsStatus(jobId, demographicsStatus) {
  try {
    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${JOBS_DATASET_ID}.${JOBS_TABLE_ID}\`
        SET
          demographicsStatus = @demographicsStatus,
          status = CASE
            WHEN @demographicsStatus = 'completed' AND paidAdsStatus = 'completed'
              THEN 'completed'
            WHEN @demographicsStatus IN ('no_location', 'no_data')
              THEN 'needs_review'
            ELSE status
          END
        WHERE jobId = @jobId
      `,
      params: { jobId, demographicsStatus },
    });

    console.log(
      `✅ [DEMOS] Updated client_audits_jobs.demographicsStatus to "${demographicsStatus}" for job ${jobId}`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error updating client_audits_jobs demographicsStatus for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
  }
}

// =============== Start server ===============
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Worker service listening on port ${port}`);
});
