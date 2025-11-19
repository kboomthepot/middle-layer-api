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

// ---------- HELPERS ----------
function toNumberOrNull(value) {
  if (value === null || value === undefined) return null;
  if (value === '') return null;
  const n = Number(value);
  return Number.isNaN(n) ? null : n;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------- HEALTH CHECK ----------
app.get('/', (req, res) => {
  res.send('Worker service listening on port 8080');
});

// ---------- PUB/SUB PUSH ENDPOINT ----------
app.post('/', async (req, res) => {
  try {
    const envelope = req.body;

    if (!envelope || !envelope.message || !envelope.message.data) {
      console.error('❌ Invalid Pub/Sub message format:', JSON.stringify(envelope));
      // ACK anyway so Pub/Sub doesn’t retry forever
      return res.status(204).send();
    }

    const decoded = Buffer.from(envelope.message.data, 'base64').toString('utf8');

    let payload;
    try {
      payload = JSON.parse(decoded);
    } catch (e) {
      console.error('❌ Failed to parse Pub/Sub message JSON:', decoded, e);
      return res.status(204).send();
    }

    console.log('📩 Received job message:', payload);

    const { jobId, location } = payload;

    if (!jobId) {
      console.error('❌ [DEMOS] Missing jobId in message payload. Skipping.');
      return res.status(204).send();
    }

    console.log(
      `✅ Worker received job ${jobId} (location=${location || 'N/A'})`
    );

    await processJobDemographics(jobId, location || null);

    // Always ACK so Pub/Sub does not retry this message
    res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    // ACK even on error to avoid infinite retry loop
    res.status(204).send();
  }
});

// ---------- DEMOGRAPHICS PROCESSOR ----------
async function processJobDemographics(jobId, locationFromMessage) {
  console.log(`▶️ [DEMOS] Starting demographics processing for job ${jobId}`);

  // ---- Step 1: Load job row from client_audits_jobs ----
  console.log('➡️ [DEMOS] Step 1: Load job row from client_audits_jobs');

  let jobRows;
  try {
    const [rows] = await bigquery.query({
      query: `
        SELECT
          jobId,
          location,
          demographicsStatus,
          paidAdsStatus,
          status
        FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        WHERE jobId = @jobId
        LIMIT 1
      `,
      params: { jobId }
    });
    jobRows = rows;
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error querying job ${jobId} in ${DATASET_ID}.${JOBS_TABLE_ID}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
    return;
  }

  console.log(`ℹ️ [DEMOS] Step 1 result rows: ${jobRows.length}`);

  if (!jobRows.length) {
    console.warn(
      `⚠️ [DEMOS] Job ${jobId} not found in ${DATASET_ID}.${JOBS_TABLE_ID}.`
    );
    return;
  }

  const job = jobRows[0];
  const location = job.location || locationFromMessage || null;

  console.log(
    `ℹ️ [DEMOS] Job ${jobId} location = "${location}", ` +
    `demographicsStatus = ${job.demographicsStatus}, paidAdsStatus = ${job.paidAdsStatus}, status = ${job.status}`
  );

  if (!location) {
    console.warn(
      `⚠️ [DEMOS] Job ${jobId} has no location; cannot process demographics.`
    );
    return;
  }

  // ---- Step 2: Upsert pending row into jobs_demographics ----
  console.log('➡️ [DEMOS] Step 2: Upsert pending row into jobs_demographics');

  let existingRows;
  try {
    const [rows] = await bigquery.query({
      query: `
        SELECT jobId
        FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        WHERE jobId = @jobId
        LIMIT 1
      `,
      params: { jobId }
    });
    existingRows = rows;
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error checking existing jobs_demographics for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
    return;
  }

  if (existingRows.length > 0) {
    console.log(
      `ℹ️ [DEMOS] jobs_demographics already has row for job ${jobId}; will not insert duplicate.`
    );
  } else {
    console.log(
      `ℹ️ [DEMOS] No jobs_demographics row for job ${jobId}, inserting pending row via DML.`
    );

    try {
      const [insertJob] = await bigquery.createQueryJob({
        query: `
          INSERT \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
            (jobId, status, location, population_no, median_age,
             median_income_households, median_income_families,
             male_percentage, female_percentage, createdAt)
          VALUES
            (@jobId, 'pending', @location, NULL, NULL,
             NULL, NULL, NULL, NULL, CURRENT_TIMESTAMP())
        `,
        params: { jobId, location }
      });

      const [insertMeta] = await insertJob.getMetadata();
      const dmlStats =
        insertMeta.statistics &&
        insertMeta.statistics.query &&
        insertMeta.statistics.query.dmlStats;
      const insertedRows = dmlStats ? dmlStats.insertedRowCount : 'unknown';

      console.log(
        `✅ [DEMOS] Inserted pending demographics row for job ${jobId} (insertedRowCount=${insertedRows})`
      );
    } catch (err) {
      console.error(
        `❌ [DEMOS] Error inserting pending row for job ${jobId} into jobs_demographics (DML):`,
        JSON.stringify(err.errors || err, null, 2)
      );
      return; // don't continue if we can't create the row
    }
  }

  // Step 2b: verify that the pending row actually exists (up to 3 attempts)
  let foundAfterInsert = false;
  for (let attempt = 1; attempt <= 3; attempt++) {
    await sleep(300 * attempt); // small backoff: 300ms, 600ms, 900ms

    const [verifyRows] = await bigquery.query({
      query: `
        SELECT jobId, status
        FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        WHERE jobId = @jobId
        LIMIT 1
      `,
      params: { jobId }
    });

    if (verifyRows.length > 0) {
      console.log(
        `ℹ️ [DEMOS] Verification attempt ${attempt}: jobs_demographics row FOUND for job ${jobId} with status=${verifyRows[0].status}`
      );
      foundAfterInsert = true;
      break;
    } else {
      console.warn(
        `⚠️ [DEMOS] Verification attempt ${attempt}: no jobs_demographics row found yet for job ${jobId}`
      );
    }
  }

  if (!foundAfterInsert) {
    console.error(
      `❌ [DEMOS] After 3 verification attempts, no jobs_demographics row exists for job ${jobId}. Marking as failed.`
    );

    // Try to mark statuses as failed
    try {
      // In case a phantom row exists, mark it failed
      await bigquery.query({
        query: `
          UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
          SET status = 'failed_insert'
          WHERE jobId = @jobId
        `,
        params: { jobId }
      });

      await bigquery.query({
        query: `
          UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
          SET demographicsStatus = 'failed'
          WHERE jobId = @jobId
        `,
        params: { jobId }
      });

      console.log(
        `ℹ️ [DEMOS] Marked job ${jobId} as failed (failed_insert / demographicsStatus=failed).`
      );
    } catch (markErr) {
      console.error(
        `⚠️ [DEMOS] Failed to update failure statuses for job ${jobId}:`,
        JSON.stringify(markErr.errors || markErr, null, 2)
      );
    }

    return; // stop processing this job
  }

  // At the end of Step 2, mark main job's overall status = 'pending'
  try {
    const [pendingJob] = await bigquery.createQueryJob({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET status = 'pending'
        WHERE jobId = @jobId
      `,
      params: { jobId }
    });
    const [pendingMeta] = await pendingJob.getMetadata();
    const dmlStatsPending =
      pendingMeta.statistics &&
      pendingMeta.statistics.query &&
      pendingMeta.statistics.query.dmlStats;
    const updatedPending = dmlStatsPending ? dmlStatsPending.updatedRowCount : 'unknown';
    console.log(
      `ℹ️ [DEMOS] Step 2: Updated main job status to 'pending' for job ${jobId} (updatedRowCount=${updatedPending}).`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] Failed to update main job status to 'pending' for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
  }

  // ---- Step 3: Load demographics source row from Client_audits_data.1_demographics ----
  console.log(
    '➡️ [DEMOS] Step 3: Load demographics from Client_audits_data.1_demographics'
  );

  let demoRows;
  try {
    const [rows] = await bigquery.query({
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
      params: { location }
    });
    demoRows = rows;
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error querying demographics for location "${location}":`,
      JSON.stringify(err.errors || err, null, 2)
    );
    return;
  }

  console.log(`ℹ️ [DEMOS] Step 3 result rows (demographics): ${demoRows.length}`);

  if (!demoRows.length) {
    console.warn(
      `⚠️ [DEMOS] No demographics found in ${DEMOS_DATASET_ID}.${DEMOS_SOURCE_TABLE_ID} for location "${location}". Marking as no_data.`
    );

    try {
      await bigquery.query({
        query: `
          UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
          SET status = 'no_data'
          WHERE jobId = @jobId
        `,
        params: { jobId }
      });

      await bigquery.query({
        query: `
          UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
          SET demographicsStatus = 'no_data'
          WHERE jobId = @jobId
        `,
        params: { jobId }
      });

      console.log(
        `ℹ️ [DEMOS] Marked job ${jobId} as no_data for demographics.`
      );
    } catch (err) {
      console.error(
        `❌ [DEMOS] Error marking job ${jobId} as no_data:`,
        JSON.stringify(err.errors || err, null, 2)
      );
    }

    return;
  }

  const demo = demoRows[0];

  console.log(
    `ℹ️ [DEMOS] Found demographics for "${location}": ` +
    JSON.stringify(demo)
  );

  // ---- Step 4: Update jobs_demographics row with actual values ----
  console.log(
    '➡️ [DEMOS] Step 4: Update jobs_demographics with demographics values'
  );

  const updateParams = {
    jobId,
    population_no: toNumberOrNull(demo.population_no),
    median_age: toNumberOrNull(demo.median_age),
    median_income_households: toNumberOrNull(demo.median_income_households),
    median_income_families: toNumberOrNull(demo.median_income_families),
    male_percentage: toNumberOrNull(demo.male_percentage),
    female_percentage: toNumberOrNull(demo.female_percentage)
  };

  const allMetricsPresent = [
    updateParams.population_no,
    updateParams.median_age,
    updateParams.median_income_households,
    updateParams.median_income_families,
    updateParams.male_percentage,
    updateParams.female_percentage
  ].every(v => v !== null);

  const newStatus = allMetricsPresent ? 'completed' : 'partial';

  console.log('ℹ️ [DEMOS] Step 4 params:', JSON.stringify(updateParams, null, 2));
  console.log(`ℹ️ [DEMOS] Step 4 newStatus for job ${jobId}: ${newStatus}`);

  try {
    // Run UPDATE as a query job so we can inspect DML stats
    const [jobObj] = await bigquery.createQueryJob({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        SET
          population_no = @population_no,
          median_age = @median_age,
          median_income_households = @median_income_households,
          median_income_families = @median_income_families,
          male_percentage = @male_percentage,
          female_percentage = @female_percentage,
          status = @status
        WHERE jobId = @jobId
      `,
      params: { ...updateParams, status: newStatus }
    });

    const [metadata] = await jobObj.getMetadata();
    const dmlStats =
      metadata.statistics &&
      metadata.statistics.query &&
      metadata.statistics.query.dmlStats;
    const updatedRows = dmlStats ? dmlStats.updatedRowCount : 'unknown';

    console.log(
      `✅ [DEMOS] UPDATE jobs_demographics completed for job ${jobId}. updatedRowCount=${updatedRows}`
    );

    // Re-select the row to see exactly what is stored now
    const [checkRows] = await bigquery.query({
      query: `
        SELECT jobId, location,
               population_no,
               median_age,
               median_income_households,
               median_income_families,
               male_percentage,
               female_percentage,
               status
        FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        WHERE jobId = @jobId
        LIMIT 1
      `,
      params: { jobId }
    });

    const checkRow = checkRows[0] || null;

    console.log(
      `ℹ️ [DEMOS] Step 4 check row for job ${jobId}:`,
      JSON.stringify(checkRow, null, 2)
    );

    if (!checkRow) {
      console.error(
        `❌ [DEMOS] Step 4: After UPDATE, no jobs_demographics row found for job ${jobId}. Marking as error_update.`
      );

      await bigquery.query({
        query: `
          UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
          SET status = 'error_update'
          WHERE jobId = @jobId
        `,
        params: { jobId }
      });

      await bigquery.query({
        query: `
          UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
          SET demographicsStatus = 'error'
          WHERE jobId = @jobId
        `,
        params: { jobId }
      });

      return;
    }
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error in Step 4 updating jobs_demographics for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );

    // Try to mark as error
    try {
      await bigquery.query({
        query: `
          UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
          SET status = 'error'
          WHERE jobId = @jobId
        `,
        params: { jobId }
      });

      await bigquery.query({
        query: `
          UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
          SET demographicsStatus = 'error'
          WHERE jobId = @jobId
        `,
        params: { jobId }
      });
    } catch (markErr) {
      console.error(
        `⚠️ [DEMOS] Failed to mark job ${jobId} as error after Step 4 failure:`,
        JSON.stringify(markErr.errors || markErr, null, 2)
      );
    }

    return;
  }

  // ---- Step 5: Update job's demographicsStatus in main table ----
  console.log(
    '➡️ [DEMOS] Step 5: Update client_audits_jobs.demographicsStatus'
  );

  try {
    const [statusJob] = await bigquery.createQueryJob({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET demographicsStatus = 'completed'
        WHERE jobId = @jobId
      `,
      params: { jobId }
    });

    const [statusMeta] = await statusJob.getMetadata();
    const dmlStats2 =
      statusMeta.statistics &&
      statusMeta.statistics.query &&
      statusMeta.statistics.query.dmlStats;
    const updatedRows2 = dmlStats2 ? dmlStats2.updatedRowCount : 'unknown';

    console.log(
      `✅ [DEMOS] Marked demographicsStatus = completed for job ${jobId} (updatedRowCount=${updatedRows2})`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error in Step 5 updating client_audits_jobs for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
    return;
  }

  // ---- Step 6: Optionally update main status if all segments done ----
  console.log(
    '➡️ [DEMOS] Step 6: Optionally mark main job status = completed if all segments done'
  );

  try {
    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET status = 'completed'
        WHERE jobId = @jobId
          AND demographicsStatus = 'completed'
          AND paidAdsStatus = 'completed'
      `,
      params: { jobId }
    });

    console.log(
      `ℹ️ [DEMOS] Step 6 checked for full completion for job ${jobId}.`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error in Step 6 updating main status for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
  }
}

// ---------- START SERVER ----------
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Worker service listening on port ${port}`);
});
