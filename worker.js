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

  // ---- Step 2: Ensure pending row exists in jobs_demographics (streaming insert + verify) ----
  console.log('➡️ [DEMOS] Step 2: Ensure pending row exists in jobs_demographics');

  let existingRows;
  try {
    const [rows] = await bigquery.query({
      query: `
        SELECT jobId, status
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
      `ℹ️ [DEMOS] jobs_demographics already has row for job ${jobId} (status=${existingRows[0].status}); will reuse it.`
    );
  } else {
    console.log(
      `ℹ️ [DEMOS] No jobs_demographics row for job ${jobId}, inserting pending row via streaming insert.`
    );

    const pendingRow = {
      jobId,
      status: 'pending',
      location,
      population_no: null,
      median_age: null,
      median_income_households: null,
      median_income_families: null,
      male_percentage: null,
      female_percentage: null,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };

    try {
      await bigquery
        .dataset(DATASET_ID)
        .table(JOBS_DEMOS_TABLE_ID)
        .insert([pendingRow], {
          ignoreUnknownValues: true
        });

      console.log(
        `✅ [DEMOS] Streaming insert pending row for job ${jobId} into ${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}`
      );
    } catch (err) {
      console.error(
        `❌ [DEMOS] Streaming insert failed for job ${jobId} into jobs_demographics:`,
        JSON.stringify(err.errors || err, null, 2)
      );
      // Mark as failed_insert + failed
      await markFailedInsert(jobId);
      return;
    }

    // Step 2b: verify that the row actually exists (up to 2 attempts)
    let foundAfterInsert = false;
    for (let attempt = 1; attempt <= 2; attempt++) {
      await sleep(400 * attempt); // 400ms, 800ms

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
        `❌ [DEMOS] After verification attempts, no jobs_demographics row exists for job ${jobId}. Marking as failed_insert / failed.`
      );
      await markFailedInsert(jobId);
      return;
    }
  }

  // Step 2c: update main job status to 'pending'
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
          SET status = 'no_data', updatedAt = CURRENT_TIMESTAMP()
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
    const [updateJob] = await bigquery.createQueryJob({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        SET
          population_no = @population_no,
          median_age = @median_age,
          median_income_households = @median_income_households,
          median_income_families = @median_income_families,
          male_percentage = @male_percentage,
          female_percentage = @female_percentage,
          status = @status,
          updatedAt = CURRENT_TIMESTAMP()
        WHERE jobId = @jobId
      `,
      params: { ...updateParams, status: newStatus }
    });

    const [metadata] = await updateJob.getMetadata();
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
        SET demographicsStatus = @demographicsStatus
        WHERE jobId = @jobId
      `,
      params: { jobId, demographicsStatus: newStatus }
    });

    const [statusMeta] = await statusJob.getMetadata();
    const dmlStats2 =
      statusMeta.statistics &&
      statusMeta.statistics.query &&
      statusMeta.statistics.query.dmlStats;
    console.log(
      `✅ [DEMOS] Marked demographicsStatus = ${newStatus} for job ${jobId} (dmlStats=`,
      JSON.stringify(dmlStats2 || null, null, 2),
      ')'
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
    if (newStatus === 'completed') {
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
    } else {
      console.log(
        `ℹ️ [DEMOS] Step 6 skipped full-complete status because demographicsStatus is ${newStatus}.`
      );
    }
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error in Step 6 updating main status for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
  }
}

// ---------- MARK FAILED INSERT ----------
async function markFailedInsert(jobId) {
  try {
    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        SET status = 'failed_insert', updatedAt = CURRENT_TIMESTAMP()
        WHERE jobId = @jobId
      `,
      params: { jobId }
    });
  } catch (err) {
    console.error(
      `⚠️ [DEMOS] Could not mark jobs_demographics row as failed_insert for job ${jobId} (maybe row truly missing):`,
      JSON.stringify(err.errors || err, null, 2)
    );
  }

  try {
    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET demographicsStatus = 'failed'
        WHERE jobId = @jobId
      `,
      params: { jobId }
    });
  } catch (err) {
    console.error(
      `⚠️ [DEMOS] Could not mark client_audits_jobs.demographicsStatus = failed for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
  }

  console.log(
    `ℹ️ [DEMOS] Marked job ${jobId} as failed_insert / demographicsStatus=failed`
  );
}

// ---------- START SERVER ----------
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Worker service listening on port ${port}`);
});
