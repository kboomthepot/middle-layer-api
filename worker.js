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
    await markDemographicsFailed(jobId, 'no_location');
    return;
  }

  // ---- Step 2: Mark main job status = pending ----
  console.log('➡️ [DEMOS] Step 2: Mark main job status = pending');

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
    await markDemographicsFailed(jobId, 'query_error');
    return;
  }

  console.log(`ℹ️ [DEMOS] Step 3 result rows (demographics): ${demoRows.length}`);

  if (!demoRows.length) {
    console.warn(
      `⚠️ [DEMOS] No demographics found in ${DEMOS_DATASET_ID}.${DEMOS_SOURCE_TABLE_ID} for location "${location}". Marking as no_data.`
    );
    await markDemographicsNoData(jobId);
    return;
  }

  const demo = demoRows[0];

  console.log(
    `ℹ️ [DEMOS] Found demographics for "${location}": ` +
    JSON.stringify(demo)
  );

  // ---- Step 4: Upsert jobs_demographics with demographics values via MERGE (pure DML) ----
  console.log(
    '➡️ [DEMOS] Step 4: Upsert jobs_demographics with demographics values via MERGE'
  );

  const population_no = toNumberOrNull(demo.population_no);
  const median_age = toNumberOrNull(demo.median_age);
  const median_income_households = toNumberOrNull(demo.median_income_households);
  const median_income_families = toNumberOrNull(demo.median_income_families);
  const male_percentage = toNumberOrNull(demo.male_percentage);
  const female_percentage = toNumberOrNull(demo.female_percentage);

  const metrics = [
    population_no,
    median_age,
    median_income_households,
    median_income_families,
    male_percentage,
    female_percentage
  ];

  const allNull = metrics.every(v => v === null);
  const allNonNull = metrics.every(v => v !== null);

  let newStatus;
  if (allNull) {
    newStatus = 'failed';
  } else if (allNonNull) {
    newStatus = 'completed';
  } else {
    newStatus = 'partial';
  }

  console.log('ℹ️ [DEMOS] Step 4 params:', JSON.stringify({
    jobId,
    location,
    population_no,
    median_age,
    median_income_households,
    median_income_families,
    male_percentage,
    female_percentage,
    newStatus
  }, null, 2));

  try {
    const [mergeJob] = await bigquery.createQueryJob({
      query: `
        MERGE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\` T
        USING (
          SELECT
            @jobId AS jobId,
            @location AS location,
            @population_no AS population_no,
            @median_age AS median_age,
            @median_income_households AS median_income_households,
            @median_income_families AS median_income_families,
            @male_percentage AS male_percentage,
            @female_percentage AS female_percentage,
            @status AS status
        ) S
        ON T.jobId = S.jobId
        WHEN MATCHED THEN
          UPDATE SET
            T.location = S.location,
            T.population_no = S.population_no,
            T.median_age = S.median_age,
            T.median_income_households = S.median_income_households,
            T.median_income_families = S.median_income_families,
            T.male_percentage = S.male_percentage,
            T.female_percentage = S.female_percentage,
            T.status = S.status,
            T.updatedAt = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (
            jobId,
            location,
            population_no,
            median_age,
            median_income_households,
            median_income_families,
            male_percentage,
            female_percentage,
            status,
            createdAt,
            updatedAt
          )
          VALUES (
            S.jobId,
            S.location,
            S.population_no,
            S.median_age,
            S.median_income_households,
            S.median_income_families,
            S.male_percentage,
            S.female_percentage,
            S.status,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
          )
      `,
      params: {
        jobId,
        location,
        population_no,
        median_age,
        median_income_households,
        median_income_families,
        male_percentage,
        female_percentage,
        status: newStatus
      }
    });

    const [metadata] = await mergeJob.getMetadata();
    const dmlStats =
      metadata.statistics &&
      metadata.statistics.query &&
      metadata.statistics.query.dmlStats;
    console.log(
      `✅ [DEMOS] MERGE jobs_demographics completed for job ${jobId}. dmlStats=`,
      JSON.stringify(dmlStats || null, null, 2)
    );

    // Re-check row
    const [checkRows] = await bigquery.query({
      query: `
        SELECT
          jobId,
          location,
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
        `❌ [DEMOS] After MERGE, still no jobs_demographics row for job ${jobId}. Marking as failed.`
      );
      await markDemographicsFailed(jobId, 'merge_missing');
      return;
    }

    // If all values are null, force failed, regardless of what we computed
    if (allNull) {
      console.warn(
        `⚠️ [DEMOS] All metrics NULL for job ${jobId}. Forcing demographicsStatus=failed.`
      );
      await markDemographicsFailed(jobId, 'all_null');
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

    if (newStatus === 'completed') {
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
    } else {
      console.log(
        `ℹ️ [DEMOS] Step 6 skipped full-complete status because demographicsStatus is ${newStatus}.`
      );
    }
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error in Step 4 MERGE for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
    await markDemographicsFailed(jobId, 'merge_error');
  }
}

// ---------- STATUS HELPERS ----------
async function markDemographicsFailed(jobId, reason) {
  console.log(`ℹ️ [DEMOS] Marking job ${jobId} as failed for reason="${reason}"`);

  try {
    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        SET status = 'failed', updatedAt = CURRENT_TIMESTAMP()
        WHERE jobId = @jobId
      `,
      params: { jobId }
    });
  } catch (err) {
    console.error(
      `⚠️ [DEMOS] Could not mark jobs_demographics row as failed for job ${jobId}:`,
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
}

async function markDemographicsNoData(jobId) {
  console.log(`ℹ️ [DEMOS] Marking job ${jobId} as no_data`);

  try {
    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        SET status = 'no_data', updatedAt = CURRENT_TIMESTAMP()
        WHERE jobId = @jobId
      `,
      params: { jobId }
    });
  } catch (err) {
    console.error(
      `⚠️ [DEMOS] Could not mark jobs_demographics row as no_data for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
  }

  try {
    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET demographicsStatus = 'no_data'
        WHERE jobId = @jobId
      `,
      params: { jobId }
    });
  } catch (err) {
    console.error(
      `⚠️ [DEMOS] Could not mark client_audits_jobs.demographicsStatus = no_data for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
  }
}

// ---------- START SERVER ----------
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Worker service listening on port ${port}`);
});
