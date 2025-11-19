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
      // ACK anyway so Pub/Sub doesn’t retry forever
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

    // Always ACK so Pub/Sub does not retry this message
    res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    // ACK even on error to avoid infinite retry loop
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
        paidAdsStatus,
        status
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
      // households_no: null, // <- not in jobs_demographics schema (yet)
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
          ignoreUnknownValues: true, // safety
        });

      console.log(
        `✅ [DEMOS] Inserted pending demographics row for job ${jobId} into ${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}`
      );
    } catch (err) {
      console.error(
        `❌ [DEMOS] Error inserting pending row for job ${jobId} into jobs_demographics:`,
        JSON.stringify(err.errors || err, null, 2)
      );
      return; // don't continue if we can't create the row
    }
  }

  // ---- Step 3: Load demographics source row from Client_audits_data.1_demographics ----
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
      `⚠️ [DEMOS] No demographics found in ${DEMOS_DATASET_ID}.${DEMOS_SOURCE_TABLE_ID} for location "${location}". Marking as no_data.`
    );

    // Mark jobs_demographics + job as no_data
    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        SET status = 'no_data'
        WHERE jobId = @jobId
      `,
      params: { jobId },
    });

    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET demographicsStatus = 'no_data'
        WHERE jobId = @jobId
      `,
      params: { jobId },
    });

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

  // Ensure numeric fields are numbers or null
  const updateParams = {
    jobId,
    population_no:
      demo.population_no != null && demo.population_no !== ''
        ? Number(demo.population_no)
        : null,
    median_age:
      demo.median_age != null && demo.median_age !== ''
        ? Number(demo.median_age)
        : null,
    median_income_households:
      demo.median_income_households != null && demo.median_income_households !== ''
        ? Number(demo.median_income_households)
        : null,
    median_income_families:
      demo.median_income_families != null && demo.median_income_families !== ''
        ? Number(demo.median_income_families)
        : null,
    male_percentage:
      demo.male_percentage != null && demo.male_percentage !== ''
        ? Number(demo.male_percentage)
        : null,
    female_percentage:
      demo.female_percentage != null && demo.female_percentage !== ''
        ? Number(demo.female_percentage)
        : null,
  };

  console.log('ℹ️ [DEMOS] Step 4 params:', JSON.stringify(updateParams, null, 2));

  try {
    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        SET
          population_no = @population_no,
          median_age = @median_age,
          median_income_households = @median_income_households,
          median_income_families = @median_income_families,
          male_percentage = @male_percentage,
          female_percentage = @female_percentage,
          status = 'completed'
        WHERE jobId = @jobId
      `,
      params: {
        jobId,
        population_no: updateParams.population_no,
        median_age: updateParams.median_age,
        median_income_households: updateParams.median_income_households,
        median_income_families: updateParams.median_income_families,
        male_percentage: updateParams.male_percentage,
        female_percentage: updateParams.female_percentage,
      },
    });

    console.log(
      `✅ [DEMOS] Updated jobs_demographics row for job ${jobId} with demographics data`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error in Step 4 updating jobs_demographics for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
    // Mark as error so job doesn’t stay pending forever
    try {
      await bigquery.query({
        query: `
          UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
          SET status = 'error'
          WHERE jobId = @jobId
        `,
        params: { jobId },
      });
      await bigquery.query({
        query: `
          UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
          SET demographicsStatus = 'error'
          WHERE jobId = @jobId
        `,
        params: { jobId },
      });
    } catch (markErr) {
      console.error(
        '⚠️ [DEMOS] Failed to mark job as error after Step 4 failure:',
        JSON.stringify(markErr.errors || markErr, null, 2)
      );
    }
    return; // stop further processing for this job
  }

  // ---- Step 5: Update job's demographicsStatus in main table ----
  console.log(
    '➡️ [DEMOS] Step 5: Update client_audits_jobs.demographicsStatus'
  );

  try {
    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET demographicsStatus = 'completed'
        WHERE jobId = @jobId
      `,
      params: { jobId },
    });

    console.log(
      `✅ [DEMOS] Marked demographicsStatus = completed for job ${jobId}`
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
      params: { jobId },
    });

    console.log(
      `ℹ️ [DEMOS] Step 6 checked for full completion for job ${jobId}.`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] Error in Step 6 updating main status for job ${jobId}:`,
      JSON.stringify(err.errors || err, null, 2)
    );
    // Not fatal, just log it.
  }


// ---------- START SERVER ----------
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Worker service listening on port ${port}`);
});
