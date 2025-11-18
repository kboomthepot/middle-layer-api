// index.js
const express = require('express');
const bodyParser = require('body-parser');
const { BigQuery } = require('@google-cloud/bigquery');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(bodyParser.json());

// === BigQuery setup ===
const bigquery = new BigQuery({
  projectId: 'ghs-construction-1734441714520',
});

// Dataset + main jobs table
const DATASET_ID = 'Client_audits';
const JOBS_TABLE_ID = 'client_audits_jobs';

// Other tables
const DEMOGRAPHICS_SOURCE_TABLE_ID = '1_demographics';
const JOBS_DEMOGRAPHICS_TABLE_ID = 'jobs_demographics';

// ---------- HEALTH CHECK ----------
app.get('/', (req, res) => {
  res.send('Middle-layer API is running');
});

// === POST /jobs - submit a new job ===
app.post('/jobs', async (req, res) => {
  const jobId = uuidv4();
  const {
    user = {},
    business = {},
    revenue = null,
    budget = null,
    services = [],
    location = null,
  } = req.body;

  const row = {
    jobId,

    // user fields
    firstName: user.firstName || null,
    lastName: user.lastName || null,
    email: user.email || null,
    phone: user.phone || null,

    // business fields
    businessName: business.name || null,
    website: business.website || null,

    // job context
    services: JSON.stringify(services || []),
    revenue,
    budget,
    location,

    // overall job status
    status: 'queued',

    // section statuses
    demographicsStatus: 'queued',
    paidAdsStatus: 'queued',

    createdAt: new Date().toISOString(), // STRING or TIMESTAMP-compatible
  };

  try {
    await bigquery.dataset(DATASET_ID).table(JOBS_TABLE_ID).insert([row]);
    console.log(`✅ Job inserted successfully: ${jobId}`);

    // ⛔ Removed fake progression: no more simulateReport(jobId)

    res.json({
      jobId,
      status: 'queued',
      demographicsStatus: 'queued',
      paidAdsStatus: 'queued',
    });
  } catch (err) {
    console.error('❌ BigQuery Insert Error:', err);
    const message = err.errors ? JSON.stringify(err.errors) : err.message;
    res.status(500).json({ error: 'Failed to insert job', details: message });
  }
});

// === GET /status?jobId= - check job status ===
app.get('/status', async (req, res) => {
  const { jobId } = req.query;
  if (!jobId) return res.status(400).json({ error: 'jobId is required' });

  const query = {
    query: `
      SELECT status, demographicsStatus, paidAdsStatus
      FROM \`${bigquery.projectId}.${DATASET_ID}.${JOBS_TABLE_ID}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId },
  };

  try {
    const [rows] = await bigquery.query(query);
    if (!rows.length) return res.status(404).json({ error: 'Job not found' });

    const row = rows[0];
    res.json({
      jobId,
      status: row.status,
      demographicsStatus: row.demographicsStatus,
      paidAdsStatus: row.paidAdsStatus,
    });
  } catch (err) {
    console.error('Failed to fetch job status:', err);
    res.status(500).json({ error: 'Failed to fetch status' });
  }
});

// === GET /jobs - list all jobs (dashboard) ===
app.get('/jobs', async (req, res) => {
  const query = `
    SELECT *
    FROM \`${bigquery.projectId}.${DATASET_ID}.${JOBS_TABLE_ID}\`
    ORDER BY createdAt DESC
  `;

  try {
    const [rows] = await bigquery.query({ query });
    res.json(rows);
  } catch (err) {
    console.error('Failed to fetch jobs:', err);
    res.status(500).json({ error: 'Failed to fetch jobs' });
  }
});

// === DELETE /jobs/:jobId - remove a job ===
app.delete('/jobs/:jobId', async (req, res) => {
  const jobId = req.params.jobId;
  if (!jobId) return res.status(400).json({ error: 'jobId is required' });

  const query = {
    query: `
      DELETE FROM \`${bigquery.projectId}.${DATASET_ID}.${JOBS_TABLE_ID}\`
      WHERE jobId = @jobId
    `,
    params: { jobId },
  };

  try {
    await bigquery.query(query);
    console.log(`Deleted job ${jobId}`);
    res.json({ message: 'Job deleted', jobId });
  } catch (err) {
    console.error('Failed to delete job:', err);
    res.status(500).json({ error: 'Failed to delete job' });
  }
});

// === DEMOGRAPHICS WORKER ===

// HTTP endpoint that scheduler or you can call to process queued demographics jobs
app.get('/run-demographics', async (req, res) => {
  try {
    const result = await runDemographics();
    res.json(result);
  } catch (err) {
    console.error('❌ /run-demographics error:', err);
    res.status(500).json({ error: 'Failed to run demographics worker', details: err.message });
  }
});

/**
 * Find all jobs with demographicsStatus = 'queued',
 * fetch demographics for each job.location from 1_demographics,
 * insert into jobs_demographics, and update statuses in client_audits_jobs.
 */
async function runDemographics() {
  console.log('▶️ Starting demographics worker...');

  // 1) Get all jobs that need demographics processing
  const jobsQuery = {
    query: `
      SELECT jobId, location, paidAdsStatus, status
      FROM \`${bigquery.projectId}.${DATASET_ID}.${JOBS_TABLE_ID}\`
      WHERE demographicsStatus = 'queued'
    `,
  };

  const [jobRows] = await bigquery.query(jobsQuery);

  if (!jobRows.length) {
    console.log('No jobs with demographicsStatus = "queued".');
    return { processedJobs: 0 };
  }

  console.log(`Found ${jobRows.length} job(s) with demographicsStatus = "queued".`);

  let processedCount = 0;
  let skippedNoLocation = 0;
  let skippedNoDemographics = 0;
  let errors = [];

  for (const job of jobRows) {
    const { jobId, location, paidAdsStatus } = job;

    if (!location) {
      console.warn(`Job ${jobId} has no location; skipping.`);
      skippedNoLocation++;
      continue;
    }

    try {
      // 2) Fetch demographics row from 1_demographics by location
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
          FROM \`${bigquery.projectId}.${DATASET_ID}.${DEMOGRAPHICS_SOURCE_TABLE_ID}\`
          WHERE location = @location
          LIMIT 1
        `,
        params: { location },
      });

      if (!demoRows.length) {
        console.warn(`No demographics found for location "${location}" (jobId: ${jobId}). Skipping.`);
        skippedNoDemographics++;
        continue;
      }

      const d = demoRows[0];

      // 3) Delete existing row for this jobId to keep idempotent
      await bigquery.query({
        query: `
          DELETE FROM \`${bigquery.projectId}.${DATASET_ID}.${JOBS_DEMOGRAPHICS_TABLE_ID}\`
          WHERE jobId = @jobId
        `,
        params: { jobId },
      });

      const demographicsRow = {
        jobId,
        status: 'completed', // demographics section status
        location,
        population_no: d.population_no ?? null,
        median_age: d.median_age ?? null,
        households_no: d.households_no ?? null,
        median_income_households: d.median_income_households ?? null,
        median_income_families: d.median_income_families ?? null,
        male_percentage: d.male_percentage ?? null,
        female_percentage: d.female_percentage ?? null,
        createdAt: new Date().toISOString(),
      };

      await bigquery
        .dataset(DATASET_ID)
        .table(JOBS_DEMOGRAPHICS_TABLE_ID)
        .insert([demographicsRow]);

      console.log(`✅ Inserted demographics for job ${jobId}`);

      // 4) Update demographicsStatus in client_audits_jobs
      await bigquery.query({
        query: `
          UPDATE \`${bigquery.projectId}.${DATASET_ID}.${JOBS_TABLE_ID}\`
          SET
            demographicsStatus = 'completed',
            status = CASE
              WHEN paidAdsStatus = 'completed' THEN 'completed'
              ELSE status
            END
          WHERE jobId = @jobId
        `,
        params: { jobId },
      });

      console.log(`✅ Updated demographicsStatus for job ${jobId}`);
      processedCount++;
    } catch (err) {
      console.error(`❌ Error processing job ${jobId}:`, err);
      errors.push({ jobId, message: err.message });
    }
  }

  const summary = {
    processedJobs: processedCount,
    skippedNoLocation,
    skippedNoDemographics,
    errorsCount: errors.length,
    errors,
  };

  console.log('Demographics worker summary:', summary);
  return summary;
}

// === Start server ===
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Middle-layer API running on port ${port}`);
});
