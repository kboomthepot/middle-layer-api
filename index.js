const express = require('express');
const bodyParser = require('body-parser');
const { BigQuery } = require('@google-cloud/bigquery');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(bodyParser.json());

// === BigQuery setup ===
const PROJECT_ID = 'ghs-construction-1734441714520';

const bigquery = new BigQuery({
  projectId: PROJECT_ID,
  // keyFilename: './service-account.json', // only if testing locally
});

// === Dataset & table IDs ===
const DATASET_ID = 'Client_audits';
const JOBS_TABLE_ID = 'client_audits_jobs';
const DEMOGRAPHICS_SOURCE_TABLE_ID = '1_demographics';
const JOBS_DEMOGRAPHICS_TABLE_ID = 'jobs_demographics';

// === POST /jobs - submit a new job ===
app.post('/jobs', async (req, res) => {
  const jobId = uuidv4();
  const { user, business, revenue, budget, services, location } = req.body;

  const row = {
    jobId,

    // user fields
    firstName: user.firstName || null,
    lastName: user.lastName || null,
    email: user.email,
    phone: user.phone,

    // business fields
    businessName: business.name,
    website: business.website,

    // job context
    services: JSON.stringify(services || []),
    revenue,
    budget,
    location,

    // overall job status
    status: 'queued',

    // section / report-part statuses
    demographicsStatus: 'queued', // will be set to 'completed' by processDemographics
    paidAdsStatus: 'queued',

    createdAt: new Date().toISOString(),
  };

  try {
    await bigquery.dataset(DATASET_ID).table(JOBS_TABLE_ID).insert([row]);
    console.log(`✅ Job inserted successfully: ${jobId}`);

    // Simulated global status updates (keep for now)
    simulateReport(jobId);

    // 🔹 Start demographics processing in background
    processDemographics(jobId).catch((err) => {
      console.error(`❌ Demographics processing failed for job ${jobId}:`, err);
    });

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
    query: `SELECT status, demographicsStatus, paidAdsStatus
            FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
            WHERE jobId=@jobId`,
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
    FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
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
    query: `DELETE FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
            WHERE jobId=@jobId`,
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

// === Demographics processing ===
async function processDemographics(jobId) {
  console.log(`🚀 Starting demographics processing for job ${jobId}`);

  // 1) Get job location from client_audits_jobs
  const [jobRows] = await bigquery.query({
    query: `SELECT location
            FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
            WHERE jobId=@jobId`,
    params: { jobId },
  });

  if (!jobRows.length) {
    console.warn(`⚠️ No job found for jobId ${jobId} when processing demographics`);
    return;
  }

  const location = jobRows[0].location;
  console.log(`ℹ️ Job ${jobId} location: ${location}`);

  // 2) Fetch demographics for that location from 1_demographics
  const [demoRows] = await bigquery.query({
    query: `SELECT
              location,
              population_no,
              median_age,
              households_no,
              median_income_households,
              median_income_families,
              male_percentage,
              female_percentage
            FROM \`${PROJECT_ID}.${DATASET_ID}.${DEMOGRAPHICS_SOURCE_TABLE_ID}\`
            WHERE location=@location`,
    params: { location },
  });

  if (!demoRows.length) {
    console.warn(`⚠️ No demographics found for location "${location}"`);
    // Mark demographicsStatus as no_data so frontend can show it
    await bigquery.query({
      query: `UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
              SET demographicsStatus=@status
              WHERE jobId=@jobId`,
      params: { jobId, status: 'no_data' },
    });
    return;
  }

  const demo = demoRows[0];

  // 3) Insert into jobs_demographics table
  const demographicsRow = {
    jobId,
    status: 'completed',
    location: demo.location,
    population_no: demo.population_no,
    median_age: demo.median_age,
    households_no: demo.households_no,
    median_income_households: demo.median_income_households,
    median_income_families: demo.median_income_families,
    male_percentage: demo.male_percentage,
    female_percentage: demo.female_percentage,
  };

  await bigquery
    .dataset(DATASET_ID)
    .table(JOBS_DEMOGRAPHICS_TABLE_ID)
    .insert([demographicsRow]);

  console.log(`✅ Demographics row inserted for job ${jobId}`);

  // 4) Update demographicsStatus in client_audits_jobs
  await bigquery.query({
    query: `UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
            SET demographicsStatus=@status
            WHERE jobId=@jobId`,
    params: { jobId, status: 'completed' },
  });

  console.log(`✅ demographicsStatus set to 'completed' for job ${jobId}`);
}

// === Simulate report processing (overall status only, for now) ===
async function simulateReport(jobId) {
  const statuses = ['in_progress', 'completed'];
  const delay = 3000; // 3 seconds between updates

  for (const status of statuses) {
    await new Promise((resolve) => setTimeout(resolve, delay));
    const query = {
      query: `UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
              SET status=@status
              WHERE jobId=@jobId`,
      params: { jobId, status },
    };

    try {
      await bigquery.query(query);
      console.log(`Job ${jobId} status updated to ${status}`);
    } catch (err) {
      console.error(`Failed to update job status for ${jobId}:`, err);
    }
  }
}

// === Start server ===
const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`Middle-layer API running on port ${port}`));
