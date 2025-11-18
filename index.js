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

// ---------- HEALTH CHECK ----------
app.get('/', (req, res) => {
  res.send('Middle-layer API is running');
});

// === POST /jobs - submit a new job (CREATE ONLY) ===
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

    // section statuses (initial state)
    demographicsStatus: 'queued',
    paidAdsStatus: 'queued',

    createdAt: new Date().toISOString(), // STRING or TIMESTAMP-compatible
  };

  try {
    await bigquery.dataset(DATASET_ID).table(JOBS_TABLE_ID).insert([row]);
    console.log(`✅ Job inserted successfully: ${jobId}`);

    // No workers, no fake status changes here

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

// === GET /status?jobId= - check job status (from jobs table only) ===
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

// === Start server ===
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Middle-layer API running on port ${port}`);
});
