// index.js
const express = require('express');
const bodyParser = require('body-parser');
const { BigQuery } = require('@google-cloud/bigquery');
const { PubSub } = require('@google-cloud/pubsub');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(bodyParser.json());

// === GCP setup ===
const PROJECT_ID = 'ghs-construction-1734441714520';

// BigQuery client
const bigquery = new BigQuery({ projectId: PROJECT_ID });

// Dataset + main jobs table
const DATASET_ID = 'Client_audits';
const JOBS_TABLE_ID = 'client_audits_jobs';

// Pub/Sub client
const pubsub = new PubSub({ projectId: PROJECT_ID });
const JOB_EVENTS_TOPIC = 'client-audit-job';

// ---------- HEALTH CHECK ----------
app.get('/', (req, res) => {
  res.send('Middle-layer API is running');
});

// ---------- Publish job event to Pub/Sub ----------
async function publishJobEvent(payload) {
  const topic = pubsub.topic(JOB_EVENTS_TOPIC);
  const dataBuffer = Buffer.from(JSON.stringify(payload));

  const messageId = await topic.publishMessage({ data: dataBuffer });
  console.log(
    `📨 Published job ${payload.jobId} to Pub/Sub topic "${JOB_EVENTS_TOPIC}" with messageId=${messageId}`
  );
}

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

  const createdAt = new Date().toISOString();

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

    createdAt,
  };

  try {
    // DML INSERT to avoid streaming buffer
    const insertQuery = `
      INSERT \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        (jobId, firstName, lastName, email, phone,
         businessName, website, services,
         revenue, budget, location,
         status, demographicsStatus, paidAdsStatus, createdAt)
      VALUES
        (@jobId, @firstName, @lastName, @email, @phone,
         @businessName, @website, @services,
         @revenue, @budget, @location,
         @status, @demographicsStatus, @paidAdsStatus, @createdAt)
    `;

    await bigquery.query({
      query: insertQuery,
      params: {
        jobId,
        firstName: row.firstName,
        lastName: row.lastName,
        email: row.email,
        phone: row.phone,
        businessName: row.businessName,
        website: row.website,
        services: row.services,
        revenue: row.revenue,
        budget: row.budget,
        location: row.location,
        status: row.status,
        demographicsStatus: row.demographicsStatus,
        paidAdsStatus: row.paidAdsStatus,
        createdAt: row.createdAt,
      },
    });

    console.log(`✅ Job inserted successfully (DML): ${jobId}`);

    // publish event to worker
    await publishJobEvent({ jobId, location, createdAt });

    res.json({
      jobId,
      status: 'queued',
      demographicsStatus: 'queued',
      paidAdsStatus: 'queued',
    });
  } catch (err) {
    console.error('❌ BigQuery Insert Error (DML):', err);
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
      FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
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
    query: `
      DELETE FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
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
