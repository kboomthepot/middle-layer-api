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
const JOB_EVENTS_TOPIC = 'client-audits-job-events';

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
    `📨 Published job ${payload.jobId} (stage=${payload.stage}) to Pub/Sub topic "${JOB_EVENTS_TOPIC}" with messageId=${messageId}`
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
  const initialStatus = 'queued';

  // Prepare values (note: we store services as JSON string)
  const row = {
    jobId,
    firstName: user.firstName || null,
    lastName: user.lastName || null,
    email: user.email || null,
    phone: user.phone || null,
    businessName: business.name || null,
    website: business.website || null,
    services: JSON.stringify(services || []),
    revenue,
    budget,
    location,
    status: initialStatus,

    // Segment statuses (1–10)
    s1: initialStatus,  // 1_demographics_Status
    s2: initialStatus,  // 2_industryStats_Status
    s3: initialStatus,  // 3_leadChannelRanking_Status
    s4: initialStatus,  // 4_marketStats_Status
    s5: initialStatus,  // 5_keywords_Status
    s6: initialStatus,  // 6_seasonality_Status
    s7: initialStatus,  // 7_organicSearch_Status
    s8: initialStatus,  // 8_paidAds_Status
    s9: initialStatus,  // 9_clientInput_Status
    s10: initialStatus, // 10_summary_Status

    createdAt,
  };

  try {
    const insertQuery = `
      INSERT \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        (jobId,
         createdAt,
         status,
         businessName,
         firstName,
         lastName,
         email,
         phone,
         website,
         services,
         revenue,
         budget,
         location,
         \`1_demographics_Status\`,
         \`2_industryStats_Status\`,
         \`3_leadChannelRanking_Status\`,
         \`4_marketStats_Status\`,
         \`5_keywords_Status\`,
         \`6_seasonality_Status\`,
         \`7_organicSearch_Status\`,
         \`8_paidAds_Status\`,
         \`9_clientInput_Status\`,
         \`10_summary_Status\`
        )
      VALUES
        (@jobId,
         @createdAt,
         @status,
         @businessName,
         @firstName,
         @lastName,
         @email,
         @phone,
         @website,
         @services,
         @revenue,
         @budget,
         @location,
         @s1,
         @s2,
         @s3,
         @s4,
         @s5,
         @s6,
         @s7,
         @s8,
         @s9,
         @s10
        )
    `;

    await bigquery.query({
      query: insertQuery,
      params: {
        jobId: row.jobId,
        createdAt: row.createdAt,
        status: row.status,
        businessName: row.businessName,
        firstName: row.firstName,
        lastName: row.lastName,
        email: row.email,
        phone: row.phone,
        website: row.website,
        services: row.services,
        revenue: row.revenue,
        budget: row.budget,
        location: row.location,
        s1: row.s1,
        s2: row.s2,
        s3: row.s3,
        s4: row.s4,
        s5: row.s5,
        s6: row.s6,
        s7: row.s7,
        s8: row.s8,
        s9: row.s9,
        s10: row.s10,
      },
    });

    console.log(`✅ Job inserted successfully (DML): ${jobId}`);

    // publish events to worker for multiple stages
    await publishJobEvent({
      jobId,
      location,
      createdAt,
      stage: 'demographics',
    });

    await publishJobEvent({
      jobId,
      location,
      createdAt,
      stage: '7_organicSearch',
    });

    res.json({
      jobId,
      status: row.status,
      '1_demographics_Status': row.s1,
      '7_organicSearch_Status': row.s7,
      '8_paidAds_Status': row.s8,
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
      SELECT
        status,
        \`1_demographics_Status\` AS demographicsStatus,
        \`7_organicSearch_Status\` AS organicSearchStatus,
        \`8_paidAds_Status\` AS paidAdsStatus
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
      organicSearchStatus: row.organicSearchStatus,
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
