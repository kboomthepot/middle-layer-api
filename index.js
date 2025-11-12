const express = require('express');
const bodyParser = require('body-parser');
const { BigQuery } = require('@google-cloud/bigquery');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(bodyParser.json());

// === BigQuery setup ===
const bigquery = new BigQuery({
  projectId: 'ghs-construction-1734441714520', // your GCP project ID
  // keyFilename: './service-account.json', // only if testing locally
});

const datasetId = 'Client_audits';           // dataset name
const tableId = 'Client Audits - Jobs';      // table name

// === POST /jobs - submit a new job ===
app.post('/jobs', async (req, res) => {
  const jobId = uuidv4();
  const { user, business, revenue, budget, services, location } = req.body;

  if (!user || !business) {
    return res.status(400).json({ error: 'User and business info are required' });
  }

  const row = {
    jobId,
    userName: user.name,
    email: user.email,
    phone: user.phone,
    businessName: business.name,
    website: business.website,
    services: JSON.stringify(services || []),
    revenue: revenue || 0,
    budget: budget || 0,
    location: location || '',
    status: 'queued',
    createdAt: new Date().toISOString()
  };

  try {
    await bigquery.dataset(datasetId).table(tableId).insert([row]);
    console.log(`Inserted job ${jobId}`);

    // Simulate report processing
    simulateReport(jobId);

    res.json({ jobId, status: 'queued' });
  } catch (err) {
    console.error('Failed to insert job:', err);
    res.status(500).json({ error: 'Failed to insert job' });
  }
});

// === GET /status?jobId= - check job status ===
app.get('/status', async (req, res) => {
  const { jobId } = req.query;
  if (!jobId) return res.status(400).json({ error: 'jobId is required' });

  const query = {
    query: `SELECT status FROM \`${bigquery.projectId}.${datasetId}.${tableId}\` WHERE jobId=@jobId`,
    params: { jobId }
  };

  try {
    const [rows] = await bigquery.query(query);
    if (!rows.length) return res.status(404).json({ error: 'Job not found' });
    res.json({ jobId, status: rows[0].status });
  } catch (err) {
    console.error('Failed to fetch job status:', err);
    res.status(500).json({ error: 'Failed to fetch status' });
  }
});

// === GET /jobs - list all jobs (dashboard) ===
app.get('/jobs', async (req, res) => {
  const query = `SELECT * FROM \`${bigquery.projectId}.${datasetId}.${tableId}\` ORDER BY createdAt DESC`;

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
    query: `DELETE FROM \`${bigquery.projectId}.${datasetId}.${tableId}\` WHERE jobId=@jobId`,
    params: { jobId }
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

// === Simulate report processing ===
async function simulateReport(jobId) {
  const statuses = ['in_progress', 'completed'];
  const delay = 3000; // 3 seconds between updates

  for (const status of statuses) {
    await new Promise(resolve => setTimeout(resolve, delay));
    const query = {
      query: `UPDATE \`${bigquery.projectId}.${datasetId}.${tableId}\` SET status=@status WHERE jobId=@jobId`,
      params: { jobId, status }
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
