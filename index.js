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

const datasetId = 'Client_audits';
const jobsTableId = 'client_audits_jobs';
const jobsDemographicsTableId = 'jobs_demographics';
const demographicsSourceTableId = '1_demographics';

// === POST /jobs - submit a new job ===
app.post('/jobs', async (req, res) => {
  const jobId = uuidv4();
  const { user = {}, business = {}, revenue, budget, services, location } = req.body;

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
    revenue: revenue ?? null,
    budget: budget ?? null,
    location: location || null,

    // overall job status
    status: 'queued',

    // section / report-part statuses
    demographicsStatus: 'queued',
    paidAdsStatus: 'queued', // not implemented yet, just reserved

    createdAt: new Date().toISOString(),
  };

  try {
    console.log('▶️ Inserting new job row:', row);
    await bigquery.dataset(datasetId).table(jobsTableId).insert([row]);
    console.log(`✅ Job inserted successfully: ${jobId}`);

    // Kick off demographics processing (do not await if you want fully async)
    processDemographics(jobId, location).catch(err => {
      console.error(`❌ processDemographics crash for jobId=${jobId}:`, err);
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
    query: `
      SELECT status, demographicsStatus, paidAdsStatus
      FROM \`${bigquery.projectId}.${datasetId}.${jobsTableId}\`
      WHERE jobId = @jobId
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
    FROM \`${bigquery.projectId}.${datasetId}.${jobsTableId}\`
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
      DELETE FROM \`${bigquery.projectId}.${datasetId}.${jobsTableId}\`
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

// === Process Demographics for a job ===
async function processDemographics(jobId, location) {
  console.log(`▶️ processDemographics start for jobId=${jobId}, location="${location}"`);

  if (!location) {
    console.warn(`processDemographics: jobId=${jobId} has no location, marking as no_data`);
    await setDemographicsStatus(jobId, 'no_data');
    await updateOverallStatus(jobId);
    return;
  }

  try {
    // 1) Fetch demographic row from 1_demographics
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
        FROM \`${bigquery.projectId}.${datasetId}.${demographicsSourceTableId}\`
        WHERE location = @location
        LIMIT 1
      `,
      params: { location },
    });

    console.log('processDemographics: demographics query result:', demoRows);

    if (!demoRows.length) {
      console.warn(`processDemographics: no demographics found for location="${location}"`);
      await setDemographicsStatus(jobId, 'no_data');
      await updateOverallStatus(jobId);
      return;
    }

    const demo = demoRows[0];

    // 2) Insert into jobs_demographics
    const demographicsRow = {
      jobId,
      location,
      population_no: demo.population_no,
      median_age: demo.median_age,
      households_no: demo.households_no,
      median_income_households: demo.median_income_households,
      median_income_families: demo.median_income_families,
      male_percentage: demo.male_percentage,
      female_percentage: demo.female_percentage,
      createdAt: new Date().toISOString(),
    };

    console.log('processDemographics: inserting into jobs_demographics:', demographicsRow);

    await bigquery
      .dataset(datasetId)
      .table(jobsDemographicsTableId)
      .insert([demographicsRow]);

    console.log(`✅ processDemographics: inserted demographics for jobId=${jobId}`);

    // 3) Mark demographicsStatus as completed
    await setDemographicsStatus(jobId, 'completed');
    await updateOverallStatus(jobId);
  } catch (err) {
    console.error(`❌ processDemographics error for jobId=${jobId}:`, err);
    // Mark as failed but don't crash the server
    await setDemographicsStatus(jobId, 'failed').catch(e =>
      console.error('Failed to set demographicsStatus=failed', e)
    );
    await updateOverallStatus(jobId);
  }
}

// Helper: update demographicsStatus in jobs table
async function setDemographicsStatus(jobId, status) {
  console.log(`setDemographicsStatus: jobId=${jobId} -> ${status}`);
  await bigquery.query({
    query: `
      UPDATE \`${bigquery.projectId}.${datasetId}.${jobsTableId}\`
      SET demographicsStatus = @status
      WHERE jobId = @jobId
    `,
    params: { jobId, status },
  });
}

// === Update overall job status based on segment statuses ===
async function updateOverallStatus(jobId) {
  try {
    const [rows] = await bigquery.query({
      query: `
        SELECT demographicsStatus, paidAdsStatus
        FROM \`${bigquery.projectId}.${datasetId}.${jobsTableId}\`
        WHERE jobId = @jobId
        LIMIT 1
      `,
      params: { jobId },
    });

    if (!rows.length) {
      console.warn(`updateOverallStatus: jobId=${jobId} not found`);
      return;
    }

    const { demographicsStatus, paidAdsStatus } = rows[0];
    console.log(
      `updateOverallStatus: jobId=${jobId} current demo=${demographicsStatus}, paidAds=${paidAdsStatus}`
    );

    let newStatus = 'queued';

    if (demographicsStatus === 'failed' || paidAdsStatus === 'failed') {
      newStatus = 'failed';
    } else if (
      (demographicsStatus === 'completed' || demographicsStatus === 'no_data') &&
      (!paidAdsStatus || paidAdsStatus === 'queued')
    ) {
      // For now, treat demographics completion (or no_data) as enough to complete the job.
      newStatus = 'completed';
    } else if (
      demographicsStatus === 'in_progress' ||
      paidAdsStatus === 'in_progress' ||
      demographicsStatus === 'queued' ||
      paidAdsStatus === 'queued'
    ) {
      newStatus = 'in_progress';
    }

    console.log(`updateOverallStatus: jobId=${jobId} -> new status=${newStatus}`);

    await bigquery.query({
      query: `
        UPDATE \`${bigquery.projectId}.${datasetId}.${jobsTableId}\`
        SET status = @status
        WHERE jobId = @jobId
      `,
      params: { jobId, status: newStatus },
    });
  } catch (err) {
    console.error(`❌ updateOverallStatus error for jobId=${jobId}:`, err);
  }
}

// === Start server ===
const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`Middle-layer API running on port ${port}`));
