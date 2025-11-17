app.post('/jobs', async (req, res) => {
  // provide safe defaults so we don't crash if some fields are missing
  const {
    user = {},
    business = {},
    revenue = null,
    budget = null,
    services = [],
    location = null,
  } = req.body;

  const jobId = uuidv4();

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

    // section / report-part statuses
    demographicsStatus: 'queued',
    paidAdsStatus: 'queued',

    createdAt: new Date().toISOString(),
  };

  try {
    await bigquery.dataset(DATASET_ID).table(JOBS_TABLE_ID).insert([row]);
    console.log(`✅ Job inserted successfully: ${jobId}`);

    // no direct processing here
    // processDemographics(jobId, location);
    // simulateReport(jobId);

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

const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Middle-layer API running on port ${port}`);
});

