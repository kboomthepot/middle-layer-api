// worker.js
const express = require('express');
const bodyParser = require('body-parser');
const { BigQuery } = require('@google-cloud/bigquery');

const app = express();
app.use(bodyParser.json());

const PROJECT_ID = 'ghs-construction-1734441714520';

// Main jobs + jobs_demographics live here
const JOBS_DATASET_ID = 'Client_audits';
const JOBS_TABLE_ID = 'client_audits_jobs';
const JOBS_DEMOGRAPHICS_TABLE_ID = 'jobs_demographics';

// Demographics *source* table lives in a different dataset
const DEMOGRAPHICS_DATASET_ID = 'Client_audits_data';
const DEMOGRAPHICS_SOURCE_TABLE_ID = '1_demographics';

const bigquery = new BigQuery({ projectId: PROJECT_ID });

async function processJobDemographics(jobId) {
  console.log(`▶️ [DEMOS] Starting demographics processing for job ${jobId}`);

  try {
    // 1) Load job row
    console.log('➡️ [DEMOS] Step 1: Load job row from client_audits_jobs');
    const [jobRows] = await bigquery.query({
      query: `
        SELECT jobId, location, paidAdsStatus, demographicsStatus, status
        FROM \`${PROJECT_ID}.${JOBS_DATASET_ID}.${JOBS_TABLE_ID}\`
        WHERE jobId = @jobId
        LIMIT 1
      `,
      params: { jobId },
    });

    console.log(`ℹ️ [DEMOS] Step 1 result rows: ${jobRows.length}`);
    if (!jobRows.length) {
      console.warn(`⚠️ [DEMOS] No job found with jobId=${jobId}.`);
      return { processed: false, reason: 'job_not_found' };
    }

    const job = jobRows[0];
    const location = job.location;
    const paidAdsStatus = job.paidAdsStatus || 'queued';

    console.log(`ℹ️ [DEMOS] Job ${jobId} location = "${location}", paidAdsStatus = "${paidAdsStatus}"`);

    if (!location) {
      console.warn(`⚠️ [DEMOS] Job ${jobId} has no location; skipping demographics.`);
      return { processed: false, reason: 'no_location' };
    }

	// 2) Mark demographics as pending in jobs_demographics

	console.log('➡️ [DEMOS] Step 2: Upsert pending row into jobs_demographics');

	// 2a) Check if a row already exists for this jobId
	const [existingRows] = await bigquery.query({
	  query: `
		SELECT jobId
		FROM \`${PROJECT_ID}.${JOBS_DATASET_ID}.${JOBS_DEMOGRAPHICS_TABLE_ID}\`
		WHERE jobId = @jobId
		LIMIT 1
	  `,
	  params: { jobId },
	});

	if (existingRows.length) {
	  // 2b) Row exists → just update it to pending and refresh location
	  console.log(`ℹ️ [DEMOS] jobs_demographics row already exists for job ${jobId}, updating to pending.`);
	  await bigquery.query({
		query: `
		  UPDATE \`${PROJECT_ID}.${JOBS_DATASET_ID}.${JOBS_DEMOGRAPHICS_TABLE_ID}\`
		  SET
			status = 'pending',
			location = @location
		  WHERE jobId = @jobId
		`,
		params: { jobId, location },
	  });
	} else {
	  // 2c) No row yet → insert a new pending row
	  console.log(`ℹ️ [DEMOS] No jobs_demographics row for job ${jobId}, inserting pending row.`);
	  const pendingRow = {
		jobId,
		status: 'pending',
		location,
		population_no: null,
		median_age: null,
		households_no: null,
		median_income_households: null,
		median_income_families: null,
		male_percentage: null,
		female_percentage: null,
		createdAt: new Date().toISOString(),
	  };

	  await bigquery
		.dataset(JOBS_DATASET_ID)
		.table(JOBS_DEMOGRAPHICS_TABLE_ID)
		.insert([pendingRow]);
	}

	console.log(`⏳ [DEMOS] Marked demographics as pending for job ${jobId}`);


    // 3) Fetch demographics row
    console.log('➡️ [DEMOS] Step 3: Fetch demographics from Client_audits_data.1_demographics');
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
        FROM \`${PROJECT_ID}.${DEMOGRAPHICS_DATASET_ID}.${DEMOGRAPHICS_SOURCE_TABLE_ID}\`
        WHERE location = @location
        LIMIT 1
      `,
      params: { location },
    });

    console.log(`ℹ️ [DEMOS] Step 3 result rows: ${demoRows.length}`);
    if (!demoRows.length) {
      console.warn(`⚠️ [DEMOS] No demographics found for location="${location}" (jobId=${jobId}).`);
      // We leave as pending so you can see it's unresolved.
      return { processed: false, reason: 'no_demographics' };
    }

    const d = demoRows[0];

    // 4) Update jobs_demographics with real data + completed status
    console.log('➡️ [DEMOS] Step 4: Update jobs_demographics with final values');
    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${JOBS_DATASET_ID}.${JOBS_DEMOGRAPHICS_TABLE_ID}\`
        SET
          status = 'completed',
          population_no = @population_no,
          median_age = @median_age,
          households_no = @households_no,
          median_income_households = @median_income_households,
          median_income_families = @median_income_families,
          male_percentage = @male_percentage,
          female_percentage = @female_percentage
        WHERE jobId = @jobId
      `,
      params: {
        jobId,
        population_no: d.population_no ?? null,
        median_age: d.median_age ?? null,
        households_no: d.households_no ?? null,
        median_income_households: d.median_income_households ?? null,
        median_income_families: d.median_income_families ?? null,
        male_percentage: d.male_percentage ?? null,
        female_percentage: d.female_percentage ?? null,
      },
    });

    console.log(`✅ [DEMOS] Copied demographics into jobs_demographics for job ${jobId}`);

    // 5) Update statuses in client_audits_jobs
    console.log('➡️ [DEMOS] Step 5: Update statuses in client_audits_jobs');
    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${JOBS_DATASET_ID}.${JOBS_TABLE_ID}\`
        SET
          demographicsStatus = 'completed',
          status = CASE
            WHEN paidAdsStatus = 'completed' THEN 'completed'
            WHEN status = 'queued' THEN 'in_progress'
            ELSE status
          END
        WHERE jobId = @jobId
      `,
      params: { jobId },
    });

    console.log(`✅ [DEMOS] Updated job ${jobId} statuses after demographics processing`);

    return { processed: true, reason: 'ok' };
  } catch (err) {
    console.error(`❌ [DEMOS] Error in processJobDemographics for job ${jobId}:`, err);
    // Rethrow so outer handler can decide whether to retry (500) or not.
    throw err;
  }
}

// Pub/Sub push endpoint
app.post('/', async (req, res) => {
  try {
    const pubsubMessage = req.body?.message;
    if (!pubsubMessage || !pubsubMessage.data) {
      console.error('⚠️ No message data received', req.body);
      return res.status(400).send('Bad Request: no message data');
    }

    const payloadJson = JSON.parse(
      Buffer.from(pubsubMessage.data, 'base64').toString('utf8')
    );

    const { jobId } = payloadJson;
    console.log('📩 Received job message:', payloadJson);

    if (!jobId) {
      console.error('⚠️ Missing jobId in message payload');
      return res.status(400).send('Bad Request: missing jobId');
    }

    const result = await processJobDemographics(jobId);
    console.log(`ℹ️ [DEMOS] Demographics processing summary for ${jobId}:`, result);

    return res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    return res.status(500).send('Internal Server Error');
  }
});

// Health endpoint
app.get('/', (req, res) => {
  res.send('Worker service is running');
});

const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Worker service listening on port ${port}`);
});
