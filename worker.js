// worker.js

const express = require('express');
const bodyParser = require('body-parser');
const { BigQuery } = require('@google-cloud/bigquery');

// Segment handlers
const { handleDemographicsSegment } = require('./demographics');
const {
  handleOrganicSearchSegment,
  handleOrganicResultCallback,
} = require('./organicSearch');

// Status helpers (used by segments, but fine to load here too)
const { markSegmentStatus } = require('./status');

const bigquery = new BigQuery();

const PROJECT_ID = 'ghs-construction-1734441714520';
const JOBS_TABLE = `${PROJECT_ID}.Client_audits.client_audits_jobs`;

const app = express();
app.use(bodyParser.json());

/**
 * Load a single job row from client_audits_jobs.
 * If you already have loadJob in jobHelpers.js, you can import it instead.
 */
async function loadJob(jobId) {
  const [rows] = await bigquery.query({
    query: `
      SELECT
        jobId,
        location,
        businessName,
        services,
        createdAt,
        \`1_demographics_Status\` AS demographicsStatus,
        \`7_organicSearch_Status\` AS organicSearchStatus,
        \`8_paidAds_Status\` AS paidAdsStatus,
        status
      FROM \`${JOBS_TABLE}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId },
  });

  return rows;
}

/**
 * Internal helper: decide which segment handler to call for a Pub/Sub payload.
 */
async function handlePubSubPayload(payload) {
  const { jobId, location, stage } = payload;

  console.log('📩 Received job message:', payload);

  if (!jobId || !stage) {
    throw new Error('Missing jobId or stage in Pub/Sub payload');
  }

  const rows = await loadJob(jobId);
  if (!rows || rows.length === 0) {
    console.warn(`⚠️ No job row found in ${JOBS_TABLE} for jobId=${jobId}`);
    return;
  }

  const jobRow = rows[0];

  console.log(
    `✅ Worker received job ${jobId} (stage=${stage}, location=${location})`
  );

  if (stage === 'demographics') {
    // 1_demographics segment
    await handleDemographicsSegment(jobRow);
  } else if (stage === '7_organicSearch') {
    // 7_organicSearch segment
    await handleOrganicSearchSegment(jobRow);
  } else {
    console.log(`ℹ️ Unknown stage "${stage}" - nothing to do yet.`);
  }
}

/**
 * Pub/Sub push endpoint – receives messages with stage + jobId
 */
app.post('/', async (req, res) => {
  try {
    const message = req.body && req.body.message;
    if (!message || !message.data) {
      console.error('❌ Invalid Pub/Sub message format', req.body);
      res.status(400).send('Bad Request: no message.data');
      return;
    }

    const dataBuffer = Buffer.from(message.data, 'base64');
    const payload = JSON.parse(dataBuffer.toString('utf8'));

    await handlePubSubPayload(payload);

    // Pub/Sub expects 2xx. 204 is standard for push acks.
    res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    // Let Pub/Sub retry by returning 500
    res.status(500).send();
  }
});

/**
 * Callback endpoint for organic search results from n8n
 *
 * organicSearch.js already exposes `handleOrganicResultCallback(req, res)`
 * so we just plug it directly here.
 */
app.post('/organic-result', handleOrganicResultCallback);

/**
 * Simple GET health check for Cloud Run.
 */
app.get('/', (req, res) => {
  res.status(200).send('OK');
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`🚀 client-audits-worker listening on port ${PORT}`);
});
