// worker.js
const express = require('express');
const bodyParser = require('body-parser');
const { BigQuery } = require('@google-cloud/bigquery');

const { handleDemographicsSegment } = require('./demographics');
const {
  handleOrganicSearchSegment,
  handleOrganicResultCallback,
} = require('./organicSearch');

const app = express();
app.use(bodyParser.json());

const bigquery = new BigQuery();

const PROJECT_ID = 'ghs-construction-1734441714520';
const JOBS_TABLE = `${PROJECT_ID}.Client_audits.client_audits_jobs`;

/**
 * Root Pub/Sub push endpoint
 * Receives messages with: { jobId, location, createdAt, stage }
 */
app.post('/', async (req, res) => {
  try {
    const message = req.body && req.body.message;
    if (!message || !message.data) {
      console.error('❌ Invalid Pub/Sub message format', req.body);
      // Still ack so Pub/Sub doesn’t hammer us forever
      return res.status(204).send();
    }

    const dataBuffer = Buffer.from(message.data, 'base64');
    const payload = JSON.parse(dataBuffer.toString('utf8'));

    const { jobId, location, createdAt, stage } = payload;

    console.log('📩 Received job message:', payload);

    if (!jobId || !stage) {
      console.error('❌ Missing jobId or stage in Pub/Sub payload');
      // Ack anyway to avoid endless retries
      return res.status(204).send();
    }

    console.log(
      `✅ Worker received job ${jobId} (stage=${stage}, location=${location})`
    );

    // Route by stage – IMPORTANT: call the correct functions
    try {
      if (stage === 'demographics' || stage === '1_demographics') {
        await handleDemographicsSegment(jobId);
      } else if (stage === '7_organicSearch') {
        await handleOrganicSearchSegment(jobId);
      } else {
        console.log(`ℹ️ Unknown stage "${stage}" - nothing to do yet.`);
      }
    } catch (stageErr) {
      // Any error inside a segment should already have updated statuses to "failed"
      console.error(
        `❌ Error while processing stage "${stage}" for job ${jobId}:`,
        stageErr
      );
      // DO NOT rethrow – we still ack Pub/Sub below
    }

    // Always ack the Pub/Sub message so we don't get stuck in a loop
    res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message (outer):', err);
    // Even if we blow up parsing, we still ack to stop infinite retries
    res.status(204).send();
  }
});

/**
 * Callback endpoint for organic search results from n8n
 * Body example:
 * [
 *   {
 *     jobId: "...",
 *     services: "deck building",
 *     location: "Redding city, California",
 *     rank1Name: "...",
 *     rank1Url: "...",
 *     ...
 *   }
 * ]
 */
app.post('/organic-result', async (req, res) => {
  try {
    await handleOrganicResultCallback(req.body);
    res.status(200).json({ ok: true });
  } catch (err) {
    console.error('❌ Error in /organic-result handler:', err);
    // We still respond 200 so n8n doesn’t keep retrying forever.
    // Inside handleOrganicResultCallback you should already mark the segment as failed.
    res.status(200).json({ ok: false, error: 'merge_failed' });
  }
});

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
