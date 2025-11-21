// worker.js
const express = require('express');
const bodyParser = require('body-parser');

const { handleDemographicsSegment } = require('./demographics');
const {
  handleOrganicSearchSegment,
  handleOrganicResultCallback,
} = require('./organicSearch');

const app = express();
app.use(bodyParser.json());

/**
 * Pub/Sub push endpoint – receives messages with stage + jobId
 * Topic payload (before base64) is expected to look like:
 * {
 *   "jobId": "...",
 *   "location": "Redding city, California",
 *   "createdAt": "2025-11-21T10:03:25.912Z",
 *   "stage": "demographics" | "7_organicSearch" | ...
 * }
 */
app.post('/', async (req, res) => {
  try {
    const message = req.body && req.body.message;
    if (!message || !message.data) {
      console.error('❌ Invalid Pub/Sub message format', req.body);
      res.status(400).send('Bad Request: no message.data');
      return;
    }

    // Decode Pub/Sub data
    const dataBuffer = Buffer.from(message.data, 'base64');
    const payloadStr = dataBuffer.toString('utf8');

    let payload;
    try {
      payload = JSON.parse(payloadStr);
    } catch (err) {
      console.error('❌ Failed to parse Pub/Sub message data as JSON:', payloadStr, err);
      res.status(400).send('Bad Request: invalid JSON');
      return;
    }

    const { jobId, location, createdAt, stage } = payload;

    console.log('📩 Received job message:', payload);

    if (!jobId || !stage) {
      console.error('❌ Missing jobId or stage in Pub/Sub payload');
      res.status(400).send('Bad Request: missing jobId or stage');
      return;
    }

    console.log(
      `✅ Worker received job ${jobId} (stage=${stage}, location=${location})`
    );

    // Route by stage – each segment is responsible for checking its own status
    switch (stage) {
      case 'demographics':
        await handleDemographicsSegment(jobId);
        break;

      case '7_organicSearch':
        await handleOrganicSearchSegment(jobId);
        break;

      default:
        console.log(`ℹ️ Unknown stage "${stage}" - nothing to do yet.`);
        break;
    }

    // Success – acknowledge to Pub/Sub so it doesn’t retry
    res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    // 500 → Pub/Sub will retry, but segments should short-circuit if not queued
    res.status(500).send();
  }
});

/**
 * Callback endpoint for organic search results from n8n.
 * Expects an array with at least one item that includes jobId + rank fields.
 */
app.post('/organic-result', async (req, res) => {
  try {
    await handleOrganicResultCallback(req.body);
    res.status(200).json({ ok: true });
  } catch (err) {
    console.error('❌ Error in /organic-result handler:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * Simple GET health check for Cloud Run.
 */
app.get('/', (req, res) => {
  res.status(200).send('OK');
});

/**
 * Optional: manual trigger endpoint to run a segment by hand.
 * POST /trigger  { "jobId": "...", "stage": "demographics" | "7_organicSearch" }
 */
app.post('/trigger', async (req, res) => {
  try {
    const { jobId, stage } = req.body || {};
    if (!jobId || !stage) {
      res.status(400).json({ error: 'Missing jobId or stage' });
      return;
    }

    console.log(`🧪 Manual trigger for job ${jobId}, stage=${stage}`);

    switch (stage) {
      case 'demographics':
        await handleDemographicsSegment(jobId);
        break;

      case '7_organicSearch':
        await handleOrganicSearchSegment(jobId);
        break;

      default:
        res.status(400).json({ error: `Unknown stage "${stage}"` });
        return;
    }

    res.status(200).json({ ok: true, jobId, stage });
  } catch (err) {
    console.error('❌ Error in /trigger handler:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`🚀 client-audits-worker listening on port ${PORT}`);
});

module.exports = app;
