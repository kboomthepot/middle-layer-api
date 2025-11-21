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
 * Pub/Sub push endpoint – receives messages with stage + jobId.
 * This ONLY runs the specified stage. Stages do NOT trigger each other.
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

    const { jobId, location, createdAt, stage } = payload;

    console.log('📩 Received job message:', payload);

    if (!jobId || !stage) {
      console.error('❌ Missing jobId or stage in Pub/Sub payload');
      res.status(400).send('Bad Request');
      return;
    }

    console.log(
      `✅ Worker received job ${jobId} (stage=${stage}, location=${location})`
    );

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

    // Always ACK Pub/Sub (no retry loops at this level)
    res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    // Let Pub/Sub retry on error:
    res.status(500).send();
  }
});

/**
 * Manual trigger endpoint to run a specific segment for a given jobId.
 * Useful for re-running stuck segments.
 *
 * POST /run-segment
 * {
 *   "jobId": "uuid",
 *   "stage": "demographics" | "7_organicSearch"
 * }
 */
app.post('/run-segment', async (req, res) => {
  try {
    const { jobId, stage } = req.body || {};
    if (!jobId || !stage) {
      res
        .status(400)
        .json({ error: 'jobId and stage are required in body' });
      return;
    }

    console.log(
      `🧪 [MANUAL] Manually triggering stage=${stage} for job ${jobId}`
    );

    switch (stage) {
      case 'demographics':
        await handleDemographicsSegment(jobId);
        break;
      case '7_organicSearch':
        await handleOrganicSearchSegment(jobId);
        break;
      default:
        res.status(400).json({ error: `Unknown stage: ${stage}` });
        return;
    }

    res.status(200).json({ ok: true, jobId, stage });
  } catch (err) {
    console.error('❌ Error in /run-segment manual handler:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * Dedicated manual endpoints if you want them even simpler:
 * POST /run-demographics { "jobId": "..." }
 * POST /run-organic     { "jobId": "..." }
 */
app.post('/run-demographics', async (req, res) => {
  try {
    const { jobId } = req.body || {};
    if (!jobId) {
      res.status(400).json({ error: 'jobId is required' });
      return;
    }
    console.log(`🧪 [MANUAL] /run-demographics for job ${jobId}`);
    await handleDemographicsSegment(jobId);
    res.status(200).json({ ok: true, jobId, stage: 'demographics' });
  } catch (err) {
    console.error('❌ Error in /run-demographics:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/run-organic', async (req, res) => {
  try {
    const { jobId } = req.body || {};
    if (!jobId) {
      res.status(400).json({ error: 'jobId is required' });
      return;
    }
    console.log(`🧪 [MANUAL] /run-organic for job ${jobId}`);
    await handleOrganicSearchSegment(jobId);
    res.status(200).json({ ok: true, jobId, stage: '7_organicSearch' });
  } catch (err) {
    console.error('❌ Error in /run-organic:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * Callback endpoint for organic search results from n8n.
 * Body is your current organic JSON blob / array.
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

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`🚀 client-audits-worker listening on port ${PORT}`);
});
