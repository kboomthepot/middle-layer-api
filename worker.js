// worker.js
'use strict';

const express = require('express');
const bodyParser = require('body-parser');

const { handleDemographicsStage } = require('./demographics');
const { handleOrganicStage, handleOrganicCallback } = require('./organic');

const app = express();
app.use(bodyParser.json());

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

    if (stage === 'demographics') {
      await handleDemographicsStage(jobId);
    } else if (stage === '7_organicSearch') {
      await handleOrganicStage(jobId);
    } else {
      console.log(`ℹ️ Unknown stage "${stage}" - nothing to do yet.`);
    }

    res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    res.status(500).send();
  }
});

/**
 * Callback endpoint for organic search results from n8n
 */
app.post('/organic-result', async (req, res) => {
  try {
    await handleOrganicCallback(req.body);
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
