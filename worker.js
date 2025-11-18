// worker.js
const express = require('express');
const bodyParser = require('body-parser');
const { BigQuery } = require('@google-cloud/bigquery');

const app = express();
app.use(bodyParser.json());

const PROJECT_ID = 'ghs-construction-1734441714520';
const DATASET_ID = 'Client_audits';
const JOBS_TABLE_ID = 'client_audits_jobs';

const bigquery = new BigQuery({ projectId: PROJECT_ID });

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

    const { jobId, location, createdAt } = payloadJson;
    console.log('📩 Received job message:', payloadJson);

    if (!jobId) {
      console.error('⚠️ Missing jobId in message payload');
      return res.status(400).send('Bad Request: missing jobId');
    }

    console.log(`✅ Worker received job ${jobId} (location=${location || 'N/A'})`);

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
