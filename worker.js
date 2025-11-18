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
// Pub/Sub (via Cloud Run / Eventarc) will POST messages here
app.post('/', async (req, res) => {
  try {
    // Messages can come in different formats depending on trigger type.
    // We handle the standard Pub/Sub push format:
    //
    // { "message": { "data": "<base64-encoded JSON>" }, "subscription": "..." }

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

    // 👉 Here is where we'll later:
    // - read job row from Client_audits.client_audits_jobs
    // - fetch demographics from 1_demographics
    // - insert into jobs_demographics
    // - update statuses in jobs table

    console.log(`✅ Worker received job ${jobId} (location=${location || 'N/A'})`);

    // Always respond 204 to acknowledge the message
    return res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    // 500 means "retry". If you want fewer retries later, we can adjust config.
    return res.status(500).send('Internal Server Error');
  }
});

// Health endpoint (optional but nice)
app.get('/', (req, res) => {
  res.send('Worker service is running');
});

const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Worker service listening on port ${port}`);
});
