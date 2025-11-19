// worker.js
const express = require('express');
const bodyParser = require('body-parser');
const { BigQuery } = require('@google-cloud/bigquery');

const PROJECT_ID = 'ghs-construction-1734441714520';

const DATASET_ID = 'Client_audits';
const JOBS_TABLE_ID = 'client_audits_jobs';

const DEMOS_DATASET_ID = 'Client_audits_data';
const DEMOS_SOURCE_TABLE_ID = '1_demographics';

const JOBS_DEMOS_TABLE_ID = 'jobs_demographics';

const bigquery = new BigQuery({ projectId: PROJECT_ID });
const app = express();
app.use(bodyParser.json());

// ---------- Helpers ----------
function toIsoStringOrNull(val) {
  if (!val) return null;
  if (val instanceof Date) return val.toISOString();
  if (typeof val === 'string') return val;
  if (typeof val === 'object' && val.value) return String(val.value);
  return null;
}

// Safely convert values like "250,000+" to numbers or null
function safeNumber(raw) {
  if (raw === null || raw === undefined) return null;
  const s = String(raw).trim();
  if (s === '') return null;
  const cleaned = s.replace(/,/g, '').replace(/\+/g, '');
  const n = Number(cleaned);
  if (Number.isNaN(n)) return null;
  return n;
}

// Status rules based on metrics set
function computeDemoStatusFromMetrics(metrics) {
  const values = [
    metrics.population_no,
    metrics.median_age,
    metrics.median_income_households,
    metrics.median_income_families,
    metrics.male_percentage,
    metrics.female_percentage,
  ];

  const nonNullCount = values.filter(v => v !== null && v !== undefined).length;

  if (nonNullCount === 0) return 'failed';
  if (nonNullCount < values.length) return 'partial';
  return 'completed';
}

// Recompute main job.status from sub-statuses
async function recomputeMainStatus(jobId) {
  try {
    const [rows] = await bigquery.query({
      query: `
        SELECT demographicsStatus, paidAdsStatus
        FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        WHERE jobId = @jobId
        LIMIT 1
      `,
      params: { jobId },
    });

    if (!rows.length) {
      console.warn(`⚠️ [DEMOS] recomputeMainStatus: Job ${jobId} not found.`);
      return;
    }

    const { demographicsStatus, paidAdsStatus } = rows[0];
    const subStatuses = [demographicsStatus, paidAdsStatus].filter(Boolean);

    let newStatus = 'pending';

    if (subStatuses.some(s => s === 'failed')) {
      newStatus = 'failed';
    } else if (subStatuses.length > 0 && subStatuses.every(s => s === 'completed')) {
      newStatus = 'completed';
    } else if (subStatuses.some(s => ['queued', 'pending', 'partial'].includes(s))) {
      newStatus = 'pending';
    }

    console.log(
      `ℹ️ [DEMOS] Recomputed main status for job ${jobId}: ${newStatus} ` +
      `(from sub-statuses=${JSON.stringify(subStatuses)})`
    );

    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET status = @status
        WHERE jobId = @jobId
      `,
      params: { jobId, status: newStatus },
    });
  } catch (err) {
    console.error(`❌ [DEMOS] Error in recomputeMainStatus for job ${jobId}:`, err);
  }
}

// ---------- Health ----------
app.get('/', (_, res) => {
  res.send('Worker service listening on port 8080');
});

// ---------- Pub/Sub endpoint ----------
app.post('/', async (req, res) => {
  try {
    const envelope = req.body;
    if (!envelope || !envelope.message || !envelope.message.data) {
      console.error('❌ Invalid Pub/Sub message format:', JSON.stringify(envelope));
      return res.status(204).send();
    }

    const payload = JSON.parse(
      Buffer.from(envelope.message.data, 'base64').toString()
    );

    console.log('📩 Received job message:', payload);
    const { jobId } = payload;

    if (!jobId) {
      console.error('❌ [DEMOS] Missing jobId in message payload. Skipping.');
      return res.status(204).send();
    }

    console.log(
      `✅ Worker received job ${jobId} (location=${payload.location || 'N/A'})`
    );

    await processJobDemographics(jobId);

    res.status(204).send();
  } catch (err) {
    console.error('❌ Error handling Pub/Sub message:', err);
    res.status(204).send(); // ACK anyway to avoid infinite retries
  }
});

// ---------- Main processor ----------
async function processJobDemographics(jobId) {
  console.log(`▶️ [DEMOS] Starting demographics processing for job ${jobId}`);

  try {
    // STEP 1 – Load job row
    console.log('➡️ [DEMOS] Step 1: Load job row from client_audits_jobs');

    const [jobRows] = await bigquery.query({
      query: `
        SELECT
          jobId,
          location,
          demographicsStatus,
          paidAdsStatus,
          status,
          createdAt
        FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        WHERE jobId = @jobId
        LIMIT 1
      `,
      params: { jobId },
    });

    console.log(`ℹ️ [DEMOS] Step 1 result rows: ${jobRows.length}`);
    if (!jobRows.length) {
      console.warn(`⚠️ [DEMOS] Job ${jobId} not found. Skipping.`);
      return;
    }

    const job = jobRows[0];
    const location = job.location || null;
    const currentDemoStatus = job.demographicsStatus || 'queued';
    const jobCreatedAtIso = toIsoStringOrNull(job.createdAt);

    console.log(
      `ℹ️ [DEMOS] Job ${jobId} location = "${location}", ` +
      `demographicsStatus = ${currentDemoStatus}, paidAdsStatus = ${job.paidAdsStatus}, ` +
      `status = ${job.status}, createdAt=${jobCreatedAtIso}`
    );

    if (!location) {
      console.warn(`⚠️ [DEMOS] Job ${jobId} has no location; cannot process demographics.`);
      return;
    }

    // If job is already fully done for demographics, skip
    if (currentDemoStatus === 'completed' || currentDemoStatus === 'partial') {
      console.log(
        `ℹ️ [DEMOS] Job ${jobId} already has demographicsStatus="${currentDemoStatus}". Skipping.`
      );
      await recomputeMainStatus(jobId);
      return;
    }

    // STEP 2 – Ensure a pending row exists in jobs_demographics
    console.log(
      '➡️ [DEMOS] Step 2: Ensure pending row exists in jobs_demographics'
    );

    const [existingRows] = await bigquery.query({
      query: `
        SELECT jobId, status, population_no, median_age,
               median_income_households, median_income_families,
               male_percentage, female_percentage, timestamp
        FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        WHERE jobId = @jobId
        LIMIT 1
      `,
      params: { jobId },
    });

    if (!existingRows.length) {
      console.log(
        `ℹ️ [DEMOS] No jobs_demographics row for job ${jobId}, inserting pending row via streaming insert.`
      );

      const nowIso = new Date().toISOString();
      const timestampIso = jobCreatedAtIso || nowIso;

      const pendingRow = {
        jobId,
        location,
        status: 'pending',
        timestamp: timestampIso,   // date from main job
        createdAt: nowIso,
        updatedAt: nowIso,
      };

      console.log(
        `ℹ️ [DEMOS] Step 2 pendingRow for job ${jobId}: ${JSON.stringify(pendingRow)}`
      );

      await bigquery
        .dataset(DATASET_ID)
        .table(JOBS_DEMOS_TABLE_ID)
        .insert([pendingRow], { ignoreUnknownValues: true });

      console.log(
        `✅ [DEMOS] Streaming insert pending row for job ${jobId} into ${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}`
      );
    } else {
      const existing = existingRows[0];
      console.log(
        `ℹ️ [DEMOS] Existing jobs_demographics row for job ${jobId}: ` +
        JSON.stringify(existing)
      );

      const metrics = {
        population_no: existing.population_no ?? null,
        median_age: existing.median_age ?? null,
        median_income_households: existing.median_income_households ?? null,
        median_income_families: existing.median_income_families ?? null,
        male_percentage: existing.male_percentage ?? null,
        female_percentage: existing.female_percentage ?? null,
      };
      const nonNullCount = Object.values(metrics).filter(
        v => v !== null && v !== undefined
      ).length;

      // If row already has metrics + completed/partial AND main status matches, skip
      if (
        nonNullCount > 0 &&
        (existing.status === 'completed' || existing.status === 'partial') &&
        (currentDemoStatus === 'completed' || currentDemoStatus === 'partial')
      ) {
        console.log(
          `ℹ️ [DEMOS] Job ${jobId} already has populated demographics row and status="${existing.status}". Skipping.`
        );
        await recomputeMainStatus(jobId);
        return;
      }
    }

    // STEP 3 – Mark demographicsStatus = pending in main table
    console.log(
      '➡️ [DEMOS] Step 3: Mark main job demographicsStatus = pending'
    );

    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET demographicsStatus = 'pending'
        WHERE jobId = @jobId
      `,
      params: { jobId },
    });

    console.log(`✅ [DEMOS] Marked demographicsStatus = 'pending' for job ${jobId}`);
    await recomputeMainStatus(jobId);

    // STEP 4 – Load demographics from source (no households_no)
    console.log(
      '➡️ [DEMOS] Step 4: Load demographics from Client_audits_data.1_demographics'
    );

    const [demoRows] = await bigquery.query({
      query: `
        SELECT
          population_no,
          median_age,
          median_income_households,
          median_income_families,
          male_percentage,
          female_percentage
        FROM \`${PROJECT_ID}.${DEMOS_DATASET_ID}.${DEMOS_SOURCE_TABLE_ID}\`
        WHERE location = @location
        LIMIT 1
      `,
      params: { location },
    });

    console.log(`ℹ️ [DEMOS] Step 4 result rows (demographics): ${demoRows.length}`);

    if (!demoRows.length) {
      console.warn(
        `⚠️ [DEMOS] No demographics found in ${DEMOS_DATASET_ID}.${DEMOS_SOURCE_TABLE_ID} for ` +
        `"${location}". Marking as failed.`
      );

      await bigquery.query({
        query: `
          UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
          SET demographicsStatus = 'failed'
          WHERE jobId = @jobId
        `,
        params: { jobId },
      });

      await recomputeMainStatus(jobId);
      return;
    }

    const demo = demoRows[0];
    console.log(
      `ℹ️ [DEMOS] Found demographics for "${location}": ${JSON.stringify(demo)}`
    );

    const metrics = {
      population_no: safeNumber(demo.population_no),
      median_age: safeNumber(demo.median_age),
      median_income_households: safeNumber(demo.median_income_households),
      median_income_families: safeNumber(demo.median_income_families),
      male_percentage: safeNumber(demo.male_percentage),
      female_percentage: safeNumber(demo.female_percentage),
    };

    const newDemoStatus = computeDemoStatusFromMetrics(metrics);

    console.log(
      `ℹ️ [DEMOS] Step 4 metrics for job ${jobId}: ` +
      `${JSON.stringify(metrics)} => newDemoStatus="${newDemoStatus}"`
    );

    const nowIso = new Date().toISOString();
    const timestampIso = jobCreatedAtIso || nowIso;

    // STEP 5 – Update jobs_demographics row via DML
    console.log(
      '➡️ [DEMOS] Step 5: UPDATE jobs_demographics with demographics values'
    );

    const [updateResult] = await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        SET
          population_no = @population_no,
          median_age = @median_age,
          median_income_households = @median_income_households,
          median_income_families = @median_income_families,
          male_percentage = @male_percentage,
          female_percentage = @female_percentage,
          status = @status,
          timestamp = @timestamp,
          updatedAt = @updatedAt
        WHERE jobId = @jobId
      `,
      params: {
        jobId,
        population_no: metrics.population_no,
        median_age: metrics.median_age,
        median_income_households: metrics.median_income_households,
        median_income_families: metrics.median_income_families,
        male_percentage: metrics.male_percentage,
        female_percentage: metrics.female_percentage,
        status: newDemoStatus,
        timestamp: timestampIso,
        updatedAt: nowIso,
      },
    });

    console.log(
      `✅ [DEMOS] UPDATE jobs_demographics completed for job ${jobId}. ` +
      `stats=${JSON.stringify(updateResult.statistics || {}, null, 2)}`
    );

    // Quick verification read
    const [verifyRows] = await bigquery.query({
      query: `
        SELECT jobId, location, population_no, median_age,
               median_income_households, median_income_families,
               male_percentage, female_percentage, status, timestamp
        FROM \`${PROJECT_ID}.${DATASET_ID}.${JOBS_DEMOS_TABLE_ID}\`
        WHERE jobId = @jobId
        LIMIT 1
      `,
      params: { jobId },
    });

    console.log(
      `ℹ️ [DEMOS] Step 5 verify row for job ${jobId}: ` +
      JSON.stringify(verifyRows[0] || null)
    );

    const verified = verifyRows[0];
    const verifyMetrics = verified
      ? {
          population_no: verified.population_no ?? null,
          median_age: verified.median_age ?? null,
          median_income_households: verified.median_income_households ?? null,
          median_income_families: verified.median_income_families ?? null,
          male_percentage: verified.male_percentage ?? null,
          female_percentage: verified.female_percentage ?? null,
        }
      : null;

    if (!verified || Object.values(verifyMetrics).every(v => v == null)) {
      console.warn(
        `⚠️ [DEMOS] Verification for job ${jobId} shows all metrics null or no row; marking failed.`
      );

      await bigquery.query({
        query: `
          UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
          SET demographicsStatus = 'failed'
          WHERE jobId = @jobId
        `,
        params: { jobId },
      });

      await recomputeMainStatus(jobId);
      return;
    }

    // STEP 6 – Update main job demographicsStatus to final value (completed/partial/failed)
    console.log('➡️ [DEMOS] Step 6: Update client_audits_jobs.demographicsStatus');

    await bigquery.query({
      query: `
        UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
        SET demographicsStatus = @demoStatus
        WHERE jobId = @jobId
      `,
      params: { jobId, demoStatus: newDemoStatus },
    });

    console.log(
      `✅ [DEMOS] Marked demographicsStatus = '${newDemoStatus}' for job ${jobId}`
    );

    await recomputeMainStatus(jobId);
  } catch (err) {
    console.error(`❌ [DEMOS] Error in processJobDemographics for job ${jobId}:`, err);

    try {
      await bigquery.query({
        query: `
          UPDATE \`${PROJECT_ID}.${DATASET_ID}.${JOBS_TABLE_ID}\`
          SET demographicsStatus = 'failed'
          WHERE jobId = @jobId
            AND demographicsStatus NOT IN ('completed','partial')
        `,
        params: { jobId },
      });
      await recomputeMainStatus(jobId);
    } catch (innerErr) {
      console.error(
        `❌ [DEMOS] Also failed to mark job ${jobId} as failed:`,
        innerErr
      );
    }
  }
}

// ---------- Start server ----------
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Worker service listening on port ${port}`);
});
