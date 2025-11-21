// demographics.js
const { BigQuery } = require('@google-cloud/bigquery');
const { markSegmentStatus, recomputeMainStatus } = require('./status');

const bigquery = new BigQuery();

const PROJECT_ID = 'ghs-construction-1734441714520';
const JOBS_TABLE = `${PROJECT_ID}.Client_audits.client_audits_jobs`;
const DEMOS_SOURCE_TABLE = `${PROJECT_ID}.Client_audits_data.1_demographics`;
const DEMOS_TARGET_TABLE = `${PROJECT_ID}.Client_audits.1_demographicJobs`;

async function loadJob(jobId) {
  const [rows] = await bigquery.query({
    query: `
      SELECT
        jobId,
        location,
        businessName,
        1_demographics_Status AS demographicsStatus,
        7_organicSearch_Status AS organicSearchStatus,
        paidAdsStatus,
        status,
        createdAt
      FROM \`${JOBS_TABLE}\`
      WHERE jobId = @jobId
      LIMIT 1
    `,
    params: { jobId },
  });
  return rows[0] || null;
}

async function handleDemographicsSegment(jobId) {
  console.log(`▶️ [DEMOS] Starting demographics processing for job ${jobId}`);

  const job = await loadJob(jobId);
  if (!job) {
    console.warn(`⚠️ [DEMOS] No job row found for jobId=${jobId}, skipping.`);
    return;
  }

  const {
    location,
    businessName,
    demographicsStatus,
    createdAt,
  } = job;

  // 🔒 Idempotency guard: only run when status is "queued"
  if (demographicsStatus && demographicsStatus !== 'queued') {
    console.log(
      `ℹ️ [DEMOS] Job ${jobId} demographicsStatus=${demographicsStatus}, not 'queued' – skipping.`
    );
    return;
  }

  console.log(
    `ℹ️ [DEMOS] Job ${jobId} location = "${location}", demographicsStatus = ${demographicsStatus}, createdAt = ${JSON.stringify(createdAt)}`
  );

  // Step 2: mark segment pending
  await markSegmentStatus(jobId, '1_demographics_Status', 'pending');

  // Step 3: load demographics source row
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
      FROM \`${DEMOS_SOURCE_TABLE}\`
      WHERE location = @location
      LIMIT 1
    `,
    params: { location },
  });

  if (!demoRows.length) {
    console.warn(
      `⚠️ [DEMOS] No demographics found for location "${location}" (job ${jobId}), marking failed.`
    );
    await markSegmentStatus(jobId, '1_demographics_Status', 'failed');
    await recomputeMainStatus(jobId);
    return;
  }

  const demo = demoRows[0];
  const createdAtTs = createdAt?.value || createdAt || null;

  console.log(
    `ℹ️ [DEMOS] Step 4 storing demographics for job ${jobId}: pop=${demo.population_no}, age=${demo.median_age}, households=${demo.households_no}, income_hh=${demo.median_income_households}, income_fam=${demo.median_income_families}, male=${demo.male_percentage}, female=${demo.female_percentage}, status=completed, date=${createdAtTs}, businessName=${businessName}`
  );

  // Step 4: MERGE into 1_demographicJobs
  await bigquery.query({
    query: `
      MERGE \`${DEMOS_TARGET_TABLE}\` T
      USING (
        SELECT
          @jobId AS jobId,
          @businessName AS businessName,
          @date AS date,
          @population_no AS population_no,
          @median_age AS median_age,
          @households_no AS households_no,
          @median_income_households AS median_income_households,
          @median_income_families AS median_income_families,
          @male_percentage AS male_percentage,
          @female_percentage AS female_percentage,
          'completed' AS status
      ) S
      ON T.jobId = S.jobId
      WHEN NOT MATCHED THEN
        INSERT (
          jobId,
          businessName,
          date,
          population_no,
          median_age,
          households_no,
          median_income_households,
          median_income_families,
          male_percentage,
          female_percentage,
          status
        )
        VALUES (
          S.jobId,
          S.businessName,
          S.date,
          S.population_no,
          S.median_age,
          S.households_no,
          S.median_income_households,
          S.median_income_families,
          S.male_percentage,
          S.female_percentage,
          S.status
        )
      WHEN MATCHED THEN
        UPDATE SET
          businessName = S.businessName,
          date = S.date,
          population_no = S.population_no,
          median_age = S.median_age,
          households_no = S.households_no,
          median_income_households = S.median_income_households,
          median_income_families = S.median_income_families,
          male_percentage = S.male_percentage,
          female_percentage = S.female_percentage,
          status = S.status
    `,
    params: {
      jobId,
      businessName: businessName || null,
      date: createdAtTs,
      population_no: demo.population_no,
      median_age: demo.median_age,
      households_no: demo.households_no,
      median_income_households: demo.median_income_households,
      median_income_families: demo.median_income_families,
      male_percentage: demo.male_percentage,
      female_percentage: demo.female_percentage,
    },
    types: {
      jobId: 'STRING',
      businessName: 'STRING',
      date: 'TIMESTAMP',
      population_no: 'INT64',
      median_age: 'FLOAT64',
      households_no: 'INT64',
      median_income_households: 'INT64',
      median_income_families: 'INT64',
      male_percentage: 'FLOAT64',
      female_percentage: 'FLOAT64',
    },
  });

  console.log(
    `✅ [DEMOS] MERGE completed for job ${jobId} into ${DEMOS_TARGET_TABLE}`
  );

  // Step 5: mark completed + recompute main status
  await markSegmentStatus(jobId, '1_demographics_Status', 'completed');
  await recomputeMainStatus(jobId);
}

module.exports = {
  handleDemographicsSegment,
};
