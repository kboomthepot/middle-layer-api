// demographics.js
'use strict';

const {
  bigquery,
  PROJECT_ID,
  bqTimestampToIso,
  safeStr,
  loadJob,
  markSegmentStatus,
  updateOverallStatus
} = require('./jobHelpers');

const DEMOS_SOURCE_TABLE = `${PROJECT_ID}.Client_audits_data.1_demographics`;
const DEMOS_JOBS_TABLE = `${PROJECT_ID}.Client_audits.1_demographicJobs`;

const MERGE_DEMOGRAPHICS_SQL = `
  MERGE \`${DEMOS_JOBS_TABLE}\` T
  USING (
    SELECT
      @jobId AS jobId,
      TIMESTAMP(@dateIso) AS date,
      @status AS status,
      @businessName AS businessName,
      CAST(@population_no AS NUMERIC) AS population_no,
      CAST(@households_no AS NUMERIC) AS households_no,
      CAST(@median_age AS FLOAT64) AS median_age,
      CAST(@median_income_households AS NUMERIC) AS median_income_households,
      CAST(@median_income_families AS NUMERIC) AS median_income_families,
      CAST(@male_percentage AS FLOAT64) AS male_percentage,
      CAST(@female_percentage AS FLOAT64) AS female_percentage
  ) S
  ON T.jobId = S.jobId
  WHEN MATCHED THEN
    UPDATE SET
      T.date = S.date,
      T.status = S.status,
      T.businessName = S.businessName,
      T.population_no = S.population_no,
      T.households_no = S.households_no,
      T.median_age = S.median_age,
      T.median_income_households = S.median_income_households,
      T.median_income_families = S.median_income_families,
      T.male_percentage = S.male_percentage,
      T.female_percentage = S.female_percentage
  WHEN NOT MATCHED THEN
    INSERT (
      jobId,
      date,
      status,
      businessName,
      population_no,
      households_no,
      median_age,
      median_income_households,
      median_income_families,
      male_percentage,
      female_percentage
    )
    VALUES (
      S.jobId,
      S.date,
      S.status,
      S.businessName,
      S.population_no,
      S.households_no,
      S.median_age,
      S.median_income_households,
      S.median_income_families,
      S.male_percentage,
      S.female_percentage
    )
`;

async function handleDemographicsStage(jobId) {
  console.log(`▶️ [DEMOS] Starting demographics processing for job ${jobId}`);

  const job = await loadJob(jobId);
  if (!job) {
    console.warn(`[DEMOS] No job row found for jobId ${jobId}`);
    return;
  }

  const createdAtIso = bqTimestampToIso(job.createdAt);

  console.log(
    `ℹ️ [DEMOS] Job ${jobId} location = "${job.location}", demographicsStatus = ${job.demographicsStatus}, organicSearchStatus = ${job.organicSearchStatus}, paidAdsStatus = ${job.paidAdsStatus}, status = ${job.status}, createdAt = ${JSON.stringify(
      job.createdAt
    )}`
  );

  // STEP 2: mark segment pending + bump overall status
  console.log(
    "➡️ [DEMOS] Step 2: Mark main job 1_demographics_Status = 'pending'"
  );
  await markSegmentStatus(jobId, '1_demographics_Status', 'pending');
  await updateOverallStatus(jobId);

  // STEP 3: load demographics source row
  console.log(
    '➡️ [DEMOS] Step 3: Load demographics from Client_audits_data.1_demographics'
  );
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
    params: { location: job.location }
  });

  console.log(
    `ℹ️ [DEMOS] Step 3 result rows (demographics): ${demoRows.length}`
  );
  if (!demoRows.length) {
    console.warn(
      `[DEMOS] No demographics row found for location "${job.location}", marking failed.`
    );
    await markSegmentStatus(jobId, '1_demographics_Status', 'failed');
    await updateOverallStatus(jobId);
    return;
  }

  const d = demoRows[0];
  console.log(
    `ℹ️ [DEMOS] Found demographics for "${job.location}": ${JSON.stringify(d)}`
  );

  // STEP 4: MERGE into per-job table
  console.log(
    '➡️ [DEMOS] Step 4: MERGE into 1_demographicJobs with demographics values'
  );
  console.log(
    `ℹ️ [DEMOS] Step 4 storing demographics for job ${jobId}: pop=${d.population_no}, age=${d.median_age}, households=${d.households_no}, income_hh=${d.median_income_households}, income_fam=${d.median_income_families}, male=${d.male_percentage}, female=${d.female_percentage}, status=completed, date=${createdAtIso}, businessName=${job.businessName}`
  );

  try {
    await bigquery.query({
      query: MERGE_DEMOGRAPHICS_SQL,
      params: {
        jobId,
        dateIso: createdAtIso,
        status: 'completed',
        businessName: safeStr(job.businessName),
        population_no: d.population_no,
        households_no: d.households_no,
        median_age: d.median_age,
        median_income_households: d.median_income_households,
        median_income_families: d.median_income_families,
        male_percentage: d.male_percentage,
        female_percentage: d.female_percentage
      }
    });
    console.log(
      `✅ [DEMOS] MERGE completed for job ${jobId} into ${DEMOS_JOBS_TABLE}`
    );
  } catch (err) {
    console.error(
      `❌ [DEMOS] MERGE FAILED for job ${jobId} into 1_demographicJobs:`,
      err && err.errors ? err.errors : err
    );
    await markSegmentStatus(jobId, '1_demographics_Status', 'failed');
    await updateOverallStatus(jobId);
    return;
  }

  // STEP 5: set segment completed + bump overall status
  console.log(
    '➡️ [DEMOS] Step 5: Update client_audits_jobs.1_demographics_Status'
  );
  await markSegmentStatus(jobId, '1_demographics_Status', 'completed');

  console.log(
    '➡️ [DEMOS] Step 6: Recalculate main job status based on all segments'
  );
  await updateOverallStatus(jobId);
}

module.exports = {
  handleDemographicsStage
};
