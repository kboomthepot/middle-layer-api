import express from "express";
import bodyParser from "body-parser";
import cors from "cors";
import { v4 as uuidv4 } from "uuid";

const app = express();

// Enable CORS for your frontend domain
app.use(cors({ origin: ["https://your-sliplane-app.com"] }));
app.use(bodyParser.json());

// In-memory store (replace with Firestore/BigQuery in production)
const jobs = new Map();

/**
 * Create a new job
 * Called by backend/N8N when quiz is submitted
 */
app.post("/jobs", (req, res) => {
  const jobId = uuidv4();
  const { user, business, revenue, budget, services, location } = req.body;

  const job = {
    jobId,
    status: "queued",
    user,
    business,
    revenue,
    budget,
    services,
    location,
    createdAt: new Date(),
    updatedAt: new Date(),
    reportUrl: null,
    errorMessage: null,
  };

  jobs.set(jobId, job);
  res.json({ jobId, status: "queued" });
});

/**
 * Update job status
 * Called by backend as report progresses
 */
app.patch("/jobs/:jobId/status", (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: "Job not found" });

  const { status, reportUrl, errorMessage } = req.body;
  if (status) job.status = status;
  if (reportUrl) job.reportUrl = reportUrl;
  if (errorMessage) job.errorMessage = errorMessage;

  job.updatedAt = new Date();
  jobs.set(job.jobId, job);

  res.json(job);
});

/**
 * Get job status (frontend polls this)
 */
app.get("/status", (req, res) => {
  const job = jobs.get(req.query.jobId);
  if (!job) return res.status(404).json({ error: "Job not found" });

  res.json({ jobId: job.jobId, status: job.status });
});

/**
 * Get full report (when ready)
 */
app.get("/report", (req, res) => {
  const job = jobs.get(req.query.jobId);
  if (!job) return res.status(404).json({ error: "Job not found" });
  if (job.status !== "ready")
    return res.status(400).json({ error: "Report not ready yet" });

  res.json(job);
});

/**
 * Admin view
 */
app.get("/admin/jobs", (req, res) => {
  res.json([...jobs.values()]);
});

// Start server
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`Middle Layer API running on port ${PORT}`));
