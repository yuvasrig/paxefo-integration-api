# Senior Integration Engineer - Take-Home Exercise v2

## The Challenge: Build a Real IoT Data Integration Pipeline

**Time Limit:** 4-5 hours  
**Submission:** Two deployed applications + GitHub repository

---

## ⚠️ Important: Read This First

Everything below represents **minimum requirements**. With modern LLMs, a capable engineer can build the basics in about an hour.

**We encourage using AI tools** (Copilot, Claude, ChatGPT, etc.) - that's how modern engineers work. But what we're really evaluating is:

- **What you do beyond the minimum** - How do you think about scalability? Error handling? Edge cases?
- **Your design decisions** - Why did you structure it this way? What tradeoffs did you consider?
- **Your ability to build production-ready systems** - not just working code

---

## Overview

Build and deploy **two applications**:

1. **Integration API** - Receives Tive webhook payloads, transforms to PAXAFE format, stores in a database
2. **Mock Tive Sender** - Generates and sends test payloads to your Integration API

This simulates real IoT integrations in pharmaceutical cold chain monitoring.

---

## Provided Schemas

| File | Description |
|------|-------------|
| `tive-incoming-schema.json` | Tive webhook payload format (input) |
| `px-sensor-schema.json` | PAXAFE sensor payload format (output) |
| `px-location-schema.json` | PAXAFE location payload format (output) |
| `sample-tive-payloads.json` | Example payloads for testing |

---

## Part 1: Integration API (75% of evaluation)

Build a webhook receiver that:
- Receives Tive payloads at `POST /api/webhook/tive`
- Validates incoming data
- Transforms to PAXAFE sensor + location formats
- Persists to a database (design your own schema)
- Handles authentication via API key

How you structure the response (sync/async), what you store, and how you handle edge cases is up to you. Document your decisions.

---

## Part 2: Mock Tive Sender (25% of evaluation)

Build an app that generates Tive payloads based on the provided schema, sends them to your Integration API, and displays the results.

How you build it (UI, CLI, etc.) is up to you.

## Requirements

- **Deploy** to the hosting platform of your choice (Vercel, Netlify, Render, or similar; free tiers are fine) - both apps publicly accessible and working end-to-end
- **Language and framework** of your choice
- **A database** of your choice for the integration API - running both locally and in your deployment
- **Tests** for transformation logic
- **README** with setup instructions

---

## Evaluation Criteria

| Area | Weight |
|------|--------|
| Code Quality | 20% |
| Integration Accuracy | 35% |
| Deployment & Operations | 15% |
| Testing & Documentation | 20% |
| Database Design | 10% |

Remember: these are minimums. Show us how you think about production systems.

---

## Submission

Email us with:
- Two GitHub repos (Integration API + Mock Sender)
- Deployed URLs for both apps
- Test API key
- Any assumptions you made

**Make reasonable assumptions and document them. Good luck!**




