# Reviewer Website

Interactive API Playground for technical reviewers:
- run map/journey API scenarios live;
- inspect execution stages, timeline, and KPIs;
- review raw payloads and logs;
- validate charts generated from live responses.

## Run Instructions

From `Assessment Solution/` use two terminals.

### Terminal 1: API server

```bash
uvicorn src.api.app:app --host 127.0.0.1 --port 8000
```

### Terminal 2: Static site server

```bash
python -m http.server 8080
```

Open:

- Website: [http://127.0.0.1:8080/reviewer_site/index.html](http://127.0.0.1:8080/reviewer_site/index.html)
- API health: [http://127.0.0.1:8000/api/health](http://127.0.0.1:8000/api/health)

## Deploy to GitHub Pages

This repository includes workflow:
- `.github/workflows/deploy-pages.yml`

Deployment behavior:
- Deploys automatically on push to `main` when files in `reviewer_site/**` change.
- Can also be triggered manually from GitHub Actions (`workflow_dispatch`).

One-time GitHub setup:
1. Push this repository to GitHub.
2. Open repository `Settings` -> `Pages`.
3. In **Build and deployment**, set **Source** to `GitHub Actions`.
4. Open `Actions` tab and run/check `Deploy Reviewer Site to GitHub Pages`.
5. After first successful run, your site URL will be:
   - `https://<github-username>.github.io/<repository-name>/`

Important runtime note:
- GitHub Pages hosts only static files. API calls from the playground must point to a reachable backend URL in `API Base URL` (not localhost).

## Notes

- Keep API base URL in UI set to `http://127.0.0.1:8000`.
- Live monitor behavior can be toggled via `Live updates`.
- API playground charts are generated from live response payloads.
