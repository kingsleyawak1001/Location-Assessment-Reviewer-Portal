# Reviewer Website

Interactive website for technical reviewers:
- browse all canonical solution docs with navigation;
- view stage-by-stage process cards;
- test API endpoints live;
- inspect returned data with charts.

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

## Notes

- Keep API base URL in UI set to `http://127.0.0.1:8000`.
- Documentation browser reads files from `docs/solution/**`.
- API playground charts are generated from live response payloads.
