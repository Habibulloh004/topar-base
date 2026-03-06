# Topar Parser App

Open from backend:

- URL: `http://localhost:8090/topar-parser-app`
- API base: same backend (`/parser-app/*`)

## Flow

1. Enter source URL and click **Run Parsing**.
2. Review parsed schema (left) and target schemas (right).
3. Set mapping rules (`eksmo.*` and `main.*`).
4. Click **Sync DB** to seed/update `eksmo_products` and `main_products`.

## Backend routes used

- `POST /parser-app/parse`
- `GET /parser-app/schema`
- `GET /parser-app/runs`
- `GET /parser-app/runs/:id`
- `GET /parser-app/runs/:id/records`
- `GET /parser-app/mappings`
- `POST /parser-app/mappings`
- `POST /parser-app/runs/:id/sync`
- `POST /parser-app/runs/:id/seed`
