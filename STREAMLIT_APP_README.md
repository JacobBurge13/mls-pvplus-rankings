# MLS 2026 PV+ Streamlit App

Run the app from this folder with:

```bash
streamlit run app.py
```

What it shows:

- 2026 season PV+ data pulled directly from Supabase/Postgres
- Team filter
- Player name filter
- Position group filter
- Minimum minutes filter
- Filterable spreadsheet of player PV+ metrics

Local secrets:

1. Copy `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml`
2. Fill in:
   `SUPABASE_DB_NAME`, `SUPABASE_DB_USER`, `SUPABASE_DB_PASSWORD`, `SUPABASE_DB_HOST`, `SUPABASE_DB_PORT`

Visual direction:

- Dark pitch-style background
- White headline typography
- Slate grid lines
- Gold and blue accent colors inspired by `/Users/jacobburgess/Desktop/download-1.png`

Deploy on Streamlit Community Cloud:

1. Put this folder in a GitHub repository.
2. Commit these files:
   `app.py`, `requirements.txt`, `.streamlit/config.toml`, `.gitignore`
3. Do not commit `.streamlit/secrets.toml`.
4. In Streamlit Community Cloud, create a new app and select `app.py` as the entrypoint.
5. In Advanced settings, paste the contents of your secrets file into the Secrets field.
6. Deploy.
