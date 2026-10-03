# Minimal config for the cloud worker: values come from GitHub Actions secrets (environment variables).
import os

MASTER_KEY = os.getenv("MASTER_KEY", "")
IV_KEY = os.getenv("IV_KEY", "")
YT_CLIENT_ID = os.getenv("YT_CLIENT_ID", "")
YT_CLIENT_SECRET = os.getenv("YT_CLIENT_SECRET", "")
