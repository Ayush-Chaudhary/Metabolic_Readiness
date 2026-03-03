"""
Databricks connection configuration.
Reads credentials from environment variables (set automatically on Databricks Apps)
or from a local .env file for local development.

For local development, create testing_tool/.env:
    DATABRICKS_HOST=https://adb-2008955168844352.12.azuredatabricks.net
    DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXX

On Databricks Apps, DATABRICKS_HOST + DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET
are injected automatically (OAuth M2M). DATABRICKS_TOKEN is NOT injected on Apps.
"""

import os


def _load_dotenv():
    """Load .env file from this directory if it exists (local dev only)."""
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            # Only set if not already in environment (env vars take precedence)
            if key and value and key not in os.environ:
                os.environ[key] = value


# Load .env on import
_load_dotenv()


def get_databricks_host() -> str | None:
    return os.environ.get("DATABRICKS_HOST") or os.environ.get("DATABRICKS_SERVER_HOSTNAME")


def get_databricks_token() -> str | None:
    return os.environ.get("DATABRICKS_TOKEN")


def get_llm_sp_client_id() -> str | None:
    """Client ID of a service principal that has CAN_QUERY on the LLM endpoint.
    Stored as LLM_SP_CLIENT_ID so it doesn't conflict with the app-injected DATABRICKS_CLIENT_ID."""
    return os.environ.get("LLM_SP_CLIENT_ID")


def get_llm_sp_client_secret() -> str | None:
    return os.environ.get("LLM_SP_CLIENT_SECRET")


def has_llm_sp_creds() -> bool:
    """True when explicit LLM service-principal credentials are configured."""
    return bool(get_llm_sp_client_id() and get_llm_sp_client_secret())


def has_oauth_m2m() -> bool:
    """True when running on Databricks Apps (OAuth M2M injected automatically)."""
    return bool(
        os.environ.get("DATABRICKS_CLIENT_ID")
        and os.environ.get("DATABRICKS_CLIENT_SECRET")
    )


def is_configured() -> bool:
    """True when any valid auth method is available."""
    return bool(get_databricks_host() and (get_databricks_token() or has_llm_sp_creds() or has_oauth_m2m()))


def connection_status() -> dict:
    """Return a dict with connection status info for display."""
    host = get_databricks_host()
    token = get_databricks_token()
    oauth = has_oauth_m2m()
    configured = bool(host and (token or oauth))
    sp = has_llm_sp_creds()
    if configured and sp:
        label = f"✅ Connected via Service Principal — {host}"
    elif configured and oauth and not token:
        label = f"✅ Connected via Databricks App (OAuth) — {host}"
    elif configured:
        label = f"✅ Connected — {host}"
    else:
        label = "⚠️ Not configured — add .env with DATABRICKS credentials for real LLM output"
    return {
        "configured": configured,
        "host": host or "Not set",
        "token_set": bool(token),
        "oauth_m2m": oauth,
        "label": label,
    }
