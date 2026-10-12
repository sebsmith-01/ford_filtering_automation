import json
import os
from dotenv import load_dotenv
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

load_dotenv()

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

TOKEN_PATH = os.getenv("GOOGLE_OAUTH_TOKEN_JSON")
CLIENT_SECRET_PATH = os.getenv("GOOGLE_OAUTH_CLIENT_JSON")
# Resolved against the repo root so scheduled jobs work whatever their working directory
SERVICE_ACCOUNT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""))

READONLY_SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]


def get_creds() -> Credentials:
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    if creds and creds.valid:
        return creds

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            with open(TOKEN_PATH, "w") as f:
                f.write(creds.to_json())
            return creds
        except RefreshError:
            pass

    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_PATH, SCOPES)
    creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")
    with open(TOKEN_PATH, "w") as f:
        f.write(creds.to_json())
    return creds


def get_service_account_email() -> str:
    with open(SERVICE_ACCOUNT_PATH) as f:
        return json.load(f)["client_email"]


def get_service_account_creds() -> service_account.Credentials:
    """Read-only service-account creds for unattended jobs — unlike the OAuth token these
    never expire, but the service account can only see sheets that are shared with it."""
    return service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_PATH, scopes=READONLY_SCOPES)
