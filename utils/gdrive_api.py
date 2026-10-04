# Minimal Google Drive client (device-flow OAuth + resumable upload) using plain `requests`.
# Uses the same Google OAuth client as YouTube (YT_CLIENT_ID / YT_CLIENT_SECRET); the Drive API must be
# enabled in that Google Cloud project. Scope `drive.file` = the bot only sees files it created itself.
# Everything here is blocking: call it from a thread (loop.run_in_executor).
import os
import time

import requests

from utils import youtube_api as ya

API = os.getenv("GD_API_BASE", "https://www.googleapis.com")
SCOPE = os.getenv("GD_SCOPE", "https://www.googleapis.com/auth/drive.file")
FOLDER_NAME = os.getenv("GD_FOLDER_NAME", "NexTGSF")
CHUNK = 8 * 1024 * 1024            # multiple of 256 KiB
TIMEOUT = 60

GDError = ya.YTError
configured = ya.configured
device_poll = ya.device_poll
refresh_access_token = ya.refresh_access_token
revoke = ya.revoke
_err = ya._err


def device_start():
    return ya.device_start(SCOPE)


def account_email(access):
    """Best-effort: the Google account the user connected (for display only)."""
    r = requests.get(f"{API}/drive/v3/about", params={"fields": "user(emailAddress,displayName)"},
                     headers={"Authorization": f"Bearer {access}"}, timeout=TIMEOUT)
    if r.status_code != 200:
        raise _err(r)
    u = r.json().get("user") or {}
    return u.get("emailAddress") or u.get("displayName") or ""


def ensure_folder(access, name=FOLDER_NAME):
    """Find (or create) the bot's upload folder in the user's Drive. Returns the folder id."""
    h = {"Authorization": f"Bearer {access}"}
    q = f"name = '{name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    r = requests.get(f"{API}/drive/v3/files", params={"q": q, "fields": "files(id)", "pageSize": 1},
                     headers=h, timeout=TIMEOUT)
    if r.status_code != 200:
        raise _err(r)
    files = r.json().get("files") or []
    if files:
        return files[0]["id"]
    r = requests.post(f"{API}/drive/v3/files", params={"fields": "id"}, headers=h,
                      json={"name": name, "mimeType": "application/vnd.google-apps.folder"}, timeout=TIMEOUT)
    if r.status_code not in (200, 201):
        raise _err(r)
    return r.json()["id"]


def upload_file(get_token, path, name, folder_id=None, mime="video/mp4", on_progress=None, cancel_ev=None):
    """Resumable upload. Returns (file_id, web_link)."""
    size = os.path.getsize(path)
    meta = {"name": name}
    if folder_id:
        meta["parents"] = [folder_id]
    token = get_token()
    r = requests.post(f"{API}/upload/drive/v3/files",
                      params={"uploadType": "resumable", "fields": "id,webViewLink"},
                      headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=UTF-8",
                               "X-Upload-Content-Length": str(size), "X-Upload-Content-Type": mime},
                      json=meta, timeout=TIMEOUT)
    if r.status_code != 200 or not r.headers.get("Location"):
        raise _err(r)
    session_url = r.headers["Location"]

    def done(resp):
        j = resp.json()
        return j["id"], j.get("webViewLink") or f"https://drive.google.com/file/d/{j['id']}/view"

    sent, fails = 0, 0
    with open(path, "rb") as f:
        while True:
            if cancel_ev is not None and cancel_ev.is_set():
                raise GDError("cancelled", "Cancelled")
            f.seek(sent)
            chunk = f.read(CHUNK)
            end = sent + len(chunk) - 1
            headers = {"Authorization": f"Bearer {token}", "Content-Length": str(len(chunk)),
                       "Content-Range": f"bytes {sent}-{end}/{size}" if chunk else f"bytes */{size}"}
            try:
                resp = requests.put(session_url, data=chunk, headers=headers, timeout=300)
            except requests.RequestException:
                resp = None
            if resp is not None and resp.status_code in (200, 201):
                if on_progress:
                    on_progress(100.0)
                return done(resp)
            if resp is not None and resp.status_code == 308:
                rng = resp.headers.get("Range")
                sent = int(rng.split("-")[1]) + 1 if rng else 0
                fails = 0
                if on_progress:
                    on_progress(min(99.0, sent * 100 / max(size, 1)))
                continue
            if resp is not None and resp.status_code == 401:
                token = get_token()
                fails += 1
            elif resp is not None and resp.status_code not in (500, 502, 503, 504):
                raise _err(resp)
            else:
                fails += 1
            if fails > 6:
                raise GDError("uploadFailed", "Google Drive upload kept failing (network / server error)")
            time.sleep(min(2 ** fails, 30))
            try:
                q = requests.put(session_url, headers={"Authorization": f"Bearer {token}", "Content-Length": "0",
                                                       "Content-Range": f"bytes */{size}"}, timeout=TIMEOUT)
                if q.status_code in (200, 201):
                    return done(q)
                if q.status_code == 308:
                    rng = q.headers.get("Range")
                    sent = int(rng.split("-")[1]) + 1 if rng else 0
            except requests.RequestException:
                pass
