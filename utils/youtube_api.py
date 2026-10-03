# Minimal YouTube Data API client (device-flow OAuth + resumable upload) using plain `requests`.
# Everything here is blocking: call it from a thread (loop.run_in_executor).
import os
import time

import requests

try:
    from config import YT_CLIENT_ID, YT_CLIENT_SECRET
except Exception:  # pragma: no cover
    YT_CLIENT_ID = YT_CLIENT_SECRET = ""

OAUTH = os.getenv("YT_OAUTH_BASE", "https://oauth2.googleapis.com")
API = os.getenv("YT_API_BASE", "https://www.googleapis.com")
# 'youtube' is allowed in Google's device flow (and also lets us read the channel name).
SCOPE = os.getenv("YT_SCOPE", "https://www.googleapis.com/auth/youtube")
CHUNK = 8 * 1024 * 1024            # must be a multiple of 256 KiB
TIMEOUT = 60


class YTError(Exception):
    def __init__(self, reason, message=""):
        super().__init__(message or reason)
        self.reason, self.message = reason, message or reason


def configured():
    return bool(YT_CLIENT_ID and YT_CLIENT_SECRET)


def _err(r):
    try:
        e = r.json().get("error", {})
        if isinstance(e, str):
            return YTError(e, r.json().get("error_description", e))
        reason = (e.get("errors") or [{}])[0].get("reason") or e.get("status") or str(r.status_code)
        return YTError(reason, e.get("message", r.text[:200]))
    except Exception:
        return YTError(str(r.status_code), r.text[:200])


# ------------------------------------------------------------ OAuth (device flow)
def device_start():
    r = requests.post(f"{OAUTH}/device/code", data={"client_id": YT_CLIENT_ID, "scope": SCOPE}, timeout=TIMEOUT)
    if r.status_code != 200:
        raise _err(r)
    return r.json()          # device_code, user_code, verification_url, expires_in, interval


def device_poll(device_code):
    """Returns (status, data). status: ok | pending | slow_down | denied | expired | error"""
    r = requests.post(f"{OAUTH}/token", data={
        "client_id": YT_CLIENT_ID, "client_secret": YT_CLIENT_SECRET, "device_code": device_code,
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code"}, timeout=TIMEOUT)
    j = r.json() if r.content else {}
    if r.status_code == 200:
        return "ok", j
    e = j.get("error", "")
    return {"authorization_pending": "pending", "slow_down": "slow_down", "access_denied": "denied",
            "expired_token": "expired"}.get(e, "error"), j


def refresh_access_token(refresh_token):
    r = requests.post(f"{OAUTH}/token", data={
        "client_id": YT_CLIENT_ID, "client_secret": YT_CLIENT_SECRET,
        "refresh_token": refresh_token, "grant_type": "refresh_token"}, timeout=TIMEOUT)
    if r.status_code != 200:
        raise _err(r)
    j = r.json()
    return j["access_token"], int(j.get("expires_in", 3600))


def revoke(token):
    try:
        requests.post(f"{OAUTH}/revoke", params={"token": token}, timeout=15)
    except Exception:
        pass


def channel_info(access):
    r = requests.get(f"{API}/youtube/v3/channels", params={"part": "snippet", "mine": "true"},
                     headers={"Authorization": f"Bearer {access}"}, timeout=TIMEOUT)
    if r.status_code != 200:
        raise _err(r)
    items = r.json().get("items") or []
    if not items:
        raise YTError("noChannel", "That Google account has no YouTube channel yet.")
    return {"id": items[0]["id"], "title": items[0]["snippet"]["title"]}


# ------------------------------------------------------------ upload
def clean_text(s, maxlen):
    return (s or "").replace("<", "‹").replace(">", "›")[:maxlen].strip()


def upload_video(get_token, path, title, description, privacy="unlisted", category="27",
                 on_progress=None, cancel_ev=None):
    """Resumable upload. `get_token()` returns a valid access token (called again after a 401).
    Returns the new video id."""
    size = os.path.getsize(path)
    meta = {"snippet": {"title": clean_text(title, 100) or "Video", "description": clean_text(description, 4900),
                        "categoryId": category},
            "status": {"privacyStatus": privacy, "selfDeclaredMadeForKids": False}}
    token = get_token()
    r = requests.post(f"{API}/upload/youtube/v3/videos",
                      params={"uploadType": "resumable", "part": "snippet,status"},
                      headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=UTF-8",
                               "X-Upload-Content-Length": str(size), "X-Upload-Content-Type": "video/mp4"},
                      json=meta, timeout=TIMEOUT)
    if r.status_code != 200 or not r.headers.get("Location"):
        raise _err(r)
    session_url = r.headers["Location"]

    sent, fails = 0, 0
    with open(path, "rb") as f:
        while True:
            if cancel_ev is not None and cancel_ev.is_set():
                raise YTError("cancelled", "Cancelled")
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
                return resp.json()["id"]
            if resp is not None and resp.status_code == 308:
                rng = resp.headers.get("Range")             # e.g. "bytes=0-8388607"
                sent = int(rng.split("-")[1]) + 1 if rng else 0
                fails = 0
                if on_progress:
                    on_progress(min(99.0, sent * 100 / size))
                continue
            if resp is not None and resp.status_code == 401:
                token = get_token()
                fails += 1
            elif resp is not None and resp.status_code not in (500, 502, 503, 504):
                raise _err(resp)
            else:
                fails += 1
            if fails > 6:
                raise YTError("uploadFailed", "YouTube upload kept failing (network / server error)")
            time.sleep(min(2 ** fails, 30))
            # ask the server how much it really has, then continue from there
            try:
                q = requests.put(session_url, headers={"Authorization": f"Bearer {token}", "Content-Length": "0",
                                                       "Content-Range": f"bytes */{size}"}, timeout=TIMEOUT)
                if q.status_code in (200, 201):
                    return q.json()["id"]
                if q.status_code == 308:
                    rng = q.headers.get("Range")
                    sent = int(rng.split("-")[1]) + 1 if rng else 0
            except requests.RequestException:
                pass


def set_thumbnail(token, video_id, jpg_path):
    with open(jpg_path, "rb") as f:
        r = requests.post(f"{API}/upload/youtube/v3/thumbnails/set", params={"videoId": video_id},
                          headers={"Authorization": f"Bearer {token}", "Content-Type": "image/jpeg"},
                          data=f.read(), timeout=TIMEOUT)
    if r.status_code != 200:
        raise _err(r)
