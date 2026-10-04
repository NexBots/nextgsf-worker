# Core logic for the download -> watermark -> upload pipeline (no Telegram code in here,
# so it can be tested on its own).
import asyncio
import base64
import io
import json
import math
import os
import re
import shutil
import tempfile
import threading
import time
from datetime import datetime
from urllib.parse import urlparse

import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFont, features
import yt_dlp

FFMPEG = os.getenv("FFMPEG_PATH", "ffmpeg")
FFPROBE = os.getenv("FFPROBE_PATH", "ffprobe")
PRESET = os.getenv("FFMPEG_PRESET", "veryfast")
DL_CONCURRENCY = max(1, min(32, int(os.getenv("DL_CONCURRENCY", "16"))))
CRF = os.getenv("FFMPEG_CRF", "23")
MAX_TG_BYTES = int(float(os.getenv("TG_MAX_MB", "1950")) * 1024 * 1024)

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORK_DIR = os.getenv("WORK_DIR", os.path.join(_BASE, "work"))
BUNDLED_FONT = os.path.join(_BASE, "assets", "fonts", "DejaVuSans-Bold.ttf")

DEFAULTS = {
    "wm_mode": "off",          # off | text | logo | both
    "wm_text": "",
    "wm_logo_b64": "",
    "wm_pos": "dr",            # ul uc ur cl cc cr dl dc dr
    "wm_size": 10,             # watermark width as % of video width
    "wm_opacity": 50,          # 10..100
    "caption_tpl": "🎬 **{title}**\n⏱ {duration} · 🎞 {res}",
    "title_tpl": "{title}",
    "desc_tpl": "",
    "yt_privacy": "unlisted",
    "fwd_chat": "",
    "thumb_b64": "",
    "next_n": 1,
    "referer": "",
    "referer_map": {},
    "fsets": [],               # forward sets: [{"name": str, "chats": [chat_id, ...]}]
    "yt_refresh": "",          # encrypted YouTube refresh token
    "yt_channel": "",
    "gd_refresh": "",          # encrypted Google Drive refresh token
    "gd_email": "",
    "gd_folder": "",           # cached id of the upload folder in the user's Drive
}

PLACEHOLDERS = ["title", "n", "date", "time", "duration", "res", "source", "uploader",
                "filename", "filesize", "yt", "yt_title", "slide", "url"]


def have_ffmpeg():
    return shutil.which(FFMPEG) is not None and shutil.which(FFPROBE) is not None


# ------------------------------------------------------------------ formatting
def fmt_duration(sec):
    sec = int(sec or 0)
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def fmt_size(b):
    b = float(b or 0)
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024 or unit == "GB":
            return f"{b:.0f} {unit}" if unit == "B" else f"{b:.1f} {unit}"
        b /= 1024


def fmt_eta(sec):
    if sec is None or sec < 0:
        return "--"
    return fmt_duration(sec)


def bar(pct, n=10):
    pct = max(0.0, min(100.0, float(pct or 0)))
    full = int(round(n * pct / 100))
    return "█" * full + "░" * (n - full)


_ph_re = re.compile(r"\{(\w+)\}")


def sanitize_value(v):
    """Remove characters that would break Telegram's markdown in captions."""
    return re.sub(r"[*_`~|\[\]<>]", " ", str(v)).strip()


def render_template(tpl, values):
    """Fill {placeholders}. Lines whose placeholders are all empty are dropped
    (so '🔗 YouTube: {yt}' disappears when there is no YouTube link)."""
    out = []
    for line in (tpl or "").split("\n"):
        names = [n for n in _ph_re.findall(line) if n in PLACEHOLDERS]
        if names and all(not str(values.get(n, "")).strip() for n in names):
            continue
        out.append(_ph_re.sub(
            lambda m: sanitize_value(values.get(m.group(1), "")) if m.group(1) in PLACEHOLDERS else m.group(0),
            line))
    return "\n".join(out).strip()


_WEAK_TITLES = {"", "playlist", "video", "index", "master", "stream", "untitled", "media", "video.m3u8",
                "playlist.m3u8", "index.m3u8", "master.m3u8"}


def is_weak_title(t):
    t = (t or "").strip().lower()
    return t in _WEAK_TITLES or bool(re.fullmatch(r"[0-9a-f\-]{24,}", t)) or t.endswith(".m3u8")


def sanitize_filename(name, maxlen=100):
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name).strip(" .")
    return (name or "video")[:maxlen]


def source_name(url, extractor=None):
    if extractor and extractor.lower() not in ("generic", "hlsplaylist"):
        return extractor
    return urlparse(url).netloc or "web"


# ------------------------------------------------------------------ images (watermark / thumbnail)
def _font_candidates(has_bengali):
    c = []
    if has_bengali:
        c += [r"C:\Windows\Fonts\Nirmala.ttf", r"C:\Windows\Fonts\NirmalaB.ttf",
              "/usr/share/fonts/truetype/noto/NotoSansBengali-Bold.ttf",
              "/usr/share/fonts/truetype/noto/NotoSansBengali-Regular.ttf",
              "/Library/Fonts/Arial Unicode.ttf"]
    c += [BUNDLED_FONT, r"C:\Windows\Fonts\arialbd.ttf",
          "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"]
    return [p for p in c if os.path.exists(p)]


def bengali_ok():
    """Bengali text needs a Bengali font AND libraqm for correct conjunct shaping."""
    return bool(features.check("raqm")) and any("engali" in p or "Nirmala" in p
                                                 for p in _font_candidates(True))


def render_text_image(text):
    has_bn = any("\u0980" <= ch <= "\u09ff" for ch in text)
    paths = _font_candidates(has_bn)
    font = ImageFont.truetype(paths[0], 160) if paths else ImageFont.load_default()
    stroke = 7
    dummy = ImageDraw.Draw(Image.new("RGBA", (1, 1)))
    box = dummy.textbbox((0, 0), text, font=font, stroke_width=stroke)
    w, h = box[2] - box[0] + 2 * stroke, box[3] - box[1] + 2 * stroke
    img = Image.new("RGBA", (max(w, 1), max(h, 1)), (0, 0, 0, 0))
    ImageDraw.Draw(img).text((stroke - box[0], stroke - box[1]), text, font=font,
                             fill=(255, 255, 255, 255), stroke_width=stroke, stroke_fill=(0, 0, 0, 255))
    return img


def build_watermark_image(mode, text, logo_bytes, target_w, opacity):
    """Return an RGBA PIL image `target_w` px wide, or None if nothing to draw."""
    parts = []
    if mode in ("text", "both") and (text or "").strip():
        parts.append(render_text_image(text.strip()))
    if mode in ("logo", "both") and logo_bytes:
        parts.append(Image.open(io.BytesIO(logo_bytes)).convert("RGBA"))
    if not parts:
        return None
    if len(parts) == 2:                       # logo (left) + text (right), same height
        txt, logo = parts[0], parts[1]
        h = max(txt.height, 1)
        logo = logo.resize((max(1, int(logo.width * h * 1.4 / logo.height)), int(h * 1.4)), Image.LANCZOS)
        gap = int(h * 0.3)
        canvas = Image.new("RGBA", (logo.width + gap + txt.width, max(logo.height, txt.height)), (0, 0, 0, 0))
        canvas.paste(logo, (0, (canvas.height - logo.height) // 2), logo)
        canvas.paste(txt, (logo.width + gap, (canvas.height - txt.height) // 2), txt)
        img = canvas
    else:
        img = parts[0]
    target_w = max(16, int(target_w))
    img = img.resize((target_w, max(1, int(img.height * target_w / img.width))), Image.LANCZOS)
    a = img.getchannel("A").point(lambda v: int(v * max(0, min(100, opacity)) / 100))
    img.putalpha(a)
    return img


def wm_xy(pos, W, H, w, h):
    """pos: ul uc ur / cl cc cr / dl dc dr  (U=up, C=centre, D=down; L, C, R).  Old tl/tr/bl/br/c still work."""
    pos = {"tl": "ul", "tr": "ur", "bl": "dl", "br": "dr", "c": "cc"}.get(pos, pos)
    if len(pos) != 2 or pos[0] not in "ucd" or pos[1] not in "lcr":
        pos = "dr"
    m = max(8, int(W * 0.02))
    x = {"l": m, "c": (W - w) // 2, "r": W - w - m}[pos[1]]
    y = {"u": m, "c": (H - h) // 2, "d": H - h - m}[pos[0]]
    return x, y


def process_logo_upload(raw):
    """Normalise an uploaded logo to a PNG (max 800px), return base64 str."""
    arr = cv2.imdecode(np.frombuffer(raw, np.uint8), cv2.IMREAD_UNCHANGED)
    if arr is None:
        raise ValueError("Not a valid image")
    h, w = arr.shape[:2]
    s = 800 / max(h, w)
    if s < 1:
        arr = cv2.resize(arr, (int(w * s), int(h * s)), interpolation=cv2.INTER_AREA)
    ok, buf = cv2.imencode(".png", arr)
    return base64.b64encode(buf.tobytes()).decode()


def process_thumb_bytes(raw):
    """Telegram thumbnails: JPEG, max 320px, < 200 KB."""
    arr = cv2.imdecode(np.frombuffer(raw, np.uint8), cv2.IMREAD_COLOR)
    if arr is None:
        raise ValueError("Not a valid image")
    h, w = arr.shape[:2]
    s = 320 / max(h, w)
    if s < 1:
        arr = cv2.resize(arr, (int(w * s), int(h * s)), interpolation=cv2.INTER_AREA)
    for q in (88, 75, 60, 45):
        ok, buf = cv2.imencode(".jpg", arr, [cv2.IMWRITE_JPEG_QUALITY, q])
        if len(buf) < 195 * 1024:
            break
    return buf.tobytes()


# ------------------------------------------------------------------ ffmpeg / ffprobe
async def probe_file(path):
    proc = await asyncio.create_subprocess_exec(
        FFPROBE, "-v", "error", "-print_format", "json", "-show_streams", "-show_format", path,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    out, _ = await proc.communicate()
    data = json.loads(out or b"{}")
    v = next((s for s in data.get("streams", []) if s.get("codec_type") == "video"), {})
    fmt = data.get("format", {})
    dur = float(fmt.get("duration") or v.get("duration") or 0)
    return {"width": int(v.get("width") or 0), "height": int(v.get("height") or 0),
            "duration": dur, "size": int(fmt.get("size") or os.path.getsize(path))}


async def run_ffmpeg(cmd, duration, on_progress=None, cancel_ev=None):
    """Run ffmpeg with -progress pipe:1 and report 0..100. Returns (returncode, stderr_text)."""
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    err_task = asyncio.create_task(proc.stderr.read())
    while True:
        if cancel_ev is not None and cancel_ev.is_set():
            proc.kill()
            break
        try:
            line = await asyncio.wait_for(proc.stdout.readline(), timeout=2)
        except asyncio.TimeoutError:
            continue
        if not line:
            break
        line = line.decode(errors="ignore").strip()
        if line.startswith(("out_time_us=", "out_time_ms=")) and duration and on_progress:
            try:
                t = int(line.split("=", 1)[1]) / 1_000_000
                on_progress(min(100.0, t * 100 / duration))
            except ValueError:
                pass
    await proc.wait()
    err = (await err_task).decode(errors="ignore")
    return proc.returncode, err


async def watermark_video(src, dst, wm_png, x, y, duration, on_progress=None, cancel_ev=None):
    base = [FFMPEG, "-y", "-hide_banner", "-loglevel", "error", "-i", src, "-i", wm_png,
            "-filter_complex", f"[0:v][1:v]overlay={x}:{y}:format=auto,format=yuv420p[v]",
            "-map", "[v]", "-map", "0:a?", "-c:v", "libx264", "-preset", PRESET, "-crf", CRF]
    tail = ["-movflags", "+faststart", "-progress", "pipe:1", "-nostats", dst]
    rc, err = await run_ffmpeg(base + ["-c:a", "copy"] + tail, duration, on_progress, cancel_ev)
    if rc != 0 and not (cancel_ev and cancel_ev.is_set()):
        rc, err = await run_ffmpeg(base + ["-c:a", "aac", "-b:a", "128k"] + tail, duration, on_progress, cancel_ev)
    if rc != 0:
        raise RuntimeError("ffmpeg watermark failed: " + err[-300:])


async def to_mp3(src, dst, kbps, duration, on_progress=None, cancel_ev=None, title=""):
    cmd = [FFMPEG, "-y", "-hide_banner", "-loglevel", "error", "-i", src, "-vn", "-c:a", "libmp3lame",
           "-b:a", f"{int(kbps)}k", "-metadata", f"title={title[:100]}", "-progress", "pipe:1", "-nostats", dst]
    rc, err = await run_ffmpeg(cmd, duration, on_progress, cancel_ev)
    if rc != 0:
        raise RuntimeError("ffmpeg mp3 failed: " + err[-300:])


async def make_thumbnail(src, out_jpg, duration, custom_bytes=None):
    if custom_bytes:
        with open(out_jpg, "wb") as f:
            f.write(process_thumb_bytes(custom_bytes))
        return out_jpg
    at = max(1, int((duration or 20) * 0.1))
    proc = await asyncio.create_subprocess_exec(
        FFMPEG, "-y", "-hide_banner", "-loglevel", "error", "-ss", str(at), "-i", src,
        "-frames:v", "1", "-vf", "scale=320:-2", "-q:v", "4", out_jpg,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    await proc.wait()
    if os.path.exists(out_jpg):
        with open(out_jpg, "rb") as f:
            data = f.read()
        with open(out_jpg, "wb") as f:
            f.write(process_thumb_bytes(data))
        return out_jpg
    return None


async def split_video(src, outdir, duration, max_bytes=None, cancel_ev=None):
    """Split into parts under max_bytes (stream copy). Returns a list of file paths."""
    max_bytes = max_bytes or MAX_TG_BYTES
    size = os.path.getsize(src)
    if size <= max_bytes:
        return [src]
    n = math.ceil(size / (max_bytes * 0.92))
    for _ in range(4):
        pattern = os.path.join(outdir, "part%03d.mp4")
        for old in os.listdir(outdir):
            if old.startswith("part") and old.endswith(".mp4"):
                os.remove(os.path.join(outdir, old))
        seg = max(5, duration / n)
        rc, err = await run_ffmpeg(
            [FFMPEG, "-y", "-hide_banner", "-loglevel", "error", "-i", src, "-map", "0", "-c", "copy",
             "-f", "segment", "-segment_time", f"{seg:.2f}", "-reset_timestamps", "1",
             "-segment_format", "mp4", "-segment_format_options", "movflags=+faststart",
             "-progress", "pipe:1", "-nostats", pattern], duration, None, cancel_ev)
        if rc != 0:
            raise RuntimeError("ffmpeg split failed: " + err[-300:])
        parts = sorted(os.path.join(outdir, f) for f in os.listdir(outdir)
                       if f.startswith("part") and f.endswith(".mp4"))
        if parts and all(os.path.getsize(p) <= max_bytes for p in parts):
            return parts
        n += 1
    raise RuntimeError("Could not split the video into small enough parts")


# ------------------------------------------------------------------ yt-dlp
class _SilentLogger:
    def debug(self, msg): pass
    def info(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): pass


def _headers(referer):
    h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"}
    if referer:
        h["Referer"] = referer
        p = urlparse(referer)
        if p.scheme and p.netloc:
            h["Origin"] = f"{p.scheme}://{p.netloc}"
    return h


def _valid_cookie_lines(text):
    return [l for l in (text or "").splitlines() if l.strip() and not l.lstrip().startswith("#")
            and len(l.split(None, 6)) >= 7]


def make_cookie_file(name=None):
    """Return the path of a TEMP Netscape-format cookie file, or None when no valid cookies exist.
    Sources (first valid wins): config value `name` (YT_COOKIES / INSTA_COOKIES), then a
    cookies.txt in the project folder (or COOKIES_FILE=...). Temp copy => your original is never modified."""
    text = ""
    if name:
        try:
            import config as _cfg
            text = getattr(_cfg, name, "") or ""
        except Exception:
            text = ""
    if not _valid_cookie_lines(text):
        for cand in (os.getenv("COOKIES_FILE", ""), os.path.join(_BASE, "cookies.txt"), "cookies.txt"):
            if cand and os.path.isfile(cand):
                with open(cand, encoding="utf-8", errors="ignore") as f:
                    text = f.read()
                if _valid_cookie_lines(text):
                    break
    lines = _valid_cookie_lines(text)
    if not lines:
        return None
    body = "\n".join("\t".join(l.split(None, 6)) for l in lines)       # spaces -> tabs (env vars lose tabs)
    fd, path = tempfile.mkstemp(suffix=".txt", prefix="ck_")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write("# Netscape HTTP Cookie File\n" + body + "\n")
    return path


def split_referers(text):
    """The Referer setting may hold several website addresses (space / comma / new line separated)."""
    return [x for x in re.split(r"[\s,]+", text or "") if x.lower().startswith("http")]


BUNNY_REFERERS = ["https://iframe.mediadelivery.net/", "https://player.mediadelivery.net/"]
EXTRA_REFERERS = split_referers(os.getenv("EXTRA_REFERERS", ""))        # optional, owner-level (env only)


def link_expiry(url):
    """Unix time at which a signed link stops working (expires= / exp= ... in the query), or None."""
    from urllib.parse import parse_qs
    q = {k.lower(): v for k, v in parse_qs(urlparse(url).query).items()}
    for k in ("expires", "expire", "exp", "e", "expiry", "expires_at"):
        v = (q.get(k) or [""])[0]
        if v.isdigit() and len(v) in (10, 13):
            t = int(v)
            return t // 1000 if len(v) == 13 else t
    return None


def _referer_ok(url, ref, timeout):
    import urllib.request
    try:
        req = urllib.request.Request(url, headers=_headers(ref))
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read(512)
            if r.status >= 400:
                return False
            if ".m3u8" in url.lower() or "mpegurl" in (r.headers.get("Content-Type") or "").lower():
                return b"#EXT" in body          # a real playlist, not a 200 error page
            return True
    except Exception:
        return False


def referer_candidates(url, saved_text="", learned=None):
    """Everything worth trying, best first. No setup needed from the user."""
    from urllib.parse import parse_qs
    u = urlparse(url)
    host = u.netloc.lower()
    own = f"{u.scheme}://{host}/"
    learned = learned or {}
    raw = [learned.get(host.replace(".", "|")), "", own]
    for k, v in parse_qs(u.query).items():                  # some links carry their site: ?referer=... / ?origin=...
        if k.lower() in ("referer", "referrer", "ref", "origin", "site", "domain") and v:
            val = v[0].strip()
            raw.append(val if val.lower().startswith("http") else f"https://{val}/")
    raw += split_referers(saved_text) + EXTRA_REFERERS
    raw += [x for x in learned.values() if isinstance(x, str)]  # a site that worked before often owns several CDNs
    if host.endswith("b-cdn.net") or host.endswith("mediadelivery.net"):
        raw += BUNNY_REFERERS
    parts = host.split(".")
    if len(parts) > 2:
        raw.append(f"{u.scheme}://{'.'.join(parts[-2:])}/")
    out = []
    for c in raw:
        if c is not None and c not in out:
            out.append(c)
    return out


def pick_referer(url, saved_text="", learned=None, timeout=10):
    """Find which Referer this link's server accepts, like a browser would: all candidates are tried at the
    same time and the best working one wins. Returns (referer, worked)."""
    from concurrent.futures import ThreadPoolExecutor
    cands = referer_candidates(url, saved_text, learned)
    ex = ThreadPoolExecutor(max_workers=min(8, len(cands)))
    try:
        futs = [ex.submit(_referer_ok, url, c, timeout) for c in cands]
        for c, f in zip(cands, futs):
            if f.result():
                return c, True
    finally:
        ex.shutdown(wait=False)
    return "", False


def probe_url_sync(url, referer=""):
    opts = {"quiet": True, "no_warnings": True, "noplaylist": True, "http_headers": _headers(referer),
            "skip_download": True, "logger": _SilentLogger()}
    ck = make_cookie_file()
    if ck:
        opts["cookiefile"] = ck
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
    finally:
        if ck and os.path.exists(ck):
            os.remove(ck)
    if info.get("_type") == "playlist" and info.get("entries"):
        info = info["entries"][0]
    dur = info.get("duration") or 0
    best = {}
    for f in info.get("formats", []):
        h = f.get("height")
        if not h or f.get("vcodec") == "none":
            continue
        size, est = f.get("filesize") or f.get("filesize_approx"), False
        if not size and f.get("tbr") and dur:
            size, est = int(f["tbr"] * 1000 / 8 * dur), True       # bitrate x duration
        cur = best.get(int(h))
        if cur is None or (size or 0) > (cur["size"] or 0):
            best[int(h)] = {"height": int(h), "width": f.get("width") or 0, "ext": f.get("ext") or "mp4",
                            "size": size, "est": est or bool(f.get("filesize_approx"))}
    formats = [best[h] for h in sorted(best, reverse=True)]
    return {"title": info.get("title") or "", "duration": dur,
            "uploader": info.get("uploader") or info.get("channel") or "",
            "source": source_name(url, info.get("extractor_key")), "heights": [f["height"] for f in formats],
            "formats": formats}


def _to_seconds(tok):
    tok = tok.strip().lower().replace(" ", "")
    if not tok:
        raise ValueError("empty time")
    if ":" in tok:
        parts = tok.split(":")
        if len(parts) > 3 or not all(p.isdigit() for p in parts):
            raise ValueError(tok)
        sec = 0
        for p in parts:
            sec = sec * 60 + int(p)
        return sec
    m = re.fullmatch(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s?)?", tok)
    if not m or not any(m.groups()):
        raise ValueError(tok)
    h, mi, s = (int(x or 0) for x in m.groups())
    return h * 3600 + mi * 60 + s


def parse_clip(text, duration=0):
    """'10:00-25:30', '1:05:00-1:20:00', '90-300' (seconds), '10:00-' (to the end), '10:00 to 25:30'.
    Returns (start_seconds, end_seconds | None). Raises ValueError with a message for the user."""
    t = (text or "").strip().lower().replace("–", "-").replace("—", "-")
    t = re.sub(r"\s+(to|till|until)\s+", "-", t)
    if t.count("-") != 1:
        raise ValueError("Send it like `10:00-25:30` (start-end). Use `10:00-` to go till the end.")
    a, b = t.split("-")
    try:
        start = _to_seconds(a) if a.strip() else 0
        end = _to_seconds(b) if b.strip() else None
    except ValueError:
        raise ValueError("I could not read that time. Use mm:ss or h:mm:ss, e.g. `10:00-25:30`.")
    if end is not None and end <= start:
        raise ValueError("The end time must be after the start time.")
    if duration and start >= duration:
        raise ValueError(f"The start is after the end of the video (it is {fmt_duration(duration)} long).")
    if duration and end is not None and end > duration:
        end = None                      # past the end: just go till the end
    if end is None and start == 0:
        raise ValueError("That is the whole video - choose 🎞 Full video instead.")
    return start, end


def fmt_clip(clip):
    if not clip:
        return "Full video"
    s, e = clip
    return f"{fmt_duration(s)} → {fmt_duration(e) if e else 'end'}"


def download_sync(url, outdir, quality, referer, on_progress, cancel_ev, clip=None):
    """Blocking download (run in a thread). Returns the path of the downloaded file.
    clip = (start_sec, end_sec | None) downloads only that part (needs ffmpeg)."""
    if quality == "best":
        fmt = "bv*+ba/b"
    elif quality == "audio":
        fmt = "ba/b"
    else:
        h = int(quality)
        fmt = f"bv*[height<={h}]+ba/b[height<={h}]/b"

    def hook(d):
        if cancel_ev.is_set():
            raise yt_dlp.utils.DownloadCancelled()
        if d.get("status") == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            done = d.get("downloaded_bytes") or 0
            pct = (done * 100 / total) if total else 0
            if d.get("fragment_count"):
                pct = (d.get("fragment_index") or 0) * 100 / d["fragment_count"]
            on_progress(pct, d.get("speed"), d.get("eta"), done, total)

    opts = {"format": fmt, "outtmpl": os.path.join(outdir, "src.%(ext)s"), "merge_output_format": "mp4",
            "noplaylist": True, "quiet": True, "no_warnings": True, "noprogress": True, "retries": 3, "fragment_retries": 3, "socket_timeout": 25,
            "concurrent_fragment_downloads": DL_CONCURRENCY, "logger": _SilentLogger(), "progress_hooks": [hook], "http_headers": _headers(referer)}
    if os.path.dirname(shutil.which(FFMPEG) or ""):
        opts["ffmpeg_location"] = os.path.dirname(shutil.which(FFMPEG))
    if clip:
        cs, ce = clip
        opts["download_ranges"] = yt_dlp.utils.download_range_func(None, [(float(cs), float(ce) if ce else float("inf"))])
        if os.getenv("CLIP_ACCURATE", "") == "1":        # frame-accurate cuts, but re-encodes (slow on small servers)
            opts["force_keyframes_at_cuts"] = True
    ck = make_cookie_file()
    if ck:
        opts["cookiefile"] = ck
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.extract_info(url, download=True)
    finally:
        if ck and os.path.exists(ck):
            os.remove(ck)
    files = [os.path.join(outdir, f) for f in os.listdir(outdir) if f.startswith("src")      # sections may add a suffix
             and not f.endswith((".part", ".ytdl", ".json"))]
    if not files:
        raise RuntimeError("Download finished but no file was found")
    best = max(files, key=os.path.getsize)
    if clip and os.path.getsize(best) < 20_000:
        raise RuntimeError("Could not cut that part from this link (the server does not allow seeking). Choose 🎞 Full video instead.")
    return best


def cleanup_stale(max_age_hours=24):
    if not os.path.isdir(WORK_DIR):
        return
    for d in os.listdir(WORK_DIR):
        p = os.path.join(WORK_DIR, d)
        try:
            if time.time() - os.path.getmtime(p) > max_age_hours * 3600:
                shutil.rmtree(p, ignore_errors=True)
        except OSError:
            pass


def is_youtube(url):
    h = urlparse(url).netloc.lower()
    return any(h == d or h.endswith("." + d) for d in ("youtube.com", "youtu.be", "youtube-nocookie.com"))


def link_kind(url):
    """Short label shown in 'link detected': YouTube / HLS stream / Direct file / Web page."""
    path = urlparse(url).path.lower()
    if is_youtube(url):
        return "YouTube"
    if path.endswith(".m3u8") or ".m3u8" in url.lower():
        return "HLS stream (m3u8)"
    if path.endswith((".mp4", ".mkv", ".webm", ".mov", ".m4v", ".avi", ".mp3", ".m4a")):
        return "Direct file"
    return "Web video"
