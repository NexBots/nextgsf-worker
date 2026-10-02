# The job runner shared by the bot (local mode) and the cloud worker (GitHub Actions).
# It knows nothing about Telegram handlers: everything it needs comes through `ctx`:
#   ctx.client                  Pyrogram client used to send videos (send_video, stop_transmission)
#   await ctx.edit(job, text, final=False)   update the progress message
#   await ctx.get_cfg(uid) / await ctx.set_cfg(uid, **kw)
#   ctx.stats                   dict with "done" / "failed" counters (optional)
import asyncio
import base64
import logging
import os
import shutil
import threading
import time
import uuid
from datetime import datetime
from urllib.parse import urlparse

from utils import pipeline_core as pc

logger = logging.getLogger(__name__)

STAGE = {"prepare": "🔎 Preparing", "download": "⬇️ Downloading", "encode": "🎨 Adding watermark",
         "split": "✂️ Splitting (file is over 2 GB)", "upload": "⬆️ Uploading", "queued": "⏳ Queued"}


class JobCancelled(Exception):
    pass


class Job:
    def __init__(self, uid, chat_id, links, quality, wm_on, msg=None, job_id=None, msg_id=None):
        self.id = job_id or uuid.uuid4().hex[:10]
        self.uid, self.chat_id, self.links = uid, chat_id, [tuple(x) for x in links]
        self.quality, self.wm_on, self.msg = quality, wm_on, msg
        self.msg_id = msg_id or getattr(msg, "id", None)
        self.cancel = threading.Event()
        self.idx, self.total = 0, len(self.links)
        self.title, self.stage, self.pct, self.extra = "", "queued", 0.0, ""
        self.ok, self.fail, self.notes, self.done = 0, [], [], False

    def to_doc(self):
        return {"_id": self.id, "uid": self.uid, "chat_id": self.chat_id, "msg_id": self.msg_id,
                "links": [list(x) for x in self.links], "quality": self.quality, "wm_on": self.wm_on}

    @classmethod
    def from_doc(cls, d):
        return cls(d["uid"], d["chat_id"], d["links"], d["quality"], d["wm_on"],
                   job_id=d["_id"], msg_id=d.get("msg_id"))


# ------------------------------------------------------------ settings helpers (shared)
def wm_ready(cfg):
    has_t = bool((cfg["wm_text"] or "").strip())
    has_l = bool(cfg["wm_logo_b64"])
    m = cfg["wm_mode"]
    return (m == "text" and has_t) or (m == "logo" and has_l) or (m == "both" and (has_t or has_l))


async def load_cfg(users, uid):
    doc = await users.find_one({"user_id": uid}) or {}
    cfg = dict(pc.DEFAULTS)
    cfg.update(doc.get("pl") or {})
    return cfg


async def save_cfg(users, uid, **kw):
    await users.update_one({"user_id": uid}, {"$set": {f"pl.{k}": v for k, v in kw.items()}}, upsert=True)


# ------------------------------------------------------------ progress
def render_progress(job):
    head = f"🎬 {job.idx}/{job.total}" + (f" · {job.title[:50]}" if job.title else "")
    body = f"{STAGE.get(job.stage, job.stage)}\n{pc.bar(job.pct)} {job.pct:.0f}%"
    return head + "\n" + body + (f"\n{job.extra}" if job.extra else "")


async def progress_loop(job, ctx, every=4):
    last = ""
    while not job.done:
        text = render_progress(job)
        if text != last:
            await ctx.edit(job, text)
            last = text
        await asyncio.sleep(every)


# ------------------------------------------------------------ one video
async def process_one(job, ctx, cfg, n, url, title_override, logo, custom_thumb, wm_active):
    loop = asyncio.get_running_loop()
    workdir = os.path.join(pc.WORK_DIR, f"{job.uid}_{job.id}_{job.idx}")
    os.makedirs(workdir, exist_ok=True)
    try:
        job.stage, job.pct, job.extra = "prepare", 0, ""
        try:
            info = await loop.run_in_executor(None, pc.probe_url_sync, url, cfg["referer"])
        except Exception:
            info = {"title": "", "duration": 0, "uploader": "", "source": urlparse(url).netloc, "heights": []}
        raw_title = title_override or info["title"]
        if pc.is_weak_title(raw_title):
            raw_title = f"Class {n}"
        job.title = raw_title

        # ---- download
        job.stage, job.pct = "download", 0

        def dl_cb(pct, speed, eta):
            job.pct = pct
            job.extra = f"{pc.fmt_size(speed)}/s · ETA {pc.fmt_eta(eta)}" if speed else ""

        try:
            path = await loop.run_in_executor(None, pc.download_sync, url, workdir, job.quality,
                                              cfg["referer"], dl_cb, job.cancel)
        except Exception as e:
            if job.cancel.is_set():
                raise JobCancelled()
            raise RuntimeError(str(e).replace("ERROR: ", "")[:200])
        meta = await pc.probe_file(path)

        # ---- watermark
        if wm_active:
            img = pc.build_watermark_image(cfg["wm_mode"], cfg["wm_text"], logo,
                                           meta["width"] * cfg["wm_size"] / 100, cfg["wm_opacity"])
            if img is not None:
                job.stage, job.pct, job.extra = "encode", 0, ""
                wm_png = os.path.join(workdir, "wm.png")
                img.save(wm_png)
                x, y = pc.wm_xy(cfg["wm_pos"], meta["width"], meta["height"], img.width, img.height)
                out = os.path.join(workdir, "wm_out.mp4")
                try:
                    await pc.watermark_video(path, out, wm_png, x, y, meta["duration"],
                                             lambda p: setattr(job, "pct", p), job.cancel)
                except Exception:
                    if job.cancel.is_set():
                        raise JobCancelled()
                    raise
                os.remove(path)
                path = out
                meta = await pc.probe_file(path)
        if job.cancel.is_set():
            raise JobCancelled()

        # ---- title / caption
        now = datetime.now()
        vals = {"title": raw_title, "n": n, "date": now.strftime("%Y-%m-%d"), "time": now.strftime("%H:%M:%S"),
                "duration": pc.fmt_duration(meta["duration"]), "res": f"{meta['height']}p",
                "source": info["source"], "uploader": info["uploader"], "url": url,
                "yt": "", "yt_title": "", "slide": ""}
        final_title = pc.render_template(cfg["title_tpl"], vals) or raw_title
        filename = pc.sanitize_filename(final_title) + ".mp4"
        final_path = os.path.join(workdir, filename)
        os.replace(path, final_path)
        vals.update(title=final_title, filename=filename, filesize=pc.fmt_size(os.path.getsize(final_path)))
        caption = pc.render_template(cfg["caption_tpl"], vals)
        job.title = final_title

        # ---- thumbnail + split
        thumb = await pc.make_thumbnail(final_path, os.path.join(workdir, "thumb.jpg"), meta["duration"], custom_thumb)
        if os.path.getsize(final_path) > pc.MAX_TG_BYTES:
            job.stage, job.pct, job.extra = "split", 0, ""
            parts = await pc.split_video(final_path, workdir, meta["duration"], cancel_ev=job.cancel)
        else:
            parts = [final_path]

        # ---- upload
        for pi, part in enumerate(parts, 1):
            if job.cancel.is_set():
                raise JobCancelled()
            pm = meta if len(parts) == 1 else await pc.probe_file(part)
            cap = caption + (f"\n\n📦 Part {pi}/{len(parts)}" if len(parts) > 1 else "")
            job.stage, job.pct = "upload", 0
            job.extra = f"Part {pi}/{len(parts)}" if len(parts) > 1 else ""

            def up_cb(cur, tot):
                job.pct = cur * 100 / tot if tot else 0
                job.extra = (f"Part {pi}/{len(parts)} · " if len(parts) > 1 else "") + f"{pc.fmt_size(cur)} / {pc.fmt_size(tot)}"
                if job.cancel.is_set():
                    ctx.client.stop_transmission()

            try:
                sent = await ctx.client.send_video(
                    job.chat_id, part, caption=cap[:1024],
                    file_name=filename if len(parts) == 1 else f"{os.path.splitext(filename)[0]} part{pi}.mp4",
                    duration=int(pm["duration"]), width=pm["width"], height=pm["height"],
                    thumb=thumb, supports_streaming=True, progress=up_cb)
            except Exception as e:
                if job.cancel.is_set():
                    raise JobCancelled()
                raise RuntimeError(f"Upload failed: {str(e)[:150]}")
            if cfg["fwd_chat"]:
                try:
                    await ctx.client.send_video(int(cfg["fwd_chat"]), sent.video.file_id, caption=cap[:1024])
                except Exception as e:
                    note = f"Auto-forward failed ({str(e)[:80]})"
                    if note not in job.notes:
                        job.notes.append(note)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


# ------------------------------------------------------------ a whole batch
async def run_job(job, ctx):
    if job.cancel.is_set():
        await ctx.edit(job, "⏹ Cancelled.", final=True)
        return
    cfg = await ctx.get_cfg(job.uid)
    logo = base64.b64decode(cfg["wm_logo_b64"]) if cfg["wm_logo_b64"] else None
    custom_thumb = base64.b64decode(cfg["thumb_b64"]) if cfg["thumb_b64"] else None
    wm_active = job.wm_on and wm_ready(cfg)
    n = int(cfg["next_n"])
    stats = getattr(ctx, "stats", None)
    updater = asyncio.create_task(progress_loop(job, ctx))
    cancelled = False
    try:
        for i, (url, title_o) in enumerate(job.links, 1):
            if job.cancel.is_set():
                cancelled = True
                break
            job.idx = i
            try:
                await process_one(job, ctx, cfg, n, url, title_o, logo, custom_thumb, wm_active)
                job.ok += 1
                n += 1
                await ctx.set_cfg(job.uid, next_n=n)
                if stats is not None:
                    stats["done"] = stats.get("done", 0) + 1
            except JobCancelled:
                cancelled = True
                break
            except Exception as e:
                logger.error(f"job {job.id} item {i} failed: {e}")
                job.fail.append((url, str(e)[:150]))
                if stats is not None:
                    stats["failed"] = stats.get("failed", 0) + 1
    finally:
        job.done = True
        updater.cancel()

    if cancelled:
        text = f"⏹ Cancelled. {job.ok} video(s) were already sent."
    else:
        text = f"🎉 Batch complete!\n✅ Sent: {job.ok}\n❌ Failed: {len(job.fail)}"
        for u, why in job.fail[:5]:
            text += f"\n• {u[:60]}… — {why}"
    for note in job.notes:
        text += f"\n⚠️ {note}"
    await ctx.edit(job, text, final=True)
    return cancelled
