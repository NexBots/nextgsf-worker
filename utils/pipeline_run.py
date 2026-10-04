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

STAGE = {"prepare": "🔎 Preparing…", "download": "📥 Downloading file…", "encode": "🎨 Adding watermark…",
         "convert": "🎧 Converting to MP3…", "split": "✂️ Splitting (file is over 2 GB)…",
         "upload": "📤 Uploading to Telegram…", "yt": "▶️ Uploading to YouTube…", "gd": "☁️ Uploading to Google Drive…", "queued": "⏳ Queued"}
BRAND = os.getenv("BRAND_NAME") or "NexTGSF"


class JobCancelled(Exception):
    pass


class Job:
    def __init__(self, uid, chat_id, links, quality, wm_on, msg=None, job_id=None, msg_id=None, opts=None):
        self.id = job_id or uuid.uuid4().hex[:10]
        self.uid, self.chat_id, self.links = uid, chat_id, [tuple(x) for x in links]
        self.quality, self.wm_on, self.msg = quality, wm_on, msg
        self.opts = opts or {}
        self.outputs = []          # extra result lines, e.g. YouTube links
        self.msg_id = msg_id or getattr(msg, "id", None)
        self.cancel = threading.Event()
        self.idx, self.total = 0, len(self.links)
        self.title, self.stage, self.pct, self.extra = "", "queued", 0.0, ""
        self.t0, self.done_b, self.total_b, self.speed, self.eta, self.speed_txt = time.time(), 0, 0, None, None, ""
        self.ok, self.fail, self.notes, self.done = 0, [], [], False
        self.start = time.time()

    def to_doc(self):
        return {"_id": self.id, "uid": self.uid, "chat_id": self.chat_id, "msg_id": self.msg_id,
                "links": [list(x) for x in self.links], "quality": self.quality, "wm_on": self.wm_on, "opts": self.opts}

    @classmethod
    def from_doc(cls, d):
        return cls(d["uid"], d["chat_id"], d["links"], d["quality"], d["wm_on"],
                   job_id=d["_id"], msg_id=d.get("msg_id"), opts=d.get("opts"))


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
def set_stage(job, stage):
    job.stage, job.pct, job.extra = stage, 0.0, ""
    job.t0, job.done_b, job.total_b, job.speed, job.eta, job.speed_txt = time.time(), 0, 0, None, None, ""


def update_bytes(job, done, total):
    """Upload-style progress: derive speed and ETA from bytes and elapsed time."""
    el = max(time.time() - job.t0, 0.001)
    job.done_b, job.total_b = done, total
    job.pct = done * 100 / total if total else 0
    job.speed = done / el
    job.eta = (total - done) / job.speed if job.speed and total else None


def update_media(job, pct, media_secs):
    """Encode progress: 'speed' is how many seconds of video are processed per second (e.g. 1.8x)."""
    el = max(time.time() - job.t0, 0.001)
    job.pct = pct
    job.speed_txt = f"{media_secs * pct / 100 / el:.1f}x"
    job.eta = el * (100 - pct) / pct if pct > 1 else None


def render_progress(job):
    lines = [STAGE.get(job.stage, job.stage), "", f"{pc.bar(job.pct, 12)}  {job.pct:.2f}%", ""]
    if job.total_b:
        lines.append(f"📦 Size: {pc.fmt_size(job.done_b)} / {pc.fmt_size(job.total_b)}")
    spd = job.speed_txt or (f"{pc.fmt_size(job.speed)}/s" if job.speed else "")
    if spd or job.eta is not None:
        lines.append(f"⚡ Speed: {spd or '--'}  |  ⏳ ETA: {pc.fmt_eta(job.eta)}")
    if job.extra:
        lines.append(job.extra)
    el = int(time.time() - job.start)
    lines += ["", f"🎬 {job.idx}/{job.total}" + (f" · {job.title[:45]}" if job.title else ""),
              f"🕒 Total time: {el // 60}:{el % 60:02d}", f"⚡ Powered by {BRAND}"]
    return "\n".join(lines)


async def progress_loop(job, ctx, every=4):
    last = ""
    while not job.done:
        text = render_progress(job)
        if text != last:
            await ctx.edit(job, text)
            last = text
        await asyncio.sleep(every)


async def run_cancellable(job, fn, *args):
    """Run a blocking function in a thread but give up at once when the user presses Cancel."""
    loop = asyncio.get_running_loop()
    fut = loop.run_in_executor(None, fn, *args)
    fut.add_done_callback(lambda f: f.cancelled() or f.exception())     # silence 'never retrieved'
    while True:
        done, _ = await asyncio.wait({fut}, timeout=1)
        if done:
            return fut.result()
        if job.cancel.is_set():
            raise JobCancelled()


def fwd_targets(job, cfg):
    mode = (job.opts or {}).get("fwd_mode", "default")
    if mode == "none":
        return []
    if mode == "custom":
        return [int(x) for x in (job.opts.get("fwd") or [])]
    return [int(cfg["fwd_chat"])] if cfg.get("fwd_chat") else []


def gd_upload_sync(cfg, path, name, on_pct, cancel_ev):
    """Blocking: upload to the user's connected Google Drive. Returns (file_id, link, folder_id)."""
    from utils import gdrive_api as gd
    from utils.encrypt import dcs
    if not gd.configured():
        raise RuntimeError("Google Drive is not set up on the server (YT_CLIENT_ID / YT_CLIENT_SECRET missing)")
    if not cfg.get("gd_refresh"):
        raise RuntimeError("Google Drive is not connected. Send /gdconnect first")
    refresh = dcs(cfg["gd_refresh"])
    get_token = lambda: gd.refresh_access_token(refresh)[0]          # noqa: E731
    folder = cfg.get("gd_folder") or ""
    if not folder:
        folder = gd.ensure_folder(get_token())
    try:
        fid, link = gd.upload_file(get_token, path, name, folder, "video/mp4", on_progress=on_pct, cancel_ev=cancel_ev)
    except gd.GDError as e:
        if folder and getattr(e, "reason", "") == "notFound":      # cached folder was deleted: recreate once
            folder = gd.ensure_folder(get_token())
            fid, link = gd.upload_file(get_token, path, name, folder, "video/mp4", on_progress=on_pct, cancel_ev=cancel_ev)
        else:
            raise
    return fid, link, folder


async def forward_all(ctx, job, cfg, sent):
    """Copy the delivered message to every chosen group/channel."""
    for t in fwd_targets(job, cfg):
        try:
            await ctx.client.copy_message(t, job.chat_id, sent.id)
        except Exception as e:
            note = f"Forward to {t} failed ({str(e)[:70]})"
            if note not in job.notes:
                job.notes.append(note)


def yt_upload_sync(cfg, path, title, desc, privacy, thumb, on_pct, cancel_ev):
    """Blocking: upload to the user's connected YouTube channel. Returns (video_id, note)."""
    from utils import youtube_api as ya
    from utils.encrypt import dcs
    if not ya.configured():
        raise RuntimeError("YouTube is not set up on the server (YT_CLIENT_ID / YT_CLIENT_SECRET missing)")
    if not cfg.get("yt_refresh"):
        raise RuntimeError("YouTube is not connected. Send /ytconnect first")
    refresh = dcs(cfg["yt_refresh"])
    get_token = lambda: ya.refresh_access_token(refresh)[0]          # noqa: E731
    vid = ya.upload_video(get_token, path, title, desc, privacy, on_progress=on_pct, cancel_ev=cancel_ev)
    note = ""
    if thumb and os.path.exists(thumb):
        try:
            ya.set_thumbnail(get_token(), vid, thumb)
        except Exception as e:
            note = f"YouTube thumbnail not set ({str(e)[:60]})"
    return vid, note


# ------------------------------------------------------------ one video
async def process_one(job, ctx, cfg, n, url, title_override, logo, custom_thumb, wm_active):
    loop = asyncio.get_running_loop()
    audio = job.quality.startswith("mp3:")
    kbps = int(job.quality.split(":")[1]) if audio else 0
    workdir = os.path.join(pc.WORK_DIR, f"{job.uid}_{job.id}_{job.idx}")
    os.makedirs(workdir, exist_ok=True)
    try:
        set_stage(job, "prepare")
        learned = dict(cfg.get("referer_map") or {})
        host = urlparse(url).netloc.lower()
        ref, worked = await run_cancellable(job, pc.pick_referer, url, cfg["referer"], learned)
        if worked and ref and learned.get(host.replace(".", "|")) != ref:
            learned[host.replace(".", "|")] = ref
            try:
                await ctx.set_cfg(job.uid, referer_map=learned)
            except Exception:
                pass
        try:
            info = await run_cancellable(job, pc.probe_url_sync, url, ref)
        except JobCancelled:
            raise
        except Exception:
            info = {"title": "", "duration": 0, "uploader": "", "source": urlparse(url).netloc, "heights": []}
        o = job.opts or {}
        tm = o.get("title_mode", "fetched")
        if tm == "custom" and (o.get("title") or "").strip():
            raw_title = o["title"].strip() + (f" {job.idx}" if job.total > 1 else "")
        elif tm == "skip":
            raw_title = f"{'yt video' if pc.is_youtube(url) else 'video'} {datetime.now().strftime('%d%m%Y')}"
            if job.total > 1:
                raw_title += f" {job.idx}"
        else:
            raw_title = title_override or info["title"]
            if pc.is_weak_title(raw_title):
                raw_title = f"Class {n}"
        job.title = raw_title

        # ---- download
        set_stage(job, "download")

        def dl_cb(pct, speed, eta, done=0, total=0):
            job.pct, job.speed, job.eta, job.done_b, job.total_b = pct, speed, eta, done, total

        try:
            clip = tuple(o["clip"]) if o.get("clip") else None
            path = await run_cancellable(job, pc.download_sync, url, workdir,
                                         "audio" if audio else job.quality, ref, dl_cb, job.cancel, clip)
        except JobCancelled:
            raise
        except Exception as e:
            if job.cancel.is_set():
                raise JobCancelled()
            raise RuntimeError(str(e).replace("ERROR: ", "")[:200])
        meta = await pc.probe_file(path)

        # ---- watermark (not for audio)
        if wm_active and not audio:
            img = pc.build_watermark_image(cfg["wm_mode"], cfg["wm_text"], logo,
                                           meta["width"] * cfg["wm_size"] / 100, cfg["wm_opacity"])
            if img is not None:
                set_stage(job, "encode")
                wm_png = os.path.join(workdir, "wm.png")
                img.save(wm_png)
                x, y = pc.wm_xy(cfg["wm_pos"], meta["width"], meta["height"], img.width, img.height)
                out = os.path.join(workdir, "wm_out.mp4")
                try:
                    await pc.watermark_video(path, out, wm_png, x, y, meta["duration"],
                                             lambda p: update_media(job, p, meta["duration"]), job.cancel)
                except Exception:
                    if job.cancel.is_set():
                        raise JobCancelled()
                    raise
                os.remove(path)
                path = out
                meta = await pc.probe_file(path)
        if job.cancel.is_set():
            raise JobCancelled()
        if audio:
            return await process_audio(job, ctx, cfg, n, url, raw_title, info, path, meta, kbps, custom_thumb, workdir)

        # ---- title / caption
        now = datetime.now()
        vals = {"title": raw_title, "n": n, "date": now.strftime("%Y-%m-%d"), "time": now.strftime("%H:%M:%S"),
                "duration": pc.fmt_duration(meta["duration"]), "res": f"{meta['height']}p",
                "source": info["source"], "uploader": info["uploader"], "url": url,
                "yt": "", "yt_title": "", "slide": o.get("slide", "")}
        final_title = pc.render_template(cfg["title_tpl"], vals) or raw_title
        filename = pc.sanitize_filename(final_title) + ".mp4"
        final_path = os.path.join(workdir, filename)
        os.replace(path, final_path)
        vals.update(title=final_title, filename=filename, filesize=pc.fmt_size(os.path.getsize(final_path)))
        job.title = final_title

        # ---- thumbnail
        thumb = await pc.make_thumbnail(final_path, os.path.join(workdir, "thumb.jpg"), meta["duration"], custom_thumb)

        # ---- YouTube (optional)
        dest = o.get("dest", "tg")
        if dest in ("yt", "both"):
            set_stage(job, "yt")

            def yt_pct(pct):
                job.pct = pct

            desc = pc.render_template(cfg["desc_tpl"], vals) if cfg.get("desc_tpl") else ""
            if vals["slide"] and "{slide}" not in (cfg.get("desc_tpl") or ""):
                desc = (desc + "\n\n" if desc else "") + f"Slide: {vals['slide']}"
            vid, ynote = await run_cancellable(job, yt_upload_sync, cfg, final_path, final_title, desc,
                                               cfg["yt_privacy"], thumb, yt_pct, job.cancel)
            vals.update(yt=f"https://youtu.be/{vid}", yt_title=final_title)
            job.outputs.append(f"▶️ YouTube: https://youtu.be/{vid} ({cfg['yt_privacy']})")
            if ynote:
                job.notes.append(ynote)

        # ---- Google Drive (optional)
        if dest in ("gd", "tg_gd"):
            set_stage(job, "gd")

            def gd_pct(pct):
                job.pct = pct

            fid, glink, gfolder = await run_cancellable(job, gd_upload_sync, cfg, final_path, filename, gd_pct, job.cancel)
            if gfolder and gfolder != cfg.get("gd_folder"):
                try:
                    await ctx.set_cfg(job.uid, gd_folder=gfolder)
                except Exception:
                    pass
            vals.update(gd=glink)
            job.outputs.append(f"☁️ Google Drive: {glink}")

        caption_tpl = cfg["caption_tpl"]
        if vals["slide"] and "{slide}" not in caption_tpl:
            caption_tpl += "\n📎 Slide: {slide}"
        if vals["yt"] and "{yt}" not in caption_tpl:
            caption_tpl += "\n▶️ {yt}"
        caption = pc.render_template(caption_tpl, vals)
        if dest in ("yt", "gd"):
            return                                   # no Telegram delivery for these destinations

        # ---- split
        if os.path.getsize(final_path) > pc.MAX_TG_BYTES:
            set_stage(job, "split")
            parts = await pc.split_video(final_path, workdir, meta["duration"], cancel_ev=job.cancel)
        else:
            parts = [final_path]

        # ---- upload
        for pi, part in enumerate(parts, 1):
            if job.cancel.is_set():
                raise JobCancelled()
            pm = meta if len(parts) == 1 else await pc.probe_file(part)
            cap = caption + (f"\n\n📦 Part {pi}/{len(parts)}" if len(parts) > 1 else "")
            set_stage(job, "upload")
            job.extra = f"📦 Part {pi}/{len(parts)}" if len(parts) > 1 else ""

            def up_cb(cur, tot):
                update_bytes(job, cur, tot)
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
            await forward_all(ctx, job, cfg, sent)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


async def process_audio(job, ctx, cfg, n, url, raw_title, info, src, meta, kbps, custom_thumb, workdir):
    set_stage(job, "convert")
    now = datetime.now()
    vals = {"title": raw_title, "n": n, "date": now.strftime("%Y-%m-%d"), "time": now.strftime("%H:%M:%S"),
            "duration": pc.fmt_duration(meta["duration"]), "res": f"MP3 {kbps}k", "source": info["source"],
            "uploader": info["uploader"], "url": url, "yt": "", "yt_title": "", "slide": ""}
    final_title = pc.render_template(cfg["title_tpl"], vals) or raw_title
    filename = pc.sanitize_filename(final_title) + ".mp3"
    out = os.path.join(workdir, filename)
    await pc.to_mp3(src, out, kbps, meta["duration"], lambda p: update_media(job, p, meta["duration"]),
                    job.cancel, final_title)
    if job.cancel.is_set():
        raise JobCancelled()
    vals.update(title=final_title, filename=filename, filesize=pc.fmt_size(os.path.getsize(out)))
    cap = pc.render_template(cfg["caption_tpl"], vals)[:1024]
    thumb = None
    if custom_thumb:
        thumb = await pc.make_thumbnail(out, os.path.join(workdir, "thumb.jpg"), 0, custom_thumb)
    job.title = final_title
    set_stage(job, "upload")

    def up_cb(cur, tot):
        update_bytes(job, cur, tot)
        if job.cancel.is_set():
            ctx.client.stop_transmission()

    try:
        sent = await ctx.client.send_audio(job.chat_id, out, caption=cap, duration=int(meta["duration"]),
                                           title=final_title[:64], file_name=filename, thumb=thumb, progress=up_cb)
    except Exception as e:
        if job.cancel.is_set():
            raise JobCancelled()
        raise RuntimeError(f"Upload failed: {str(e)[:150]}")
    await forward_all(ctx, job, cfg, sent)


# ------------------------------------------------------------ a whole batch
def friendly(why):
    w = (why or "").lower()
    if "expired" in w:
        return "the link has expired, get a fresh link from the site"
    if "403" in w or "forbidden" in w:
        return "the server refused access (403) — usually an expired or site-locked link"
    if "404" in w or "not found" in w:
        return "the link does not exist any more (404)"
    if "name resolution" in w or "timed out" in w or "timeout" in w or "connection" in w:
        return "network problem, try again in a minute"
    if "unsupported url" in w:
        return "this site/link type is not supported"
    if "ffmpeg" in w:
        return "video conversion failed"
    return why[:90]


async def run_job(job, ctx):
    if job.cancel.is_set():
        await ctx.edit(job, "⏹ Cancelled.", final=True)
        return
    cfg = dict(await ctx.get_cfg(job.uid))
    o = job.opts or {}
    logo = base64.b64decode(cfg["wm_logo_b64"]) if cfg["wm_logo_b64"] else None
    custom_thumb = base64.b64decode(cfg["thumb_b64"]) if cfg["thumb_b64"] else None
    wm_active = job.wm_on and wm_ready(cfg)
    wmo = o.get("wm") or {}
    if wmo.get("mode") == "skip":
        wm_active = False
    elif wmo.get("mode") == "custom" and wmo.get("logo_b64"):
        logo = base64.b64decode(wmo["logo_b64"])
        cfg.update(wm_mode="logo", wm_text="", wm_size=int(wmo.get("size", 15)),
                   wm_opacity=int(wmo.get("opacity", 70)), wm_pos=wmo.get("pos", "dr"))
        wm_active = True
    if o.get("thumb") == "skip":
        custom_thumb = None
    elif o.get("thumb") == "custom" and o.get("thumb_b64"):
        custom_thumb = base64.b64decode(o["thumb_b64"])
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
        el = int(time.time() - job.start)
        text = (("🎉 All done!" if not job.fail else "⚠️ Finished with problems") + f"\n\n✅ Sent: {job.ok} video(s)"
                + (f"\n❌ Failed: {len(job.fail)}" if job.fail else "") + f"\n🕒 Took: {el // 60}:{el % 60:02d}")
        for line in job.outputs:
            text += f"\n{line}"
        for u, why in job.fail[:5]:
            text += f"\n\n• {u[:50]}…\n  ↳ {friendly(why)}"
        if any("403" in w or "orbidden" in w for _, w in job.fail):
            text += "\n\n💡 403 usually means the link expired or is locked to one website. Get a fresh link from the site and try again."
    for note in job.notes:
        text += f"\n⚠️ {note}"
    await ctx.edit(job, text, final=True)
    return cancelled
