# Cloud worker. Runs inside GitHub Actions:   python worker.py <job_id>
# Reads the job (links, quality, ...) from MongoDB, downloads -> watermarks -> uploads to Telegram,
# edits the progress message, and writes its heartbeat back so the bot can see it is alive.
# NOTE: logging is disabled on purpose: the Actions log of a public repo is public.
import asyncio
import logging
import os
import sys
import time

logging.disable(logging.CRITICAL)

from utils import pipeline_run as pr  # noqa: E402


class WorkerCtx:
    stats = None

    def __init__(self, client, users):
        self.client, self.users = client, users

    async def edit(self, job, text, final=False):
        try:
            from pyrogram.enums import ParseMode
            from pyrogram.types import InlineKeyboardMarkup as IKM, InlineKeyboardButton as IK
            kb = None if final else IKM([[IK("⏹ Cancel", callback_data=f"pl_cx:{job.id}")]])
            await self.client.edit_message_text(job.chat_id, job.msg_id, text,
                                                reply_markup=kb, parse_mode=ParseMode.DISABLED)
        except Exception:
            pass          # "message not modified", flood wait, ... never kill the job for a cosmetic edit

    async def get_cfg(self, uid):
        return await pr.load_cfg(self.users, uid)

    async def set_cfg(self, uid, **kw):
        await pr.save_cfg(self.users, uid, **kw)


async def run_worker(job_id, client, users, jobs, poll_every=3):
    doc = await jobs.find_one({"_id": job_id})
    if not doc:
        print("job not found")
        return 1
    if doc.get("status") != "queued":
        print("job is not queued (already started or finished)")
        return 0
    await jobs.update_one({"_id": job_id}, {"$set": {"status": "running", "hb": time.time()}})
    job = pr.Job.from_doc(doc)
    ctx = WorkerCtx(client, users)

    async def monitor():
        while not job.done:
            d = await jobs.find_one({"_id": job_id}) or {}
            if d.get("cancel"):
                job.cancel.set()
            await jobs.update_one({"_id": job_id}, {"$set": {"hb": time.time(), "stage": job.stage,
                                                             "pct": round(job.pct, 1), "idx": job.idx}})
            await asyncio.sleep(poll_every)

    mon = asyncio.create_task(monitor())
    status = "done"
    try:
        cancelled = await pr.run_job(job, ctx)
        status = "cancelled" if cancelled else "done"
    except Exception as e:                      # should not happen: run_job handles item errors
        status = "failed"
        await ctx.edit(job, f"❌ Unexpected worker error: {str(e)[:150]}", final=True)
    finally:
        job.done = True
        mon.cancel()
    await jobs.update_one({"_id": job_id}, {"$set": {"status": status, "ok": job.ok,
                                                     "failed": len(job.fail), "hb": time.time()}})
    print(f"job finished: {status} (ok={job.ok}, failed={len(job.fail)})")
    return 0


async def main(job_id):
    from motor.motor_asyncio import AsyncIOMotorClient
    from pyrogram import Client
    db = AsyncIOMotorClient(os.environ["MONGO_DB"])[(os.getenv("DB_NAME") or "telegram_downloader")]
    client = Client("worker", api_id=int(os.environ["API_ID"]), api_hash=os.environ["API_HASH"],
                    bot_token=os.environ["BOT_TOKEN"], in_memory=True, no_updates=True)
    await client.start()
    try:
        return await run_worker(job_id, client, db["users"], db["pl_jobs"])
    finally:
        await client.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage: python worker.py <job_id>")
    sys.exit(asyncio.run(main(sys.argv[1])))
