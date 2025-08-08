#!/usr/bin/env python3

import redis
import json
import threading
import yaml
import os
import psutil
import time
from datetime import datetime
from pathlib import Path
from job_handlers import get_handler_for_type
from shared.log_mod import get_logger
from zoneinfo import ZoneInfo


nyc_tz = ZoneInfo("America/New_York")
CONFIG_PATH = os.path.expanduser("~/.config/Cluster-Worker/cluster-config.yaml")

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

# 🔧 Configuration
WORKER_NAME = config["worker_name"]
SHARED_ROOT = config["shared_root"]
REDIS_HOST = config["redis_host"]
REDIS_PORT = config.get("redis_port", 6379)
REDIS_QUEUE = config.get("redis_queue", "tasks")
LOG_FILE = config.get("log_file", "/mnt/cluster/cluster.log")
JOB_CAPABILITIES = config.get("capabilities")
CONTROL_CHANNEL = f"worker_control:{WORKER_NAME}"

# Event to control pause/resume behavior
is_ready = threading.Event()
is_ready.set()  # Worker starts out as ready to work

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

logger = get_logger(config)

# 🛠 Job Handlers
def resolve_path(relative_path):
    return os.path.abspath(os.path.join(SHARED_ROOT, relative_path))

def test_job(job):
    logger.info(f"[{job['id']}] ✅ Test job complete.")
    return "done"

def should_accept_job(job):
    job_type = job.get("job_type")

    capability = next(
        (c for c in JOB_CAPABILITIES if c.get("name") == job_type),
        None
    )
    if not capability:
        logger.info(f"[{job.get('id')}] ❌ No capability found for job type '{job_type}'")
        return False
    
    cpu_requirement = capability.get("cpu_threshold", 25)
    mem_requiremenet = capability.get("min_memory_mb", 1000)

    current_cpu = psutil.cpu_percent(interval=1)
    current_mem_mb = psutil.virtual_memory().available / 1024 / 1024

    if current_cpu > cpu_requirement:
        logger.info(f"[{job.get('id')}] Skipping: CPU usage {current_cpu:.1f}% > limit {cpu_requirement}%")
        return False
    if current_mem_mb < mem_requiremenet:
        logger.info(f"[{job.get('id')}] Skipping: Memory available {int(current_mem_mb)}MB < required {mem_requiremenet}MB")
        return False    
    
    return True

def heartbeat_loop(node_name, job_id, stop_event):
    key = f"heartbeat:{node_name}"
    while not stop_event.is_set():
        now = datetime.now(nyc_tz).isoformat()
        r.hset(key, mapping={
            "job_id": job_id,
            "updated_at": now
        })
        logger.info(f"[{job_id}] in progress at {now}")
        time.sleep(5)  # heartbeat every 5 seconds

# 🚀 Main Worker Loop
def main():
    
    logger.info("Worker started. Waiting for jobs...")

    listener_thread = threading.Thread(target=listen_for_commands, daemon=True)
    listener_thread.start()


    while True:
        is_ready.wait()  # Block here until 'resume' is called
        stop_event = threading.Event()

        _, job_json = r.blpop(REDIS_QUEUE)
        try:
            job = json.loads(job_json)
            job_id = job.get("id", "UNKNOWN")
            job_type = job.get("job_type")

            # Start heartbeat in a background thread
            hb_thread = threading.Thread(
                target=heartbeat_loop,
                args=(WORKER_NAME, job_id, stop_event),
                daemon=True
            )
            hb_thread.start()

            logger.info(f"[{job_id}] Received job of type '{job_type}'")

            if not should_accept_job(job):
                logger.info(f"[{job['id']}] Re-queuing due to insufficient resources.")
                r.rpush(REDIS_QUEUE, job_json)
                time.sleep(10)  # Optional backoff
                continue

            if job_type == "compress-video":
                handler = get_handler_for_type(job_type)
                result = handler(job, config)
            elif job_type == "job-test":
                result = test_job(job)
            else:
                logger.warning(f"[{job_id}] ❌ Unsupported job type: {job_type}")
                result = "unsupported"

            if result == "error":
                retries = job.get("retries", 0)
                if retries < 3:
                    job["retries"] = retries + 1
                    logger.info(f"[{job_id}] 🔁 Re-queuing job (attempt {job['retries']}/3)")
                    r.rpush(REDIS_QUEUE, json.dumps(job))
                else:
                    logger.error(f"[{job_id}] ❌ Job failed after 3 attempts. Giving up.")

        except json.JSONDecodeError:
            logger.error("❌ Received malformed JSON job.")
        except Exception as e:
            logger.exception(f"❌ Unexpected error while handling job: {e}")
        finally:
            stop_event.set()
            r.delete(f"heartbeat:{WORKER_NAME}")  # Clear when done

def listen_for_commands():
    pubsub = r.pubsub()
    pubsub.subscribe(CONTROL_CHANNEL)

    logger.info(f"[{WORKER_NAME}] Subscribed to channel: {CONTROL_CHANNEL}")

    for message in pubsub.listen():
        if message['type'] != 'message':
            continue
        command = message['data'].strip().lower()

        if command == 'pause':
            logger.info(f"[{WORKER_NAME}] Pausing work.")
            is_ready.clear()
        elif command == 'resume':
            logger.info(f"[{WORKER_NAME}] Resuming work.")
            is_ready.set()
        else:
            logger.info(f"[{WORKER_NAME}] Unknown command: {command}")

if __name__ == "__main__":
    main()
