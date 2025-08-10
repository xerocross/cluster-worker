
from shared.util import resolve_path
from shared.log_mod import get_logger
import subprocess

def handle(job, config):
    logger = get_logger(config)
    root = config["shared_root"]
    input_path = resolve_path(root, job["input"])
    output_path = resolve_path(root, job["output"])

    command = [
        "ffmpeg",
        "-i", input_path,
        "-vcodec", "libx264",
        "-crf", "23",
        "-preset", "medium",
        "-acodec", "aac",
        "-b:a", "128k",
        output_path
    ]

    logger.info(f"[{job['id']}] Running compress-video:\n      {' '.join(command)}")
    try:
        subprocess.run(command, check=True)
        logger.info(f"[{job['id']}] ✅ Video compression complete: {output_path}")
        return "success"
    except Exception as e:
        logger.error(f"[{job['id']}] ❌ Unexpected Error occurred during video compression", exc_info=True)
        return "error"