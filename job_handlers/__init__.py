from .compress_video import handle as compress_video_handler

job_registry = {
    "compress_video": compress_video_handler
}

def get_handler_for_type(job_type):
    return job_registry.get(job_type)
