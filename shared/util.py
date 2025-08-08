
import os

def resolve_path(root, relative_path):
    return os.path.abspath(os.path.join(root, relative_path))