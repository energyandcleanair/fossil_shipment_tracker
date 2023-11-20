import resource
import os

if os.path.isfile("/sys/fs/cgroup/memory/memory.limit_in_bytes"):
    with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as limit:
        mem = int(limit.read())
        resource.setrlimit(resource.RLIMIT_AS, (mem, mem))
