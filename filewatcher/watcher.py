from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path
import time
import os
from card_match import CardMatcher

matcher = CardMatcher()

# get path of dirs to watch
NSFW_WATCH_PATH = Path(os.environ.get("NSFW_WATCH_PATH", "/watched/nsfw"))
SFW_WATCH_PATH = Path(os.environ.get("SFW_WATCH_PATH", "/watched/sfw"))


# iterate through dropped directories and/or multiple files
def handle_path(path: Path, source: str, watch_root: Path):
    if not wait_for_file(path):
        return

    if path.is_file():
        pair = matcher.add_file(
            path,
            nsfw=(source == "NSFW"),
            watch_root=watch_root,
        )
        if pair:
            print(f"[MATCHED] {pair}", flush=True)
        return

    if path.is_dir():
        for p in path.rglob("*"):
            if p.is_file() and wait_for_file(p):
                pair = matcher.add_file(
                    p,
                    nsfw=(source == "NSFW"),
                    watch_root=watch_root,
                )
                if pair:
                    print(f"[MATCHED] {pair}", flush=True)


# guard to ensure file is fully ready before organizing/matching
def wait_for_file(path: Path, timeout: float = 2.0) -> bool:
    start = time.time()
    last_size = -1

    while time.time() - start < timeout:
        if not path.exists():
            time.sleep(0.05)
            continue

        size = path.stat().st_size
        if size == last_size and size > 0:
            return True

        last_size = size
        time.sleep(0.05)

    return False


# TODO: this is temp for testing, just prints to console
# print to console when file is dropped into watch dir
class PrintOnCreate(FileSystemEventHandler):
    def __init__(self, source: str, watch_root: Path):
        self.source = source
        self.watch_root = watch_root

    def on_created(self, event):
        path = Path(event.src_path)

        if path.name.startswith("."):
            return

        time.sleep(0.05)
        handle_path(path, self.source, self.watch_root)


if __name__ == "__main__":
    # make watch dirs if they dont exist
    NSFW_WATCH_PATH.mkdir(parents=True, exist_ok=True)
    SFW_WATCH_PATH.mkdir(parents=True, exist_ok=True)

    # create and start observer for watching both dirs
    observer = Observer()
    observer.schedule(
        PrintOnCreate("SFW", SFW_WATCH_PATH),
        str(SFW_WATCH_PATH),
        recursive=False,
    )
    observer.schedule(
        PrintOnCreate("NSFW", NSFW_WATCH_PATH),
        str(NSFW_WATCH_PATH),
        recursive=False,
    )
    observer.start()

    print(f"[WATCHER] Watching NSFW: {NSFW_WATCH_PATH}", flush=True)
    print(f"[WATCHER] Watching SFW: {SFW_WATCH_PATH}", flush=True)
    # keep it running
    try:
        while True:
            time.sleep(1)  # check dir every 1 sec
    except KeyboardInterrupt:
        observer.stop()  # stop watching w/ ctrl + c

    observer.join()
