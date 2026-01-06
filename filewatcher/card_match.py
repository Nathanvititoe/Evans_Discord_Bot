from pathlib import Path
from typing import Dict, Optional
import shutil
from schemas.CardPair import CardPair, CardFile
import os

# set permissions
HOST_UID = int(os.environ.get("HOST_UID", 1000))
HOST_GID = int(os.environ.get("HOST_GID", 1000))

# init partial pair on first file
class _PartialPair:
    def __init__(self, name_id: str, nsfw: bool, watch_root: Path):
        self.name_id = name_id
        self.nsfw = nsfw
        self.watch_root = watch_root
        self.raw: Optional[CardFile] = None
        self.watermarked: Optional[CardFile] = None


# create class to mutate the pair, add/subtract/etc
class CardMatcher:
    def __init__(self):
        self._buffer: Dict[str, _PartialPair] = {}
        print("[CARD_MATCH] Init Card Matcher")

    def add_file(
        self, path: Path, *, nsfw: bool, watch_root: Path
    ) -> Optional[CardPair]:
        name_id = self._derive_name_id(path)

        # if already added/organized, return early
        if path.parent == watch_root / name_id:
            print("[CARD_MATCH] Skipping file, already organized pair.")
            return None

        is_raw = self._is_raw(path)
        partial = self._buffer.get(name_id)

        # first file locks NSFW
        if partial is None:
            partial = _PartialPair(name_id=name_id, nsfw=nsfw, watch_root=watch_root)
            self._buffer[name_id] = partial
            print(f"[CARD_MATCH] New partial pair, name={name_id} nsfw={nsfw}")

        card_file = CardFile(
            path=path,
            size_bytes=path.stat().st_size,
        )

        if is_raw:
            partial.raw = card_file
            print(f"[CARD_MATCH] RAW file found | {path.name}", flush=True)
        else:
            partial.watermarked = card_file
            print(f"[CARD_MATCH] WM file found | {path.name}", flush=True)

        print(f"Card partial raw : {partial.raw}")
        print(f"Card partial wm : {partial.watermarked}")

        # if both cards are found, create pair
        if partial.raw and partial.watermarked:
            print(f"[CARD_MATCH] Pair Completed | name={name_id}", flush=True)
            pair = CardPair(
                name_id=name_id,
                raw=partial.raw,
                watermarked=partial.watermarked,
                nsfw=partial.nsfw,
            )

            self._organize_pair(pair, partial.watch_root)
            del self._buffer[name_id]  # remove card from buffer
            return pair

        return None

    """
     creates isolated dir that should only contain a raw and wm pair,
     using the base filename (date) as the dir name
     """

    @staticmethod
    def _organize_pair(pair: CardPair, watch_root: Path) -> None:
        raw_path = pair.raw.path
        wm_path = pair.watermarked.path

        target_dir = watch_root / pair.name_id
        target_dir.mkdir(exist_ok=True)

        os.chown(target_dir, HOST_UID, HOST_GID)

        print(f"[CARD_MATCH] Organizing matching pair... | target_dir={target_dir}")

        for src in (raw_path, wm_path):
            dest = target_dir / src.name
            if src.resolve() != dest.resolve():
                shutil.move(str(src), str(dest))
                os.chown(dest, HOST_UID, HOST_GID)  # change dir permissions after creation
                print(f"[CARD_MATCH] Moved file | {src.name} -> {dest}", flush=True)

    # this gets the base-name(date)
    @staticmethod
    def _derive_name_id(path: Path) -> str:
        name = path.stem.lower()

        for token in ("-raw", "_raw", "-wm", "_wm", "-watermarked", "_watermarked"):
            if name.endswith(token):
                return name[: -len(token)]

        return name

    # determines if the card is raw/wm based on the filename
    @staticmethod
    def _is_raw(path: Path) -> bool:
        name = path.name.lower()
        return "raw" in name
