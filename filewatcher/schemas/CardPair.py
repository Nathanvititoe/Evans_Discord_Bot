from dataclasses import dataclass
from typing import Optional
from pathlib import Path


@dataclass(frozen=True)
class CardFile:
    path: Path
    size_bytes: int


# class for pair of cards (raw and wm)
@dataclass(frozen=True)
class CardPair:
    name_id: str
    raw: CardFile
    watermarked: CardFile
    nsfw: bool
    purchaser: Optional[str] = None

    def complete(self) -> bool:
        return self.raw is not None and self.watermarked is not None
