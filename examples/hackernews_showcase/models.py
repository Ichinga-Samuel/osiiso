"""Small normalization layer for Hacker News shaped records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ItemRecord:
    id: int
    type: str
    by: str | None
    title: str
    text: str
    score: int
    url: str | None
    parent: int | None
    poll: int | None
    kids: tuple[int, ...]
    parts: tuple[int, ...]
    descendants: int
    time: int

    @classmethod
    def from_hn(cls, data: dict[str, Any]) -> ItemRecord:
        return cls(
            id=int(data["id"]),
            type=str(data.get("type", "story")),
            by=data.get("by"),
            title=str(data.get("title") or ""),
            text=str(data.get("text") or ""),
            score=int(data.get("score") or 0),
            url=data.get("url"),
            parent=data.get("parent"),
            poll=data.get("poll"),
            kids=tuple(int(k) for k in data.get("kids", [])),
            parts=tuple(int(p) for p in data.get("parts", [])),
            descendants=int(data.get("descendants") or 0),
            time=int(data.get("time") or 0),
        )

    def as_row(self) -> tuple[object, ...]:
        return (
            self.id,
            self.type,
            self.by,
            self.title,
            self.text,
            self.score,
            self.url,
            self.parent,
            self.poll,
            ",".join(str(k) for k in self.kids),
            ",".join(str(p) for p in self.parts),
            self.descendants,
            self.time,
        )

    def asdict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "by": self.by,
            "title": self.title,
            "text": self.text,
            "score": self.score,
            "url": self.url,
            "parent": self.parent,
            "poll": self.poll,
            "kids": list(self.kids),
            "parts": list(self.parts),
            "descendants": self.descendants,
            "time": self.time,
        }


@dataclass(frozen=True, slots=True)
class UserRecord:
    id: str
    created: int
    karma: int
    about: str
    submitted: tuple[int, ...]

    @classmethod
    def from_hn(cls, data: dict[str, Any]) -> UserRecord:
        return cls(
            id=str(data["id"]),
            created=int(data.get("created") or 0),
            karma=int(data.get("karma") or 0),
            about=str(data.get("about") or ""),
            submitted=tuple(int(i) for i in data.get("submitted", [])),
        )

    def as_row(self) -> tuple[object, ...]:
        return (self.id, self.created, self.karma, self.about, ",".join(str(i) for i in self.submitted))

    def asdict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "created": self.created,
            "karma": self.karma,
            "about": self.about,
            "submitted": list(self.submitted),
        }
