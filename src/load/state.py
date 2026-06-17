"""Pipeline checkpoint state for incremental, idempotent runs."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class SnapshotState:
    processed_at: str
    content_hash: str
    game_date_max: str | None = None
    merge_stats: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SnapshotState:
        return cls(
            processed_at=data["processed_at"],
            content_hash=data["content_hash"],
            game_date_max=data.get("game_date_max"),
            merge_stats=data.get("merge_stats", {}),
        )


@dataclass
class PipelineState:
    version: int = 1
    processed_snapshots: dict[str, SnapshotState] = field(default_factory=dict)
    watermarks: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "processed_snapshots": {
                name: snap.to_dict() for name, snap in self.processed_snapshots.items()
            },
            "watermarks": self.watermarks,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PipelineState:
        snapshots = {
            name: SnapshotState.from_dict(snap)
            for name, snap in data.get("processed_snapshots", {}).items()
        }
        return cls(
            version=data.get("version", 1),
            processed_snapshots=snapshots,
            watermarks=data.get("watermarks", {}),
        )


def state_path(config: dict[str, Any]) -> Path:
    rel = config["incremental"].get("state_file", "data/state/pipeline_state.json")
    root = Path(config["paths"]["project_root"])
    return root / rel


def load_state(config: dict[str, Any]) -> PipelineState:
    path = state_path(config)
    if not path.exists():
        return PipelineState()
    return PipelineState.from_dict(json.loads(path.read_text(encoding="utf-8")))


def save_state(config: dict[str, Any], state: PipelineState) -> Path:
    path = state_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state.to_dict(), indent=2), encoding="utf-8")
    return path


def should_process_snapshot(
    state: PipelineState,
    snapshot: str,
    content_hash: str,
    *,
    force: bool = False,
) -> bool:
    """Return True when a snapshot should be processed."""
    if force:
        return True
    record = state.processed_snapshots.get(snapshot)
    if record is None:
        return True
    return record.content_hash != content_hash


def mark_snapshot_processed(
    state: PipelineState,
    snapshot: str,
    *,
    content_hash: str,
    game_date_max: str | None,
    merge_stats: dict[str, Any],
) -> PipelineState:
    """Record a successfully processed snapshot and advance watermarks."""
    state.processed_snapshots[snapshot] = SnapshotState(
        processed_at=datetime.now(timezone.utc).isoformat(),
        content_hash=content_hash,
        game_date_max=game_date_max,
        merge_stats=merge_stats,
    )
    if game_date_max:
        current = state.watermarks.get("games_date")
        if current is None or game_date_max > current:
            state.watermarks["games_date"] = game_date_max
    state.watermarks["last_snapshot"] = snapshot
    return state