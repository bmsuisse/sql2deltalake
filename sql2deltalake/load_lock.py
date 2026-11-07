"""The lock of a load: ``meta/lock.txt`` below the destination, created only if it does not exist
yet and holding a token of the load that owns it. A lock not modified for an hour is stale (its
load died) and is taken over."""

import os
import socket
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from .destination.destination import Destination

STALE_AFTER_SECONDS = 60 * 60


class LoadAlreadyRunning(RuntimeError):
    """Another load of the same destination holds its lock (written less than an hour ago)."""


def _age_seconds(lock: Destination) -> float:
    return (datetime.now(tz=timezone.utc) - lock.modified_time()).total_seconds()


def _holder(lock: Destination) -> str:
    try:
        return lock.read_str() or "no owner recorded"
    except Exception as e:
        return f"owner unknown: {e}"


def acquire(lock: Destination, logger: Optional[Any] = None) -> str:
    """Creates ``lock`` with a new token and returns the token; takes over a stale lock. Raises
    LoadAlreadyRunning if another load holds a fresh one."""
    now = datetime.now(tz=timezone.utc).isoformat()
    token = f"{uuid.uuid4().hex} {socket.gethostname()} pid {os.getpid()} {now}"
    if lock.create_exclusive(token):
        return token
    try:
        age = _age_seconds(lock)
    except FileNotFoundError:  # released in between
        age = None
    if age is not None and age <= STALE_AFTER_SECONDS:
        raise LoadAlreadyRunning(
            f"{lock} was written {age:.0f} s ago by another load ({_holder(lock)}): wait for it to "
            f"finish, or remove the file if no load is running (it is taken over after "
            f"{STALE_AFTER_SECONDS // 60} minutes)"
        )
    if age is not None:
        stale = _holder(lock)
        if logger is not None:
            logger.warning(f"Taking over the lock {lock} ({stale}), written {age / 60:.0f} min ago")
        if _holder(lock) == stale:  # not taken over by another load in the meantime
            lock.remove()
    if not lock.create_exclusive(token):
        raise LoadAlreadyRunning(f"{lock} was taken by another load ({_holder(lock)})")
    return token


def release(lock: Destination, token: str, logger: Optional[Any] = None) -> None:
    """Removes ``lock`` if it still holds ``token``; a lock another load took over stays."""
    if not lock.exists():
        return
    try:
        holder = lock.read_str()
    except FileNotFoundError:
        return
    except NotImplementedError:  # a destination that cannot read files: no owner check
        holder = token
    if holder == token:
        lock.remove()
    elif logger is not None:
        logger.warning(f"The lock {lock} was taken over by another load ({holder}), it is left in place")
