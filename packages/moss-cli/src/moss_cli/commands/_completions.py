"""Shared autocompletion callbacks for dynamic CLI completions."""

from __future__ import annotations

import os
from typing import List


def complete_index_name(incomplete: str) -> List[str]:
    """Return index names matching *incomplete* for shell tab-completion.

    Reads credentials from environment variables or the config file.
    Silently returns an empty list when credentials are unavailable or the
    API call fails so that completion never blocks or errors.
    """
    try:
        from ..config import get_profile_credentials, get_selected_profile

        pid = os.getenv("MOSS_PROJECT_ID")
        pkey = os.getenv("MOSS_PROJECT_KEY")

        if not pid or not pkey:
            profile = get_selected_profile()
            cfg_pid, cfg_pkey = get_profile_credentials(profile)
            pid = pid or cfg_pid
            pkey = pkey or cfg_pkey

        if not pid or not pkey:
            return []

        import asyncio

        from moss import MossClient

        client = MossClient(pid, pkey)
        indexes = asyncio.run(client.list_indexes())

        names: List[str] = []
        for idx in indexes:
            name = getattr(idx, "name", None) or str(idx)
            if name.lower().startswith(incomplete.lower()):
                names.append(name)
        return names
    except Exception:
        return []
