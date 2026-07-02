"""
Parallélisme des scrapers : threads (I/O-bound) + sharding multi-process.

Réglages par variables d'env :
  SCRAPE_WORKERS   nb de threads concurrents par process (déf. 1 = séquentiel)
  SHARD_COUNT      nb total de shards (process) — déf. 1
  SHARD_INDEX      index de CE shard (0..SHARD_COUNT-1) — déf. 0

La reprise étant idempotente (checkpoints / status), on peut lancer plusieurs
process avec des SHARD_INDEX différents sans conflit, chacun sur une tranche.
Le pool Tor + HAProxy répartit les connexions → garder SCRAPE_WORKERS ≈ TOR_POOL_SIZE.
"""
from __future__ import annotations

import os
import threading
from concurrent.futures import ThreadPoolExecutor


def get_workers(default=1) -> int:
    return max(1, int(os.getenv("SCRAPE_WORKERS", str(default))))


def shard_ok(num) -> bool:
    """True si ce numéro appartient au shard courant (répartition stable)."""
    cnt = int(os.getenv("SHARD_COUNT", "1"))
    idx = int(os.getenv("SHARD_INDEX", "0"))
    if cnt <= 1:
        return True
    s = str(num)
    try:
        v = int(s)
    except ValueError:
        v = abs(hash(s))
    return v % cnt == idx


def run_pool(items, make_ctx, handle, workers):
    """Traite `items` en parallèle.

    - make_ctx() : construit la ressource propre au thread (Store, session…).
      Appelé une fois par thread (thread-local), pas une fois par item.
    - handle(ctx, item) : traite un item ; DOIT gérer ses propres exceptions.
    Séquentiel si workers <= 1 (aucun thread créé).
    """
    if workers <= 1:
        ctx = make_ctx()
        for it in items:
            handle(ctx, it)
        return

    local = threading.local()

    def _task(it):
        if not hasattr(local, "ctx"):
            local.ctx = make_ctx()
        handle(local.ctx, it)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_task, items))
