"""
scripts/db_inspect.py
---------------------
Live database inspector — shows queue state, schema, indexes, and lock info.
Run this while the system is processing jobs to see what's happening inside.

Usage (from project root):
    python scripts/db_inspect.py
    python scripts/db_inspect.py --dsn postgresql://taskuser:taskpass@localhost:5432/taskqueue
    python scripts/db_inspect.py --watch          # refresh every 3 seconds
    python scripts/db_inspect.py --watch --interval 1
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time

import asyncpg

# ---------------------------------------------------------------------------
# Colours
# ---------------------------------------------------------------------------
_TTY = sys.stdout.isatty()
def _c(code, t): return f"\033[{code}m{t}\033[0m" if _TTY else t
BOLD   = lambda t: _c("1",   t)
DIM    = lambda t: _c("2",   t)
GREEN  = lambda t: _c("92",  t)
YELLOW = lambda t: _c("93",  t)
RED    = lambda t: _c("91",  t)
CYAN   = lambda t: _c("96",  t)
BLUE   = lambda t: _c("94",  t)

STATUS_COLOUR = {
    "pending": YELLOW,
    "claimed": CYAN,
    "running": BLUE,
    "done":    GREEN,
    "failed":  RED,
    "dead":    lambda t: _c("95", t),  # magenta
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _row(label: str, value, width: int = 28):
    return f"  {DIM(label.ljust(width))} {value}"

def _table_header(*cols, widths=None):
    widths = widths or [20] * len(cols)
    header = "  " + "  ".join(BOLD(c.ljust(w)) for c, w in zip(cols, widths))
    sep    = "  " + "  ".join("─" * w for w in widths)
    return header + "\n" + sep

def _table_row(*vals, widths=None):
    widths = widths or [20] * len(vals)
    return "  " + "  ".join(str(v).ljust(w) for v, w in zip(vals, widths))

# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------

async def section_queue_depth(conn: asyncpg.Connection):
    print(BOLD("\n╔══ Queue Depth by Status ══╗"))
    rows = await conn.fetch(
        "SELECT status, COUNT(*) AS cnt FROM jobs GROUP BY status ORDER BY cnt DESC"
    )
    if not rows:
        print(DIM("  (no jobs in queue)"))
        return
    
    total = sum(r["cnt"] for r in rows)
    for r in rows:
        colour = STATUS_COLOUR.get(r["status"], lambda t: t)
        bar_len = int(r["cnt"] / max(total, 1) * 30)
        bar = colour("█" * bar_len)
        print(f"  {colour(r['status'].ljust(10))}  {str(r['cnt']).rjust(6)}  {bar}")
    print(f"  {'TOTAL'.ljust(10)}  {str(total).rjust(6)}")


async def section_recent_jobs(conn: asyncpg.Connection, limit: int = 10):
    print(BOLD(f"\n╔══ Recent {limit} Jobs ══╗"))
    rows = await conn.fetch(
        f"""
        SELECT id, type, status, priority, attempt, max_retries,
               worker_id, error,
               created_at AT TIME ZONE 'UTC' AS created_at,
               completed_at AT TIME ZONE 'UTC' AS completed_at
        FROM   jobs
        ORDER  BY created_at DESC
        LIMIT  {limit}
        """
    )
    if not rows:
        print(DIM("  (no jobs yet)"))
        return
    
    print(_table_header("ID", "Type", "Status", "Pri", "Att/Max", "Worker", widths=[6, 15, 10, 4, 8, 22]))
    for r in rows:
        colour = STATUS_COLOUR.get(r["status"], lambda t: t)
        wid = (r["worker_id"] or "─")[-16:]
        att = f"{r['attempt']}/{r['max_retries']}"
        print(_table_row(
            r["id"], r["type"][:14], colour(r["status"]), r["priority"],
            att, wid,
            widths=[6, 15, 10, 4, 8, 22]
        ))
        if r["error"]:
            print(f"         {RED('err:')} {DIM(r['error'][:70])}")


async def section_running_jobs(conn: asyncpg.Connection):
    print(BOLD("\n╔══ Running Jobs (with heartbeat age) ══╗"))
    rows = await conn.fetch("""
        SELECT id, type, worker_id,
               heartbeat_at,
               EXTRACT(EPOCH FROM (NOW() - heartbeat_at))::int AS hb_age_s,
               EXTRACT(EPOCH FROM (NOW() - created_at))::int   AS age_s
        FROM   jobs
        WHERE  status IN ('running', 'claimed')
        ORDER  BY heartbeat_at ASC
    """)
    if not rows:
        print(DIM("  (no running jobs)"))
        return

    print(_table_header("ID", "Type", "Worker", "HB age", "Total age", widths=[8, 14, 24, 8, 10]))
    for r in rows:
        hb_age = r["hb_age_s"] if r["hb_age_s"] is not None else "─"
        stale  = isinstance(hb_age, int) and hb_age > 30
        hb_str = RED(f"{hb_age}s ⚠") if stale else f"{hb_age}s"
        print(_table_row(r["id"], r["type"][:13], (r["worker_id"] or "─")[:23],
                         hb_str, f"{r['age_s']}s", widths=[8, 14, 24, 8, 10]))


async def section_dead_letter(conn: asyncpg.Connection):
    print(BOLD("\n╔══ Dead Letter Queue ══╗"))
    count = await conn.fetchval("SELECT COUNT(*) FROM dead_letter_jobs")
    if count == 0:
        print(DIM("  (empty — no jobs have exhausted retries)"))
        return

    rows = await conn.fetch("""
        SELECT id, original_id, type, attempts, error,
               died_at AT TIME ZONE 'UTC' AS died_at
        FROM   dead_letter_jobs
        ORDER  BY died_at DESC
        LIMIT  10
    """)
    print(f"  {RED(str(count))} total dead-lettered jobs (showing last 10)\n")
    print(_table_header("DLQ ID", "Orig ID", "Type", "Attempts", "Error", widths=[8, 8, 14, 9, 35]))
    for r in rows:
        err_short = (r["error"] or "")[:34]
        print(_table_row(r["id"], r["original_id"], r["type"][:13],
                         r["attempts"], err_short, widths=[8, 8, 14, 9, 35]))


async def section_advisory_locks(conn: asyncpg.Connection):
    print(BOLD("\n╔══ Active Advisory Locks (held by workers) ══╗"))
    rows = await conn.fetch("""
        SELECT l.objid AS job_id, l.pid,
               a.application_name, a.state,
               EXTRACT(EPOCH FROM (NOW() - a.state_change))::int AS state_age_s
        FROM   pg_locks l
        JOIN   pg_stat_activity a ON a.pid = l.pid
        WHERE  l.locktype = 'advisory'
          AND  l.granted  = true
        ORDER  BY l.objid
    """)
    if not rows:
        print(DIM("  (no advisory locks — workers are idle)"))
        return
    
    print(_table_header("Job ID", "PG PID", "App", "State", "Age(s)", widths=[10, 8, 20, 12, 8]))
    for r in rows:
        print(_table_row(r["job_id"], r["pid"],
                         (r["application_name"] or "─")[:19],
                         r["state"] or "─", r["state_age_s"] or 0,
                         widths=[10, 8, 20, 12, 8]))


async def section_indexes(conn: asyncpg.Connection):
    print(BOLD("\n╔══ Table Indexes ══╗"))
    rows = await conn.fetch("""
        SELECT
            i.relname                                      AS index_name,
            t.relname                                      AS table_name,
            pg_size_pretty(pg_relation_size(i.oid))        AS size,
            ix.indisunique                                  AS unique,
            idx.idx_scan                                    AS scans,
            idx.idx_tup_read                                AS tuples_read
        FROM   pg_class     t
        JOIN   pg_index     ix  ON t.oid = ix.indrelid
        JOIN   pg_class     i   ON i.oid = ix.indexrelid
        LEFT   JOIN pg_stat_user_indexes idx
               ON idx.indexrelid = i.oid
        WHERE  t.relname IN ('jobs', 'dead_letter_jobs')
        ORDER  BY t.relname, i.relname
    """)
    print(_table_header("Index", "Table", "Size", "Scans", "Rows read", widths=[30, 18, 8, 8, 12]))
    for r in rows:
        print(_table_row(r["index_name"][:29], r["table_name"][:17],
                         r["size"], r["scans"] or 0, r["tuples_read"] or 0,
                         widths=[30, 18, 8, 8, 12]))


async def section_throughput(conn: asyncpg.Connection):
    print(BOLD("\n╔══ Throughput (last 60 seconds) ══╗"))
    done_60s = await conn.fetchval("""
        SELECT COUNT(*) FROM jobs
        WHERE status = 'done' AND completed_at > NOW() - INTERVAL '60 seconds'
    """)
    done_5m = await conn.fetchval("""
        SELECT COUNT(*) FROM jobs
        WHERE status = 'done' AND completed_at > NOW() - INTERVAL '5 minutes'
    """)
    avg_exec = await conn.fetchval("""
        SELECT AVG(EXTRACT(EPOCH FROM (completed_at - created_at)))
        FROM   jobs WHERE status = 'done'
    """)
    print(_row("Jobs done (last 60s):", done_60s or 0))
    print(_row("Jobs/sec (last 60s):", f"{(done_60s or 0) / 60:.2f}"))
    print(_row("Jobs done (last 5m):", done_5m or 0))
    print(_row("Avg execution time:", f"{float(avg_exec):.3f}s" if avg_exec else "─"))


async def section_notify_channel(conn: asyncpg.Connection):
    print(BOLD("\n╔══ LISTEN/NOTIFY Subscribers ══╗"))
    rows = await conn.fetch("""
        SELECT pid, application_name, state
        FROM   pg_stat_activity
        WHERE  query ILIKE '%LISTEN%job_channel%'
           OR  wait_event = 'ClientRead'
           AND application_name != ''
        LIMIT 20
    """)
    listeners = await conn.fetchval("""
        SELECT COUNT(*) FROM pg_stat_activity
        WHERE backend_type = 'client backend'
    """)
    print(_row("Total client connections:", listeners or 0))
    # Also check active backend count
    workers_running = await conn.fetchval(
        "SELECT COUNT(DISTINCT worker_id) FROM jobs WHERE status = 'running'"
    )
    print(_row("Workers with running jobs:", workers_running or 0))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def inspect(dsn: str, watch: bool, interval: float):
    try:
        conn = await asyncpg.connect(dsn)
    except Exception as e:
        print(RED(f"Cannot connect to database: {e}"))
        print(DIM(f"DSN: {dsn}"))
        print(DIM("Make sure docker compose is running: docker compose up -d"))
        sys.exit(1)

    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )

    try:
        while True:
            if watch and _TTY:
                print("\033[2J\033[H", end="")  # clear screen

            now = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
            print(BOLD(f"PostgreSQL Task Queue — {CYAN(now)}"))
            print(DIM(f"  DSN: {dsn}\n"))

            await section_queue_depth(conn)
            await section_throughput(conn)
            await section_running_jobs(conn)
            await section_recent_jobs(conn)
            await section_dead_letter(conn)
            await section_advisory_locks(conn)
            await section_indexes(conn)
            await section_notify_channel(conn)

            if not watch:
                break
            print(DIM(f"\n  Refreshing in {interval}s  (Ctrl+C to exit)"))
            await asyncio.sleep(interval)

    except KeyboardInterrupt:
        print(DIM("\nExiting."))
    finally:
        await conn.close()


def main():
    parser = argparse.ArgumentParser(description="Task Queue DB Inspector")
    parser.add_argument(
        "--dsn",
        default=os.environ.get(
            "DATABASE_URL",
            "postgresql://taskuser:taskpass@localhost:5432/taskqueue"
        ),
        help="PostgreSQL DSN",
    )
    parser.add_argument("--watch",    action="store_true", help="Refresh continuously")
    parser.add_argument("--interval", type=float, default=3.0, help="Refresh interval (seconds)")
    args = parser.parse_args()
    asyncio.run(inspect(args.dsn, args.watch, args.interval))


if __name__ == "__main__":
    main()
