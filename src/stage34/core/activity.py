if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import argparse
import datetime as dt
import fcntl
import json
import os
import re
import sys
from pathlib import Path
WORK = Path(os.environ.get('STAGE34_WORK_DIR') or Path(__file__).resolve().parents[1] / 'work')
LOG = WORK / 'activity.jsonl'
ROTATE_BYTES = 5 * 1024 * 1024
PII = re.compile('\\b\\d{13}\\b|[\\w.+-]+@[\\w-]+\\.[\\w.]+')
MAX = 120

def clean(value):
    if isinstance(value, bool) or value is None or isinstance(value, (int, float)):
        return value
    text = ' '.join(str(value).split())[:MAX]
    return '[redacted]' if PII.search(text) else text

def now():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')

def log(event, **fields):
    """Append one event. Never raises (callers are hooks that must fail open)."""
    try:
        rec = {'ts': now(), 'event': clean(event)}
        for k, v in fields.items():
            if v not in (None, ''):
                rec[clean(k)] = clean(v)
        WORK.mkdir(parents=True, exist_ok=True)
        if LOG.exists() and LOG.stat().st_size > ROTATE_BYTES:
            LOG.replace(LOG.with_suffix('.jsonl.1'))
        with LOG.open('a', encoding='utf-8') as fh:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            fh.write(json.dumps(rec, ensure_ascii=False) + '\n')
        return rec
    except Exception:
        return None

def read(path=None):
    """All events, oldest first (rotated file included). Corrupt lines are skipped."""
    out = []
    for p in [LOG.with_suffix('.jsonl.1'), LOG] if path is None else [Path(path)]:
        try:
            lines = p.read_text(encoding='utf-8').splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            if isinstance(rec, dict) and 'ts' in rec and ('event' in rec):
                out.append(rec)
    return out

def fmt(rec):
    keys = [k for k in rec if k not in ('ts', 'event')]
    return f"{rec['ts']}  {rec['event']:<14} " + ' '.join((f'{k}={rec[k]}' for k in keys))

def main(argv=None):
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest='cmd', required=True)
    t = sub.add_parser('tail')
    t.add_argument('-n', type=int, default=30)
    t.add_argument('--slug')
    t.add_argument('--agent')
    lg = sub.add_parser('log')
    lg.add_argument('event')
    lg.add_argument('--slug')
    lg.add_argument('--agent-type')
    lg.add_argument('--agent-id')
    lg.add_argument('--task')
    lg.add_argument('kv', nargs='*')
    a, rest = ap.parse_known_args(argv)
    if a.cmd == 'tail' and rest:
        ap.error(f"unrecognized arguments: {' '.join(rest)}")
    if a.cmd == 'tail':
        evs = [e for e in read() if (not a.slug or e.get('slug') == a.slug) and (not a.agent or a.agent in (e.get('agent_id'), e.get('agent_type')))]
        for e in evs[-a.n:]:
            print(fmt(e))
        return 0
    extra = {}
    for kv in a.kv + rest:
        if '=' not in kv:
            ap.error(f'expected k=v, got {kv!r}')
        k, v = kv.split('=', 1)
        extra[k] = v
    rec = log(a.event, slug=a.slug, agent_type=a.agent_type, agent_id=a.agent_id, task=a.task, **extra)
    if rec is None:
        print('activity: could not write the log', file=sys.stderr)
        return 1
    print(fmt(rec))
    return 0
if __name__ == '__main__':
    sys.exit(main())
