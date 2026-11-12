if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
    __package__ = "stage34.core"

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import io
import os
from pathlib import Path
import re
import sys
import tarfile
import tempfile
import time
pass
from . import controller
MARKER = '<!-- projection of controller.db; edits are overwritten: controller.py render -->'

def digest(data):
    return hashlib.sha256(data).hexdigest()

def field(text, name):
    """Frozen Stage 1 field parser: never consume the next line for an empty value."""
    match = re.search('^\\*\\*' + re.escape(name) + ':\\*\\*[ \\t]*(.*)$', text, re.M)
    return match.group(1).strip() if match else ''

def legacy_info(path, data=None):
    """Frozen packet.info interpretation; deliberately does not import packet.py."""
    text = (path.read_bytes() if data is None else data).decode('utf-8')
    return {'id': path.stem, 'slug': path.parts[-3], 'status': field(text, 'Status') or 'open', 'kind': field(text, 'Kind') or 'task', 'repair_test': field(text, 'Repair-test'), 'agent': field(text, 'Agent'), 'lane': field(text, 'Lane') or 'local', 'ref': field(text, 'Ref'), 'goal': (re.search('^# \\S+: (.*)$', text, re.M) or [None, ''])[1], 'text': text}

def section(text, title):
    match = re.search('^## ' + re.escape(title) + '[^\\n]*\\n(.*?)(?=^## |\\Z)', text, re.M | re.S)
    return match.group(1).strip() if match else ''

def bullets(text, unwrap=False):
    items = [line[2:].strip() for line in text.splitlines() if line.startswith('- ')]
    return [item[1:-1] if unwrap and item.startswith('`') and item.endswith('`') else item for item in items]

def work_dir(conn):
    filename = conn.execute('PRAGMA database_list').fetchone()[2]
    if not filename:
        raise ValueError('in-memory databases have no filesystem projection')
    return Path(filename).resolve().parent

def safe_packet(work, slug, task):
    if not controller.SLUG_RE.fullmatch(slug) or not controller.TASK_RE.fullmatch(task):
        raise ValueError('invalid packet path')
    path = work / slug / 'tasks' / (task + '.md')
    for parent in (work / slug, path.parent, path):
        if parent.is_symlink():
            raise ValueError('symlink packet paths are refused')
    return path

@contextlib.contextmanager
def projection_lock(work, timeout=2.0):
    fd = os.open(str(work / '.projection.lock'), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 384)
    try:
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError('projection lock busy')
                time.sleep(0.01)
        yield
    finally:
        os.close(fd)

def packet_text(task):
    tid = task['id']
    fields = [('Status', task['status']), ('Feature', task['slug']), ('Agent', task['role']), ('Lane', task['lane']), ('Depth', task['depth']), ('Parent', task['parent']), ('Created', task['created_at']), ('Budget', task['budget']), ('Worktree', task['worktree']), ('Depends', ', '.join(task['depends'])), ('Kind', task['kind']), ('Repair-test', task['repair_test']), ('Ref', task['ref']), ('Generation', task['gen']), ('Revision', task['rev'])]
    if task['reserved_at']:
        fields.append(('Reserved', task['reserved_at']))
    lines = ['# %s: %s' % (tid, task['goal'])] + ['**%s:** %s' % pair for pair in fields]
    lines += ['', '## Goal', task['goal'], '', '## Owns (only these files)']
    lines += ['- `%s`' % item for item in task['owns']]
    lines += ['', '## Done when'] + ['- ' + item for item in task['done']]
    lines += ['', '## Return', 'Record DONE with `handoff.py <agent> DONE <what> --task %s --attempt <Launch from your prompt> --evidence <evidence>`; the owner verifies and closes it.' % tid, '', '## Notes']
    lines += ['- %s %s' % (note['ts'], note['text']) for note in task['notes']]
    lines += ['', '## Attempts', '| Gen | Attempt | Role | State | Reason | Started | Lease/ended |', '| --- | --- | --- | --- | --- | --- | --- |']
    for attempt in task['attempts']:
        values = [attempt['gen'], attempt['id'], attempt['role'], attempt['state'], attempt['reason'], controller.iso_utc(attempt['started_at']) if attempt['started_at'] is not None else '', controller.iso_utc(attempt['ended_at'] if attempt['ended_at'] is not None else attempt['lease_until']) if any((attempt[k] is not None for k in ('ended_at', 'lease_until'))) else '']
        lines.append('| ' + ' | '.join((str(v).replace('|', '\\|') for v in values)) + ' |')
    return '\n'.join(lines + ['', MARKER, ''])

def render(conn, task_id):
    """Re-read under the projection lock. Never roll back an already committed command."""
    if conn.in_transaction:
        raise ValueError('render must run after COMMIT')
    work = work_dir(conn)
    with projection_lock(work):
        conn.execute('BEGIN')
        try:
            task = controller.get(conn, task_id)
        finally:
            conn.execute('ROLLBACK')
        if task is None:
            return False
        path = safe_packet(work, task['slug'], task_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        old = path.read_bytes() if path.exists() else None
        if old is not None:
            revision = field(old.decode('utf-8', errors='replace'), 'Revision')
            if revision and int(revision) > task['rev']:
                raise ValueError('projection revision is ahead of database; recover before rendering')
        data = packet_text(task).encode('utf-8')
        if old != data and old is not None and (digest(old) not in (task['projection_sha'], task['import_sha'])):
            stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
            path.rename(path.with_name(path.name + '.hand-' + stamp))
            controller.activity.log('projection_conflict', task=task_id, rev=task['rev'])
        if old != data:
            fd, name = tempfile.mkstemp(prefix='.projection-', dir=str(path.parent))
            try:
                with os.fdopen(fd, 'wb') as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(name, str(path))
            finally:
                if os.path.exists(name):
                    os.unlink(name)
        return controller.set_projection(conn, task_id, task['rev'], digest(data))

def compare(conn):
    """Read-only comparison against the frozen Stage 1 interpretation, including orphan files."""
    work = work_dir(conn)
    differences = []
    tasks = controller.list_tasks(conn, include_done=True)
    expected_paths = set()
    for summary in tasks:
        task = controller.get(conn, summary['id'])
        path = safe_packet(work, task['slug'], task['id'])
        expected_paths.add(path)
        if not path.exists():
            differences.append((task['id'], 'missing'))
            continue
        parsed = legacy_info(path)
        expected = {'status': task['status'], 'agent': task['role'], 'lane': task['lane'], 'ref': task['ref']}
        for key, value in expected.items():
            if parsed[key] != value:
                differences.append((task['id'], key))
        if sorted(controller.TASK_RE.findall(field(parsed['text'], 'Depends'))) != sorted(task['depends']):
            differences.append((task['id'], 'depends'))
    for path in work.glob('*/tasks/T-*.md'):
        if path not in expected_paths:
            differences.append((path.stem, 'not_in_database'))
    return differences

def parse_record(path, data, now, launch_ttl, run_lease):
    info = legacy_info(path, data)
    text = info['text']
    status = info['status']
    if status not in controller.STATUSES:
        raise ValueError('%s: unknown_status' % path.stem)
    warnings = []
    for name in ('Agent', 'Kind', 'Depends', 'Created'):
        if not re.search('^\\*\\*' + name + ':\\*\\*', text, re.M):
            warnings.append('missing_field:' + name)
    notes = []
    for line in bullets(section(text, 'Notes')):
        stamp, _, note = line.partition(' ')
        notes.append((stamp, note))
    record = dict(id=info['id'], slug=info['slug'], goal=info['goal'], role=info['agent'], lane=info['lane'], kind=info['kind'], repair_test=info['repair_test'], status=status, depth=int(field(text, 'Depth') or '1'), parent=field(text, 'Parent') or 'pm', budget=field(text, 'Budget'), worktree=field(text, 'Worktree'), ref=info['ref'], created_at=field(text, 'Created') or controller.iso_utc(now), owns=bullets(section(text, 'Owns'), unwrap=True), done=bullets(section(text, 'Done when')), depends=controller.TASK_RE.findall(field(text, 'Depends')), notes=notes, gen=int(field(text, 'Generation') or '0'), rev=int(field(text, 'Revision') or '0'), import_sha=digest(data))
    if status == 'reserved':
        stamp = controller._epoch(field(text, 'Reserved'))
        if stamp is None or stamp + launch_ttl < now:
            record['status'] = 'open'
            warnings.append('expired_reservation')
        else:
            record['attempt'] = {'state': 'launching', 'lease_until': stamp + launch_ttl}
    elif status in ('running', 'waiting', 'submitted'):
        record['attempt'] = {'state': status}
        if status == 'running':
            record['attempt']['lease_until'] = now + run_lease
    controller._validate_task_fields(record)
    if not controller.TASK_RE.fullmatch(record['id']) or min(record['gen'], record['rev']) < 0:
        raise ValueError('invalid task identity/revision')
    for _, note in notes:
        controller._check_text('note', note, controller.MAX_TEXT * 4)
    return (record, warnings)

def packet_backup(work, packets, dest):
    """Archive the exact bytes parsed for import, plus checksums, before any DB write."""
    dest.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='orch-tasks-preimport-', suffix='.tgz', dir=str(dest))
    with os.fdopen(fd, 'wb') as output:
        with tarfile.open(fileobj=output, mode='w:gz') as archive:
            manifest = []
            for path, data in packets:
                relative = path.relative_to(work).as_posix()
                item = tarfile.TarInfo(relative)
                item.mode, item.size = (384, len(data))
                archive.addfile(item, io.BytesIO(data))
                manifest.append(digest(data) + '  ' + relative)
            data = ('\n'.join(manifest) + '\n').encode('utf-8')
            item = tarfile.TarInfo('MANIFEST.sha256')
            item.mode, item.size = (384, len(data))
            archive.addfile(item, io.BytesIO(data))
        output.flush()
        os.fsync(output.fileno())
    return Path(name)

def import_packets(work, dry_run=False, recover=False, slug=None, skip=(), backup_dir=None, now=None):
    """Explicit migration only. Dry run performs no filesystem or database writes."""
    now = time.time() if now is None else now
    work = Path(work).resolve()
    path = controller.db_path()
    conn = controller.open_db('ro', path=path) if path.exists() else None
    try:
        launch_ttl, run_lease = controller.ttls(conn) if conn is not None else (900, 7200)
        if slug is not None and (not controller.SLUG_RE.fullmatch(slug)):
            raise ValueError('invalid slug')
        packets, records, lines = ([], [], [])
        seen = set()
        for packet in sorted(work.glob((slug or '*') + '/tasks/T-*.md')):
            safe_packet(work, packet.parts[-3], packet.stem)
            if packet.stem in skip:
                continue
            if packet.stem in seen:
                raise ValueError('duplicate task id: ' + packet.stem)
            seen.add(packet.stem)
            data = packet.read_bytes()
            record, warnings = parse_record(packet, data, now, launch_ttl, run_lease)
            if field(data.decode('utf-8'), 'Revision') and (not recover) and (conn is None):
                raise ValueError('recover_required: projections exist without database')
            existing = controller.get(conn, packet.stem) if conn is not None else None
            if existing is not None:
                if digest(data) == existing['projection_sha'] and record['rev'] == existing['rev']:
                    record['import_sha'] = existing['import_sha']
                if not existing['import_sha'] or record['import_sha'] != existing['import_sha']:
                    raise ValueError('conflict: ' + packet.stem)
            packets.append((packet, data))
            records.append(record)
            lines.append('%s %s %s -> %s %s' % (packet.stem, record['slug'], legacy_info(packet, data)['status'], record['status'], ' '.join(warnings)))
        known = seen | {t['id'] for t in controller.list_tasks(conn, include_done=True)} if conn else seen
        for record in records:
            if any((dep not in known for dep in record['depends'])):
                lines.append(record['id'] + ' dangling_dep')
    finally:
        if conn is not None:
            conn.close()
    if dry_run:
        return (0, lines + ['dry-run: %d packets; no writes' % len(records)])
    if not records:
        return (0, ['0 imported, 0 skipped'])
    backup = packet_backup(work, packets, Path(backup_dir) if backup_dir else Path.home() / '.stage34' / 'backups')
    if not path.exists():
        if recover:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 384)
            os.close(fd)
            fresh = controller._connect(path, 'rw', controller.CLI_TIMEOUT)
            try:
                fresh.execute('PRAGMA journal_mode=WAL')
                controller.create_schema(fresh)
            finally:
                fresh.close()
        else:
            result = controller.init_db(path=path)
            if not result.ok:
                raise ValueError(result.code + ': ' + result.message)
    conn = controller.open_db(path=path)
    try:
        report = controller.import_tasks(conn, records, recover=recover, now=now)
        if report['code'] != 'ok':
            return (1, lines + [report['code'] + ': ' + report['message']])
        controller.activity.log('controller_import', imported=report['imported'], skipped=report['skipped'])
        return (0, lines + ['%d imported, %d skipped; backup %s' % (report['imported'], report['skipped'], backup)])
    finally:
        conn.close()

def main(argv=None):
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='cmd', required=True)
    render_parser = sub.add_parser('render')
    render_parser.add_argument('task', nargs='?')
    render_parser.add_argument('--all', action='store_true')
    sub.add_parser('compare')
    importer = sub.add_parser('import')
    importer.add_argument('--dry-run', action='store_true')
    importer.add_argument('--recover', action='store_true')
    importer.add_argument('--slug')
    importer.add_argument('--skip', nargs='*', default=[])
    args = parser.parse_args(argv)
    try:
        if args.cmd == 'import':
            code, lines = import_packets(controller.db_path().parent, args.dry_run, args.recover, args.slug, args.skip)
        else:
            if args.cmd == 'render' and bool(args.task) == bool(args.all):
                parser.error('render requires a task OR --all')
            conn = controller.open_db('ro' if args.cmd == 'compare' else 'rw')
            try:
                if args.cmd == 'compare':
                    differences = compare(conn)
                    code, lines = (bool(differences), ['%s %s' % row for row in differences])
                    lines.append('%d divergences' % len(differences))
                else:
                    tasks = [t['id'] for t in controller.list_tasks(conn, include_done=True)] if args.all else [args.task]
                    results = [render(conn, task) for task in tasks]
                    code, lines = (0 if all(results) else 1, ['%d rendered' % sum(results)])
            finally:
                conn.close()
        print('\n'.join(lines))
        return int(code)
    except controller.Unavailable as error:
        print(error.code, file=sys.stderr)
        return 3
    except (ValueError, OSError, controller.Refused) as error:
        print(str(error), file=sys.stderr)
        return 1
if __name__ == '__main__':
    sys.exit(main())
