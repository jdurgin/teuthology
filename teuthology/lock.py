import argparse
import httplib2
import json
import logging
import subprocess
import urllib
import yaml
import re
import collections

from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def _lock_url(ctx):
    return ctx.teuthology_config['lock_server']

def send_request(method, url, body=None, headers=None):
    http = httplib2.Http()
    resp, content = http.request(url, method=method, body=body, headers=headers)
    if resp.status == 200:
        return (True, content, resp.status)
    log.info("%s request to '%s' with body '%s' failed with response code %d",
             method, url, body, resp.status)
    return (False, None, resp.status)

def lock_many(ctx, num, user=None, description=None):
    if user is None:
        user = teuthology.get_user()
    success, content, status = send_request('POST', _lock_url(ctx),
                                    urllib.urlencode(dict(user=user, num=num)))
    if success:
        machines = json.loads(content)
        log.debug('locked {machines}'.format(machines=', '.join(machines.keys())))
        if description is not None:
            log.debug('Setting locked machine descriptions to %s', description)
            for m in machines.keys():
                update_lock(ctx, m, description)
        return machines
    if status == 503:
        log.error('Insufficient nodes available to lock %d nodes.', num)
    else:
        log.error('Could not lock %d nodes, reason: unknown.', num)
    return []

def lock(ctx, name, user=None):
    if user is None:
        user = teuthology.get_user()
    success, _, _ = send_request('POST', _lock_url(ctx) + '/' + name,
                              urllib.urlencode(dict(user=user)))
    if success:
        log.debug('locked %s as %s', name, user)
    else:
        log.error('failed to lock %s', name)
    return success

def unlock(ctx, name, user=None):
    if user is None:
        user = teuthology.get_user()
    success, _ , _ = send_request('DELETE', _lock_url(ctx) + '/' + name + '?' + \
                                  urllib.urlencode(dict(user=user)))
    if success:
        log.debug('unlocked %s', name)
    else:
        log.error('failed to unlock %s', name)
    return success

def get_status(ctx, name):
    success, content, _ = send_request('GET', _lock_url(ctx) + '/' + name)
    if success:
        return json.loads(content)
    return None

def list_locks(ctx):
    success, content, _ = send_request('GET', _lock_url(ctx))
    if success:
        return json.loads(content)
    return None

def update_lock(ctx, name, description=None, status=None, sshpubkey=None):
    updated = {}
    if description is not None:
        updated['desc'] = description
    if status is not None:
        updated['status'] = status
    if sshpubkey is not None:
        updated['sshpubkey'] = sshpubkey

    if updated:
        success, _, _ = send_request('PUT', _lock_url(ctx) + '/' + name,
                                  body=urllib.urlencode(updated),
                                  headers={'Content-type': 'application/x-www-form-urlencoded'})
        return success
    return True

def _positive_int(string):
    value = int(string)
    if value < 1:
        raise argparse.ArgumentTypeError(
            '{string} is not positive'.format(string=string))
    return value

def canonicalize_hostname(s):
    if re.match('ubuntu@.*\.front\.sepia\.ceph\.com', s) is None:
        s = 'ubuntu@' + s + '.front.sepia.ceph.com'
    return s

def main():
    parser = argparse.ArgumentParser(description="""
Lock, unlock, or query lock status of machines.
""")
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        default=False,
        help='be more verbose',
        )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--list',
        action='store_true',
        default=False,
        help='Show lock info for machines owned by you, or only machines specified. Can be restricted by --owner, --status, and --locked.',
        )
    group.add_argument(
        '--list-targets',
        action='store_true',
        default=False,
        help='Show lock info for all machines, or only machines specified, in targets: yaml format. Can be restricted by --owner, --status, and --locked.',
        )
    group.add_argument(
        '--lock',
        action='store_true',
        default=False,
        help='lock particular machines',
        )
    group.add_argument(
        '--unlock',
        action='store_true',
        default=False,
        help='unlock particular machines',
        )
    group.add_argument(
        '--lock-many',
        dest='num_to_lock',
        type=_positive_int,
        help='lock this many machines',
        )
    group.add_argument(
        '--update',
        action='store_true',
        default=False,
        help='update the description or status of some machines',
        )
    group.add_argument(
        '--summary',
        action='store_true',
        default=False,
        help='summarize locked-machine counts by owner',
        )
    parser.add_argument(
        '-a', '--all',
        action='store_true',
        default=False,
        help='list all machines, not just those owned by you',
        )
    parser.add_argument(
        '--owner',
        default=None,
        help='owner of the lock(s) (must match to unlock a machine)',
        )
    parser.add_argument(
        '-f',
        action='store_true',
        default=False,
        help='don\'t exit after the first error, continue locking or unlocking other machines',
        )
    parser.add_argument(
        '--desc',
        default=None,
        help='update description',
        )
    parser.add_argument(
        '--status',
        default=None,
        choices=['up', 'down'],
        help='whether a machine is usable for testing',
        )
    parser.add_argument(
        '--locked',
        default=None,
        choices=['true', 'false'],
        help='whether a machine is locked',
        )
    parser.add_argument(
        '--brief',
        action='store_true',
        default=False,
        help='Shorten information reported from --list',
        )
    parser.add_argument(
        '-t', '--targets',
        dest='targets',
        default=None,
        help='input yaml containing targets',
        )
    parser.add_argument(
        'machines',
        metavar='MACHINE',
        default=[],
        nargs='*',
        help='machines to operate on',
        )

    ctx = parser.parse_args()

    loglevel = logging.ERROR
    if ctx.verbose:
        loglevel = logging.DEBUG

    logging.basicConfig(
        level=loglevel,
        )

    teuthology.read_config(ctx)

    ret = 0
    user = ctx.owner
    machines = [canonicalize_hostname(m) for m in ctx.machines]
    machines_to_update = []

    if ctx.targets:
        try:
            with file(ctx.targets) as f:
                g = yaml.safe_load_all(f)
                for new in g:
                    if 'targets' in new:
                        for t in new['targets'].iterkeys():
                            machines.append(t)
        except IOError, e:
            raise argparse.ArgumentTypeError(str(e))

    if ctx.f:
        assert ctx.lock or ctx.unlock, \
            '-f is only supported by --lock and --unlock'
    if machines:
        assert ctx.lock or ctx.unlock or ctx.list or ctx.list_targets \
            or ctx.update, \
            'machines cannot be specified with that operation'
    else:
        assert ctx.num_to_lock or ctx.list or ctx.list_targets or ctx.summary,\
            'machines must be specified for that operation'
    if ctx.all:
        assert ctx.list or ctx.list_targets, \
            '--all can only be used with --list and --list-targets'
        assert ctx.owner is None, \
            '--all and --owner are mutually exclusive'
        assert not machines, \
            '--all and listing specific machines are incompatible'

    if ctx.brief:
        assert ctx.list, '--brief only applies to --list'

    if ctx.list or ctx.list_targets:
        assert ctx.desc is None, '--desc does nothing with --list'

        if machines:
            statuses = [get_status(ctx, machine) for machine in machines]
        else:
            statuses = list_locks(ctx)

        if statuses:
            if not machines and ctx.owner is None and not ctx.all:
                ctx.owner = teuthology.get_user()
            if ctx.owner is not None:
                statuses = [status for status in statuses \
                                if status['locked_by'] == ctx.owner]
            if ctx.status is not None:
                statuses = [status for status in statuses \
                                if status['up'] == (ctx.status == 'up')]
            if ctx.locked is not None:
                statuses = [status for status in statuses \
                                if status['locked'] == (ctx.locked == 'true')]
            if ctx.list:
                if ctx.brief:
                    for s in statuses:
                        locked = "un" if s['locked'] == 0 else "  "
                        mo = re.match('\w+@(\w+?)\..*', s['name'])
                        host = mo.group(1) if mo else s['name']
                        print '{host} {locked}locked {owner} "{desc}"'.format(
                            locked = locked, host = host,
                            owner=s['locked_by'], desc=s['description'])
                else:
                    print json.dumps(statuses, indent=4)
            else:
                frag = { 'targets': {} }
                for f in statuses:
                    frag['targets'][f['name']] = f['sshpubkey']
                print yaml.safe_dump(frag, default_flow_style=False)
        else:
            log.error('error retrieving lock statuses')
            ret = 1

    elif ctx.summary:
        do_summary(ctx)
        return 0

    elif ctx.lock:
        for machine in machines:
            if not lock(ctx, machine, user):
                ret = 1
                if not ctx.f:
                    return ret
            else:
                machines_to_update.append(machine)
    elif ctx.unlock:
        for machine in machines:
            if not unlock(ctx, machine, user):
                ret = 1
                if not ctx.f:
                    return ret
            else:
                machines_to_update.append(machine)
    elif ctx.num_to_lock:
        result = lock_many(ctx, ctx.num_to_lock, user)
        if not result:
            ret = 1
        else:
            machines_to_update = result.keys()
            print yaml.safe_dump(dict(targets=result), default_flow_style=False)
    elif ctx.update:
        assert ctx.desc is not None or ctx.status is not None, \
            'you must specify description or status to update'
        assert ctx.owner is None, 'only description and status may be updated'
        machines_to_update = machines

    if ctx.desc is not None or ctx.status is not None:
        for machine in machines_to_update:
            update_lock(ctx, machine, ctx.desc, ctx.status)

    return ret

def update_hostkeys():
    parser = argparse.ArgumentParser(description="""
Update any hostkeys that have changed. You can list specific machines
to run on, or use -a to check all of them automatically.
""")
    parser.add_argument(
        '-t', '--targets',
        default=None,
        help='input yaml containing targets to check',
        )
    parser.add_argument(
        'machines',
        metavar='MACHINES',
        default=[],
        nargs='*',
        help='hosts to check for updated keys',
        )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        default=False,
        help='be more verbose',
        )
    parser.add_argument(
        '-a', '--all',
        action='store_true',
        default=False,
        help='update hostkeys of all machines in the db',
        )

    ctx = parser.parse_args()

    loglevel = logging.ERROR
    if ctx.verbose:
        loglevel = logging.DEBUG

    logging.basicConfig(
        level=loglevel,
        )

    teuthology.read_config(ctx)

    assert ctx.all or ctx.targets or ctx.machines, 'You must specify machines to update'
    if ctx.all:
        assert not ctx.targets and not ctx.machines, \
            'You can\'t specify machines with the --all option'

    machines = [canonicalize_hostname(m) for m in ctx.machines]

    if ctx.targets:
        try:
            with file(ctx.targets) as f:
                g = yaml.safe_load_all(f)
                for new in g:
                    if 'targets' in new:
                        for t in new['targets'].iterkeys():
                            machines.append(t)
        except IOError, e:
            raise argparse.ArgumentTypeError(str(e))

    locks = list_locks(ctx)
    current_locks = {}
    for lock in locks:
        current_locks[lock['name']] = lock

    if ctx.all:
        machines = current_locks.keys()

    for i, machine in enumerate(machines):
        if '@' in machine:
            _, machines[i] = machine.rsplit('@')

    args = ['ssh-keyscan']
    args.extend(machines)
    p = subprocess.Popen(
        args=args,
        stdout=subprocess.PIPE,
        )
    out, _ = p.communicate()
    assert p.returncode == 0, 'ssh-keyscan failed'

    ret = 0
    for key_entry in out.splitlines():
        hostname, pubkey = key_entry.split(' ', 1)
        # TODO: separate out user
        full_name = 'ubuntu@{host}'.format(host=hostname)
        log.info('Checking %s', full_name)
        assert full_name in current_locks, 'host is not in the database!'
        if current_locks[full_name]['sshpubkey'] != pubkey:
            log.info('New key found. Updating...')
            if not update_lock(ctx, full_name, sshpubkey=pubkey):
                log.error('failed to update %s!', full_name)
                ret = 1

    return ret

def do_summary(ctx):
    lockd = collections.defaultdict(lambda: [0,0])
    for l in list_locks(ctx):
        who = l['locked_by'] if l['locked'] == 1 else '(free)'
        lockd[who][0] += 1
        lockd[who][1] += l['up']         # up is 1 or 0

    # locks = [('owner', (cnt, up)), ...]
    def sortfn(x,y):
        if x[0] == '(free)': return 1       # (free) sorts last
        return cmp(x[1][0], y[1][0])        # otherwise order by cnt
    locks = sorted([p for p in lockd.iteritems()], cmp=sortfn)

    total_count, total_up = 0, 0
    print "COUNT  UP  OWNER"
    for (owner, (count, upcount)) in locks:
        print "{count:3d}  {up:3d}  {owner}".format(count = count,
            up = upcount, owner = owner)
        total_count += count
        total_up += upcount
    print "---  ---"
    print "{cnt:3d}  {up:3d}".format(cnt = total_count, up = total_up)
