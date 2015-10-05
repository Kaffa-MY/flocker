from pyrsistent import PClass, field, pset

from twisted.internet.defer import Deferred, maybeDeferred
from twisted.protocols.basic import LineOnlyReceiver

from flocker.common import gather_deferreds
from flocker.common.runner import run_ssh

class _WallClock(PClass):
    clock = field(mandatory=True)
    client = field()

    def __call__(self, f, *a, **kw):
        def finished(ignored):
            end = self.clock.seconds()
            elapsed = end - start
            return elapsed

        start = self.clock.seconds()
        d = f(*a, **kw)
        d.addCallback(finished)
        return d


_GET_CURSOR_COMMAND = [b"journalctl", b"--show-cursor", b"--lines", b"0"]


class _ParseCursor(LineOnlyReceiver):
    def __init__(self):
        self.result = Deferred()

    def lineReceived(self, line):
        prefix = b"-- cursor: "
        if line.startswith(prefix):
            cursor = line[len(prefix):].strip()
            self.result.callback(cursor)


def get_cursor(reactor, node):
    parser = _ParseCursor()
    d = run_ssh(
        reactor,
        b"root",
        node.public_address.exploded,
        _GET_CURSOR_COMMAND,
        handle_stdout=parser.lineReceived,
    )
    d.addCallback(lambda ignored: parser.result)
    return d


def _show_journal_command(units, cursor):
    base = [
        b"journalctl",
        b"--after-cursor", cursor,
        b"--output", b"cat",
    ]
    for unit in units:
        base.extend([b"--unit", unit])
    return base


class _ParseWC(LineOnlyReceiver):
    def lineReceived(self, line):
        lines, bytes = line.split()
        self.result.callback(dict(lines=int(lines), bytes=int(bytes)))


def measure_journal(reactor, node, cursor, units):
    parser = _ParseWC()
    d = run_ssh(
        reactor,
        b"root",
        node.primary_address.exploded,
        _show_journal_command(units, cursor) + [
            # Count it all up remotely so we don't have to transfer it.
            b"|", b"wc", b"--lines", b"--bytes"
        ],
        parser.lineReceived,
    )
    d.addCallback(lambda ignored: parser.result)
    return d


_JOURNAL_UNITS = pset({
    u"flocker-control",
    u"flocker-dataset-agent",
    u"flocker-container-agent",
})


class _JournalVolume(PClass):
    clock = field(mandatory=True)
    client = field(mandatory=True)

    def __call__(self, f, *a, **kw):
        d = self.client.list_nodes()

        def get_cursors(nodes):
            d = gather_deferreds(list(
                # XXX s/clock/reactor/
                get_cursor(self.clock, node) for node in nodes
            ))
            d.addCallback(lambda cursors: (cursors, nodes))
            return d
        d.addCallback(get_cursors)

        def finished(result, (cursors, nodes)):
            d = gather_deferreds(list(
                measure_journal(
                    self.clock, node, cursor, _JOURNAL_UNITS
                ) for node, cursor in zip(nodes, cursors)
            ))
            d.addCallback(sum)
            return d

        def run(cursors):
            d = f(*a, **kw)
            d.addCallback(finished, cursors)
            return d
        d.addCallback(run)
        return d


_measurements = {
    "wallclock": _WallClock,
    "journal-volume": _JournalVolume,
}


def get_measurement(clock, client, name):
    return maybeDeferred(_measurements[name], clock=clock, client=client)