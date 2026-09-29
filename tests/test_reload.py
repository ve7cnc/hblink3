#!/usr/bin/env python
#
# Live routing reload (bridge.reload_rules, bound to SIGHUP): rules.py is re-read and the
# routing swapped in place -- no restart, nobody dropped. Driven through the frame-replay
# harness against the real routing core.
#
# Run from the repo root:   venv/bin/python -m unittest discover -s tests

import os
import sys
import tempfile
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)

from dmr_utils3.utils import bytes_3, bytes_4
import harness
import bridge


def member(system, tgid, ts=1, active=True, to_type='NONE', timeout=2):
    return {'SYSTEM': system, 'TS': ts, 'TGID': tgid, 'ACTIVE': active,
            'TIMEOUT': timeout, 'TO_TYPE': to_type, 'ON': [tgid] if to_type == 'ON' else [],
            'OFF': [], 'RESET': []}


def rules_file(bridges, obp=None, unit=None):
    """A rules.py as Trestle writes one: plain Python literals."""
    f = tempfile.NamedTemporaryFile('w', suffix='.py', delete=False)
    f.write('BRIDGES = %r\nOBP_BRIDGES = %r\nXLX_BRIDGES = {}\nUNIT = %r\n'
            % (bridges, obp or {}, unit or []))
    f.close()
    return f.name


BEFORE = {
    'TALK': [member('SERVER-1', 3100), member('REPEATER-1', 3100)],
    'PART': [member('SERVER-1', 3200, active=True, to_type='ON', timeout=15),
             member('REPEATER-1', 3200, active=True, to_type='ON', timeout=15)],
    'GONE': [member('SERVER-1', 3300), member('REPEATER-1', 3300)],
}


def after_rules():
    # TALK untouched; PART keeps its members; GONE loses REPEATER-1; NEW appears.
    return {
        'TALK': [member('SERVER-1', 3100), member('REPEATER-1', 3100)],
        'PART': [member('SERVER-1', 3200, active=False, to_type='ON', timeout=15),
                 member('REPEATER-1', 3200, active=False, to_type='ON', timeout=15)],
        'GONE': [member('SERVER-1', 3300)],
        'NEW': [member('SERVER-1', 3400), member('REPEATER-1', 3400)],
    }


class TestLiveReload(unittest.TestCase):
    def setUp(self):
        # World() consumes (mutates) its spec: give it a fresh copy.
        import copy
        self.w = harness.World(copy.deepcopy(BEFORE))
        bridge.report_server = None
        self.kw = dict(rf_src=bytes_3(312000), peer=bytes_4(312000), slot=1)

    def call(self, tgid, sid, bursts=3, src='SERVER-1'):
        kw = dict(self.kw, dst=bytes_3(tgid), stream_id=sid)
        self.w.feed_group_header(src, **kw)
        for b in range(1, bursts + 1):
            self.w.clock.tick(0.06)
            self.w.feed_group_burst(src, b, **kw)
        self.w.clock.tick(0.06)
        self.w.feed_group_terminator(src, **kw)

    def to(self, system, tgid):
        return [p for p in self.w.emitted_to(system) if p[8:11] == bytes_3(tgid)]

    def test_a_call_in_progress_on_an_untouched_talkgroup_loses_nothing(self):
        kw = dict(self.kw, dst=bytes_3(3100), stream_id=b'\x00\x00\x00\x01')
        self.w.feed_group_header('SERVER-1', **kw)
        self.w.clock.tick(0.06)
        self.w.feed_group_burst('SERVER-1', 1, **kw)
        self.assertTrue(bridge.reload_rules(rules_file(after_rules())))    # mid-call
        for b in (2, 3, 4):
            self.w.clock.tick(0.06)
            self.w.feed_group_burst('SERVER-1', b, **kw)
        self.w.clock.tick(0.06)
        self.w.feed_group_terminator('SERVER-1', **kw)
        # Header, four bursts and the terminator all reached REPEATER-1.
        self.assertEqual(len(self.to('REPEATER-1', 3100)), 6)

    def test_new_talkgroups_route_and_removed_members_stop(self):
        self.assertTrue(bridge.reload_rules(rules_file(after_rules())))
        self.w.clock.tick(10)
        self.call(3400, b'\x00\x00\x00\x02')
        self.assertTrue(self.to('REPEATER-1', 3400), 'the new talkgroup was not routed')
        self.w.clock.tick(10)
        self.call(3300, b'\x00\x00\x00\x03')
        self.assertEqual(self.to('REPEATER-1', 3300), [], 'a removed member still gets traffic')

    def test_a_running_part_time_timer_survives(self):
        before = {(m['SYSTEM']): (m['ACTIVE'], m['TIMER']) for m in bridge.BRIDGES['PART']}
        self.assertTrue(bridge.reload_rules(rules_file(after_rules())))
        # The file says ACTIVE False, but the talkgroup was up: it stays up, same timer.
        after = {(m['SYSTEM']): (m['ACTIVE'], m['TIMER']) for m in bridge.BRIDGES['PART']}
        self.assertEqual(after, before)

    def test_a_shorter_timeout_shortens_a_running_timer(self):
        rules = after_rules()
        for m in rules['PART']:
            m['TIMEOUT'] = 1
        old = {m['SYSTEM']: m['TIMER'] for m in bridge.BRIDGES['PART']}
        self.assertTrue(bridge.reload_rules(rules_file(rules)))
        for m in bridge.BRIDGES['PART']:
            self.assertLessEqual(m['TIMER'], min(old[m['SYSTEM']], bridge.time() + 60))

    def test_bad_rules_keep_the_running_routing(self):
        bad = after_rules()
        bad['NEW'].append(member('NO-SUCH-SYSTEM', 3400))
        routing = bridge.BRIDGES
        self.assertFalse(bridge.reload_rules(rules_file(bad)))
        self.assertIs(bridge.BRIDGES, routing)
        self.call(3100, b'\x00\x00\x00\x04')
        self.assertTrue(self.to('REPEATER-1', 3100), 'routing broke after a refused reload')

    def test_an_unreadable_file_keeps_the_running_routing(self):
        f = tempfile.NamedTemporaryFile('w', suffix='.py', delete=False)
        f.write('BRIDGES = {this is not python')
        f.close()
        routing = bridge.BRIDGES
        self.assertFalse(bridge.reload_rules(f.name))
        self.assertIs(bridge.BRIDGES, routing)


if __name__ == '__main__':
    unittest.main()
