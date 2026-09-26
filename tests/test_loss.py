#!/usr/bin/env python
#
# Voice loss accounting: missing bursts are counted from gaps in the superframe
# position (A..F), long dropouts sized from arrival time; the result is reported
# live (UPDATE) and per call (END), and an OpenBridge terminator may carry the
# upstream's own figure (cc2obp relaying the c-Bridge's B-off LOSS).
#
# Run from the repo root:   venv/bin/python -m unittest discover -s tests

import os
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)

import bridge
from dmr_utils3.utils import bytes_3, bytes_4
from harness import World, mk_dmrd, _FT_VOICE, _FT_DATA_SYNC, _VHEAD, _VTERM

_FT_VSYNC = 1
TG, SRC, PEER, SID = bytes_3(3100), bytes_3(3120001), bytes_4(312100), bytes_4(0xbeef)


class TestLossMath(unittest.TestCase):
    """voice_loss_track on a bare status dict, with explicit arrival times."""

    def _run(self, seq):
        st = {}
        bridge.voice_loss_reset(st)
        for vseq, t in seq:
            bridge.voice_loss_track(st, vseq, t)
        return st

    def test_clean_stream_no_loss(self):
        st = self._run([(i % 6, i * 0.06) for i in range(24)])
        self.assertEqual((st['RX_VRECV'], st['RX_VLOST']), (24, 0))
        self.assertEqual(bridge.voice_loss_pct(st), '0.0')

    def test_one_missing_burst(self):
        seq = [(i % 6, i * 0.06) for i in range(12) if i != 3]
        st = self._run(seq)
        self.assertEqual((st['RX_VRECV'], st['RX_VLOST']), (11, 1))
        self.assertEqual(bridge.voice_loss_pct(st), '8.3')

    def test_jitter_is_not_loss(self):
        # bursts bunched and delayed, but every position present in order
        times = [0, .01, .02, .20, .21, .22, .40, .41, .42, .60, .61, .62]
        st = self._run([(i % 6, t) for i, t in enumerate(times)])
        self.assertEqual(st['RX_VLOST'], 0)

    def test_dropout_longer_than_a_superframe(self):
        # A, B, then a 0.48 s gap = 8 slots: the next burst is 8 on (position D)
        st = self._run([(0, 0.0), (1, 0.06), (3, 0.54)])
        self.assertEqual(st['RX_VLOST'], 7)

    def test_whole_superframe_lost(self):
        # same position 6 slots later (0.36 s): 5 lost, not a duplicate
        st = self._run([(0, 0.0), (1, 0.06), (1, 0.42)])
        self.assertEqual(st['RX_VLOST'], 5)

    def test_duplicate_burst_ignored(self):
        st = self._run([(0, 0.0), (1, 0.06), (1, 0.07), (2, 0.12)])
        self.assertEqual((st['RX_VRECV'], st['RX_VLOST']), (3, 0))

    def test_no_voice_no_figure(self):
        st = {}
        bridge.voice_loss_reset(st)
        self.assertEqual(bridge.voice_loss_pct(st), '')

    def test_event_extras_trims_trailing_empties(self):
        self.assertEqual(bridge.event_extras('', '', ''), '')
        self.assertEqual(bridge.event_extras('-99.0', '', ''), ',-99.0')
        self.assertEqual(bridge.event_extras('', '2.5', 'c-bridge'), ',,2.5,c-bridge')


def _member(system):
    return {'SYSTEM': system, 'TS': 1, 'TGID': 3100, 'ACTIVE': True,
            'TIMEOUT': 2, 'TO_TYPE': 'NONE', 'ON': [], 'OFF': [], 'RESET': []}


class TestLossReports(unittest.TestCase):

    def setUp(self):
        self.w = World({'B': [_member('SERVER-1'), _member('OBP-1')]})
        self.w.CONFIG['REPORTS']['REPORT'] = True
        self.events = events = []

        class Rep:
            def send_bridge_event(self, data):
                events.append(data.decode() if isinstance(data, (bytes, bytearray)) else data)
        for name in ('SERVER-1', 'OBP-1'):
            bridge.systems[name]._report = Rep()

    def _feed(self, system, ft, dv, seq, ber=0):
        data = mk_dmrd(seq, SRC, TG, PEER, 1, 'group', ft, dv, SID)[:53] + bytes([ber, 0])
        bridge.systems[system].dmrd_received(PEER, SRC, TG, seq, 1, 'group', ft, dv, SID, data)

    def _call(self, system, positions, ber_on_term=0):
        self._feed(system, _FT_DATA_SYNC, _VHEAD, 0)
        for i, pos in enumerate(positions):
            self.w.clock.tick(0.06)
            self._feed(system, _FT_VSYNC if pos == 0 else _FT_VOICE, pos, i + 1)
        self.w.clock.tick(0.06)
        self._feed(system, _FT_DATA_SYNC, _VTERM, len(positions) + 1, ber=ber_on_term)

    def _end(self, system):
        return [e.split(',') for e in self.events if e.startswith('GROUP VOICE,END,RX,{},'.format(system))]

    def test_hbp_end_reports_loss(self):
        # 12 slots, position D of the first superframe missing
        self._call('SERVER-1', [p % 6 for p in range(12) if p != 3])
        end = self._end('SERVER-1')
        self.assertEqual(len(end), 1)
        self.assertEqual(end[0][11], '8.3')

    def test_obp_end_measured_loss_without_upstream_figure(self):
        self._call('OBP-1', [p % 6 for p in range(12)])
        end = self._end('OBP-1')
        self.assertEqual(len(end), 1)
        self.assertEqual(end[0][11:], ['0.0'])

    def test_obp_end_prefers_upstream_figure(self):
        # BER byte 6 on the terminator = 1 + 2 x 2.5 %: the c-Bridge's B-off LOSS
        self._call('OBP-1', [p % 6 for p in range(12)], ber_on_term=6)
        end = self._end('OBP-1')
        self.assertEqual(end[0][11:], ['2.5', 'c-bridge'])

    def test_json_carries_loss(self):
        srv = bridge.BridgeReportServer({})
        cap = []
        srv._send_json = cap.append
        srv.send_bridge_event('GROUP VOICE,END,RX,OBP-1,1,2,3,1,3100,4.20,-99.0,2.5,c-bridge')
        self.assertEqual((cap[0]['rssi'], cap[0]['loss'], cap[0]['loss_src']), (-99.0, 2.5, 'c-bridge'))
        srv.send_bridge_event('GROUP VOICE,UPDATE,RX,SERVER-1,1,2,3,1,3100,2.00,,1.5')
        self.assertEqual(cap[1]['type'], 'stream_update')
        self.assertNotIn('rssi', cap[1])
        self.assertEqual(cap[1]['loss'], 1.5)


if __name__ == '__main__':
    unittest.main()
