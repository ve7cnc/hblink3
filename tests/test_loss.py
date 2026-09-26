#!/usr/bin/env python
#
# Voice loss comes from the sender, in the DMRD BER byte as a loss code
# (0 = not measured, else 1 + 10 x percent): ipsc2hbp on HBP server systems with
# LOSS_IN_BER, cc2obp on OpenBridge systems with RSSI_TRAILER (running figure on
# bursts, the c-Bridge's own figure on the terminator). Elsewhere the byte is a
# real bit error rate and must be ignored.
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


class TestLossCode(unittest.TestCase):

    def test_decode(self):
        self.assertEqual(bridge.loss_code_pct(0), '')
        self.assertEqual(bridge.loss_code_pct(1), '0.0')
        self.assertEqual(bridge.loss_code_pct(27), '2.6')
        self.assertEqual(bridge.loss_code_pct(255), '25.4')

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

    def _feed(self, system, ft, dv, seq, ber):
        data = mk_dmrd(seq, SRC, TG, PEER, 1, 'group', ft, dv, SID)[:53] + bytes([ber, 0])
        bridge.systems[system].dmrd_received(PEER, SRC, TG, seq, 1, 'group', ft, dv, SID, data)

    def _call(self, system, burst_codes, term_code):
        self._feed(system, _FT_DATA_SYNC, _VHEAD, 0, 0)
        for i, code in enumerate(burst_codes):
            self.w.clock.tick(0.06)
            self._feed(system, _FT_VSYNC if i % 6 == 0 else _FT_VOICE, i % 6, i + 1, code)
        self.w.clock.tick(0.06)
        self._feed(system, _FT_DATA_SYNC, _VTERM, len(burst_codes) + 1, term_code)

    def _events(self, kind, system):
        return [e.split(',') for e in self.events
                if e.startswith('GROUP VOICE,{},RX,{},'.format(kind, system))]

    def test_hbp_with_loss_in_ber(self):
        self.w.CONFIG['SYSTEMS']['SERVER-1']['LOSS_IN_BER'] = True
        self._call('SERVER-1', [1, 1, 6, 6], 6)          # running 0 % then 0.5 %
        end = self._events('END', 'SERVER-1')
        self.assertEqual(len(end), 1)
        self.assertEqual(end[0][11:], ['0.5'])            # no loss_src: our own core
        ups = self._events('UPDATE', 'SERVER-1')
        self.assertEqual(ups[0][11], '0.0')

    def test_hbp_without_loss_in_ber_ignores_ber(self):
        # e.g. an MMDVM hotspot: the byte is a real bit error rate
        self._call('SERVER-1', [7, 7, 7], 7)
        end = self._events('END', 'SERVER-1')
        self.assertEqual(len(end[0]), 10, end[0])

    def test_obp_running_then_upstream_figure(self):
        self.w.CONFIG['SYSTEMS']['OBP-1']['RSSI_TRAILER'] = True
        self._call('OBP-1', [1, 1, 201, 201], 27)         # running up to 20 %, c-Bridge says 2.6 %
        ups = self._events('UPDATE', 'OBP-1')
        self.assertEqual(ups[0][11], '0.0')
        end = self._events('END', 'OBP-1')
        self.assertEqual(end[0][11:], ['2.6', 'c-bridge'])

    def test_obp_without_trailer_ignores_ber(self):
        self._call('OBP-1', [27, 27], 27)
        end = self._events('END', 'OBP-1')
        self.assertEqual(len(end[0]), 10, end[0])

    def test_json_carries_loss(self):
        srv = bridge.BridgeReportServer({})
        cap = []
        srv._send_json = cap.append
        srv.send_bridge_event('GROUP VOICE,END,RX,OBP-1,1,2,3,1,3100,4.20,-99.0,2.6,c-bridge')
        self.assertEqual((cap[0]['rssi'], cap[0]['loss'], cap[0]['loss_src']), (-99.0, 2.6, 'c-bridge'))
        srv.send_bridge_event('GROUP VOICE,UPDATE,RX,SERVER-1,1,2,3,1,3100,2.00,,0.1')
        self.assertEqual(cap[1]['type'], 'stream_update')
        self.assertNotIn('rssi', cap[1])
        self.assertEqual(cap[1]['loss'], 0.1)


if __name__ == '__main__':
    unittest.main()
