#!/usr/bin/env python
#
# RSSI plumbing: the HBP BER/RSSI trailer byte (-dBm, 0 = not reported) is
# averaged per RX call and reported on the call's END event, and an OpenBridge
# system with RSSI_TRAILER forwards the trailer (55-byte body) instead of
# dropping it.
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

TG   = bytes_3(3100)
SRC  = bytes_3(3120001)
PEER = bytes_4(312100)
SID  = bytes_4(0xabcd)


def _member(system):
    return {'SYSTEM': system, 'TS': 1, 'TGID': 3100, 'ACTIVE': True,
            'TIMEOUT': 2, 'TO_TYPE': 'NONE', 'ON': [], 'OFF': [], 'RESET': []}


class TestRssi(unittest.TestCase):

    def setUp(self):
        self.w = World({'B': [_member('SERVER-1'), _member('OBP-1')]})
        self.events = []
        events = self.events

        class Rep:
            def send_bridge_event(self, data):
                events.append(data.decode() if isinstance(data, (bytes, bytearray)) else data)

        self.w.CONFIG['REPORTS']['REPORT'] = True
        bridge.systems['SERVER-1']._report = Rep()
        bridge.systems['OBP-1']._report = Rep()

    def _frame(self, frame_type, dtype_vseq, rssi, seq=0):
        data = mk_dmrd(seq, SRC, TG, PEER, 1, 'group', frame_type, dtype_vseq, SID)
        data = data[:54] + bytes([rssi])
        bridge.systems['SERVER-1'].dmrd_received(
            PEER, SRC, TG, seq, 1, 'group', frame_type, dtype_vseq, SID, data)

    def _call(self, rssis):
        self._frame(_FT_DATA_SYNC, _VHEAD, 0)
        for i, r in enumerate(rssis):
            self._frame(_FT_VOICE, i % 6, r, seq=i + 1)
        self._frame(_FT_DATA_SYNC, _VTERM, rssis[-1] if rssis else 0, seq=len(rssis) + 1)

    def _rx_end(self):
        return [e for e in self.events if e.startswith('GROUP VOICE,END,RX,SERVER-1,')]

    def test_end_reports_average_rssi(self):
        # 0 (no reading yet) is ignored; 99, 101, 101 and the terminator's 101 average to -100.5
        self._call([0, 99, 101, 101])
        end = self._rx_end()
        self.assertEqual(len(end), 1)
        self.assertTrue(end[0].endswith(',-100.5'), end[0])

    def test_end_without_rssi_is_unchanged(self):
        self._call([0, 0, 0])
        end = self._rx_end()
        self.assertEqual(len(end), 1)
        self.assertEqual(end[0].count(','), 9, 'no trailing rssi field when none reported')

    def test_report_json_carries_rssi(self):
        srv = bridge.BridgeReportServer({})
        cap = []
        srv._send_json = cap.append
        srv.send_bridge_event('GROUP VOICE,END,RX,SERVER-1,1,2,3,1,3100,4.20,-98.5')
        self.assertEqual(cap[0]['rssi'], -98.5)
        srv.send_bridge_event('GROUP VOICE,END,RX,SERVER-1,1,2,3,1,3100,4.20')
        self.assertNotIn('rssi', cap[1])

    def test_obp_without_trailer_sends_53_bytes(self):
        self._call([99, 99])
        pkts = self.w.emitted_to('OBP-1')
        self.assertTrue(pkts)
        self.assertTrue(all(len(p) == 53 for p in pkts))

    def test_obp_rssi_trailer_forwards_ber_rssi(self):
        self.w.CONFIG['SYSTEMS']['OBP-1']['RSSI_TRAILER'] = True
        self._call([0, 99, 101])
        pkts = self.w.emitted_to('OBP-1')
        self.assertTrue(all(len(p) == 55 for p in pkts))
        self.assertEqual([p[54] for p in pkts], [0, 0, 99, 101, 101])


if __name__ == '__main__':
    unittest.main()
