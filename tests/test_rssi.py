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

    def _updates(self):
        return [e for e in self.events if e.startswith('GROUP VOICE,UPDATE,RX,SERVER-1,')]

    def test_live_update_on_change_rate_limited(self):
        self._frame(_FT_DATA_SYNC, _VHEAD, 0)
        self._frame(_FT_VOICE, 0, 99, seq=1)       # first reading -> update
        self._frame(_FT_VOICE, 1, 99, seq=2)       # unchanged -> none
        self._frame(_FT_VOICE, 2, 101, seq=3)      # changed, but < 1 s later -> none
        self.w.clock.tick(1.1)
        self._frame(_FT_VOICE, 3, 101, seq=4)      # changed vs last sent, >= 1 s -> update
        self._frame(_FT_VOICE, 4, 101, seq=5)      # unchanged -> none
        ups = self._updates()
        self.assertEqual(len(ups), 2, ups)
        self.assertTrue(ups[0].endswith(',-99.0'), ups[0])
        self.assertTrue(ups[1].endswith(',-101.0'), ups[1])

    def test_no_live_update_without_rssi(self):
        self._call([0, 0, 0])
        self.assertEqual(self._updates(), [])

    def test_update_csv_becomes_stream_update(self):
        srv = bridge.BridgeReportServer({})
        cap = []
        srv._send_json = cap.append
        srv.send_bridge_event('GROUP VOICE,UPDATE,RX,SERVER-1,1,2,3,1,3100,2.16,-99.0')
        self.assertEqual(cap[0]['type'], 'stream_update')
        self.assertEqual(cap[0]['rssi'], -99.0)

    def test_obp_origin_end_reports_terminator_rssi(self):
        # A call arriving over OpenBridge (e.g. cc2obp relaying the c-Bridge): the
        # end-of-call RSSI rides the terminator's trailer.
        obp = bridge.systems['OBP-1']
        def frame(ft, dv, rssi, seq):
            data = mk_dmrd(seq, SRC, TG, PEER, 1, 'group', ft, dv, SID)[:54] + bytes([rssi])
            obp.dmrd_received(PEER, SRC, TG, seq, 1, 'group', ft, dv, SID, data)
        frame(_FT_DATA_SYNC, _VHEAD, 0, 0)
        frame(_FT_VOICE, 0, 0, 1)
        frame(_FT_DATA_SYNC, _VTERM, 108, 2)
        end = [e for e in self.events if e.startswith('GROUP VOICE,END,RX,OBP-1,')]
        self.assertEqual(len(end), 1, self.events)
        self.assertTrue(end[0].endswith(',-108.0'), end[0])

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


class TestObpReceiveTrailer(unittest.TestCase):
    """OPENBRIDGE.datagram_received accepts the 55-byte body (75-byte packet) only
    when RSSI_TRAILER is set; the standard 73-byte packet is always accepted."""

    def _obp(self, trailer):
        import copy, hblink, config
        cfg = config.build_config(os.path.join(_HERE, 'harness.cfg'))
        cfg['SYSTEMS']['OBP-1']['RSSI_TRAILER'] = trailer
        obp = hblink.OPENBRIDGE('OBP-1', cfg, None)
        got = []
        obp.dmrd_received = lambda *a: got.append(a[-1])
        return obp, cfg['SYSTEMS']['OBP-1'], got

    def _pkt(self, sysconf, body):
        from hmac import new as hmac_new
        from hashlib import sha1
        return body + hmac_new(sysconf['PASSPHRASE'], body, sha1).digest()

    def test_75_byte_accepted_with_trailer(self):
        obp, sc, got = self._obp(True)
        body = mk_dmrd(0, SRC, TG, PEER, 1, 'group', _FT_DATA_SYNC, _VTERM, SID)[:54] + bytes([108])
        obp.datagram_received(self._pkt(sc, body), sc['TARGET_SOCK'])
        self.assertEqual(len(got), 1)
        self.assertEqual(len(got[0]), 55)
        self.assertEqual(got[0][54], 108)

    def test_73_byte_still_accepted_with_trailer(self):
        obp, sc, got = self._obp(True)
        body = mk_dmrd(0, SRC, TG, PEER, 1, 'group', _FT_DATA_SYNC, _VTERM, SID)[:53]
        obp.datagram_received(self._pkt(sc, body), sc['TARGET_SOCK'])
        self.assertEqual([len(d) for d in got], [53])

    def test_75_byte_rejected_without_trailer(self):
        obp, sc, got = self._obp(False)
        body = mk_dmrd(0, SRC, TG, PEER, 1, 'group', _FT_DATA_SYNC, _VTERM, SID)[:54] + bytes([108])
        obp.datagram_received(self._pkt(sc, body), sc['TARGET_SOCK'])
        self.assertEqual(got, [])     # HMAC is checked over 53 bytes, so it fails
