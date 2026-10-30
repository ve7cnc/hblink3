#!/usr/bin/env python
#
# Private (unit) calls: a talkgroup ACL does not apply to a unit call's destination (a
# subscriber), only a system in rules.UNIT takes one in over OpenBridge, where a call
# goes (learned from any call on a UNIT system, kept UNIT_TIME, unheard = UNIT_FLOOD),
# and a UNIT VOICE START report (which carries its target list) parses.
#
# Run from the repo root:   venv/bin/python -m unittest discover -s tests

import copy
import os
import sys
import unittest
from hashlib import sha1
from hmac import new as hmac_new

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)

import bridge
import config
import hblink
from const import DMRD
from dmr_utils3.utils import bytes_3, bytes_4

import harness
from baseline_scenarios import _full_call, _full_unit_call, _member

CFG = config.build_config(os.path.join(_HERE, 'harness.cfg'))


def _obp(system='OBP-1'):
    cfg = copy.deepcopy(CFG)
    sc = cfg['SYSTEMS'][system]
    sc['USE_ACL'] = True
    sc['TG1_ACL'] = config.acl_build('PERMIT:3027', 16776415)
    got = []
    obp = bridge.routerOBP(system, cfg, None)
    obp.group_received = lambda *a: got.append(('group', a[2]))
    obp.unit_received = lambda *a: got.append(('unit', a[2]))
    return obp, sc, got


def _frame(sc, dst, unit, src=3027004, slot=1, sid=b'\x01\x02\x03\x04'):
    bits = (0x40 if unit else 0x00) | (0x80 if slot == 2 else 0x00)
    body = (DMRD + b'\x00' + src.to_bytes(3, 'big') + dst.to_bytes(3, 'big')
            + (302719).to_bytes(4, 'big') + bytes([bits]) + sid + b'\x00' * 33)
    return body + hmac_new(sc['PASSPHRASE'], body, sha1).digest()


class TestUnitCallsOverOpenBridge(unittest.TestCase):
    def setUp(self):
        self._unit = bridge.UNIT

    def tearDown(self):
        bridge.UNIT = self._unit

    def test_talkgroup_acl_still_filters_group_calls(self):
        obp, sc, got = _obp()
        bridge.UNIT = ['OBP-1']
        obp.datagram_received(_frame(sc, 3027, False), ('127.0.0.1', 62041))
        obp.datagram_received(_frame(sc, 3100, False, sid=b'\x09\x09\x09\x09'), ('127.0.0.1', 62041))
        self.assertEqual([g[0] for g in got], ['group'])

    def test_a_unit_call_passes_the_talkgroup_acl_into_a_unit_system(self):
        obp, sc, got = _obp()
        bridge.UNIT = ['OBP-1']
        obp.datagram_received(_frame(sc, 3027083, True), ('127.0.0.1', 62041))
        self.assertEqual([g[0] for g in got], ['unit'])

    def test_an_openbridge_not_in_unit_takes_no_unit_calls(self):
        obp, sc, got = _obp()
        bridge.UNIT = []                    # e.g. a partner network
        obp.datagram_received(_frame(sc, 3027083, True), ('127.0.0.1', 62041))
        self.assertEqual(got, [])


def _sent_to(w):
    return sorted({name for name, _ in w.captures})


class TestWhereAUnitCallGoes(unittest.TestCase):
    """Learning from any call on a UNIT system, UNIT_TIME, and UNIT_FLOOD = False."""

    def test_learned_from_a_group_call_over_a_unit_openbridge(self):
        w = harness.World({'B': [_member('SERVER-1', 1, 3100)]},
                          unit_systems=['SERVER-1', 'SERVER-2', 'OBP-1'])
        _full_call(w, 'OBP-1', rf_src=bytes_3(3027004), dst=bytes_3(3100),
                   peer=bytes_4(302145), slot=1, stream_id=b'\x00\x00\x01\x01')
        self.assertEqual(bridge.UNIT_MAP[bytes_3(3027004)][0], 'OBP-1')
        w.captures.clear()
        _full_unit_call(w, 'SERVER-1', rf_src=bytes_3(3027083), dst=bytes_3(3027004),
                        peer=bytes_4(302719), slot=1, stream_id=b'\x00\x00\x01\x02')
        self.assertEqual(_sent_to(w), ['OBP-1'])            # straight there, no flood

    def test_not_learned_over_an_openbridge_outside_unit(self):
        w = harness.World({'B': [_member('SERVER-1', 1, 3100)]},
                          unit_systems=['SERVER-1', 'SERVER-2'], extra_systems=['OBP-1'])
        _full_call(w, 'OBP-1', rf_src=bytes_3(3027004), dst=bytes_3(3100),
                   peer=bytes_4(302145), slot=1, stream_id=b'\x00\x00\x01\x03')
        self.assertNotIn(bytes_3(3027004), bridge.UNIT_MAP)  # e.g. a partner's user

    def test_unheard_is_dropped_when_unit_flood_is_off(self):
        w = harness.World({}, unit_systems=['SERVER-1', 'SERVER-2', 'OBP-1'])
        w.CONFIG['GLOBAL']['UNIT_FLOOD'] = False
        _full_unit_call(w, 'SERVER-1', rf_src=bytes_3(3027083), dst=bytes_3(3027999),
                        peer=bytes_4(302719), slot=1, stream_id=b'\x00\x00\x01\x04')
        self.assertEqual(w.captures, [])

    def test_unheard_floods_by_default_as_upstream(self):
        w = harness.World({}, unit_systems=['SERVER-1', 'SERVER-2', 'OBP-1'])
        _full_unit_call(w, 'SERVER-1', rf_src=bytes_3(3027083), dst=bytes_3(3027999),
                        peer=bytes_4(302719), slot=1, stream_id=b'\x00\x00\x01\x05')
        self.assertEqual(_sent_to(w), ['OBP-1', 'SERVER-2'])

    def test_unit_time_from_config(self):
        self.assertEqual(CFG['GLOBAL']['UNIT_TIME'], 60)      # absent: upstream's minute
        self.assertTrue(CFG['GLOBAL']['UNIT_FLOOD'])


class TestUnitStartReport(unittest.TestCase):
    def test_unit_start_with_its_target_list_parses(self):
        srv = bridge.BridgeReportServer({})
        cap = []
        srv._send_json = cap.append
        srv.send_bridge_event(b"UNIT VOICE,START,RX,VE7RYS-302719,1787382792,302719,3027004,2,"
                              b"3027083,['VE7NWR-302700', 'ng1-ng2']")
        self.assertEqual(cap[0]['type'], 'stream_start')
        self.assertEqual(cap[0]['call_type'], 'UNIT VOICE')
        self.assertEqual((cap[0]['src'], cap[0]['dst'], cap[0]['slot']), (3027004, 3027083, 2))
        self.assertNotIn('duration', cap[0])


if __name__ == '__main__':
    unittest.main()
