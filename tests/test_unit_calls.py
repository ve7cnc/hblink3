#!/usr/bin/env python
#
# Private (unit) calls across OpenBridge: a talkgroup ACL does not apply to a unit
# call's destination (a subscriber), only a system in rules.UNIT takes one in, and a
# UNIT VOICE START report (which carries its target list) parses.
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
