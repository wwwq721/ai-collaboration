"""CLI receipts survive different Windows console decoders."""
import contextlib
import io
import json
from pathlib import Path
import sys
import unittest
from unittest import mock
sys.path.insert(0,str(Path(__file__).resolve().parent.parent/'scripts'))
import send

class ConsoleTests(unittest.TestCase):
    def capture_wire(self, outcome, encoding):
        output=io.BytesIO()
        with io.TextIOWrapper(output,encoding=encoding) as stream:
            with mock.patch.object(sys,'argv',['send.py','Codex','test']), contextlib.redirect_stdout(stream):
                with mock.patch.object(send,'run',**outcome):
                    code=send.main()
            stream.flush()
            wire=output.getvalue()
        return code,wire
    def capture(self, outcome):
        code,wire=self.capture_wire(outcome,'cp936')
        self.assertTrue(wire.isascii())
        decoded=[json.loads(wire.decode(codec)) for codec in ['utf8','cp936','cp1252']]
        self.assertEqual(decoded[0],decoded[1]);self.assertEqual(decoded[1],decoded[2])
        return code,decoded[0]
    def test_completed_unicode_result_survives_console_codepages(self):
        value={'state':'completed','reply':'银环-a91d-R2：5304。完成😀'}
        code,result=self.capture({'return_value':value})
        self.assertEqual(code,0);self.assertEqual(result,value)
    def test_bridge_error_survives_console_codepages(self):
        code,result=self.capture({'side_effect':send.BridgeError('等待结束，未投递')})
        self.assertEqual(code,2);self.assertEqual(result['error'],'等待结束，未投递')
    def test_fallback_error_survives_console_codepages(self):
        code,result=self.capture({'side_effect':ValueError('private input must stay hidden')})
        self.assertEqual(code,2);self.assertIn('ValueError',result['error'])
        self.assertNotIn('private input',result['error'])
    def test_real_utf8_stream_preserves_readable_unicode_json(self):
        value={'state':'completed','reply':'银环-a91d-R2：5304。完成😀'}
        cases=[({'return_value':value},0,value),
               ({'side_effect':send.BridgeError('等待结束😀，未投递')},2,{'state':'not_sent','error':'等待结束😀，未投递'}),
               ({'side_effect':ValueError('private input must stay hidden')},2,{'state':'not_sent','error':'配置或连接检查失败（ValueError）'})]
        for outcome,expected_code,expected in cases:
            with self.subTest(outcome=outcome):
                code,wire=self.capture_wire(outcome,'utf-8')
                self.assertEqual(code,expected_code)
                self.assertEqual(json.loads(wire.decode('utf-8')),expected)
                self.assertNotIn(b'\\u',wire)
                if outcome.get('return_value') or isinstance(outcome.get('side_effect'),send.BridgeError):
                    self.assertIn('😀'.encode('utf-8'),wire)
