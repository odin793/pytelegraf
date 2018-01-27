from unittest import TestCase, mock

from .client import ClientBase, TelegrafClient
from .protocol import Line
from .utils import format_string, format_value


class TestLine(TestCase):

    def test_format_key(self):
        self.assertEquals(format_string('foo'), 'foo')
        self.assertEquals(format_string('foo,bar'), r'foo\,bar')
        self.assertEquals(format_string('foo bar'), r'foo\ bar')
        self.assertEquals(format_string('foo ,bar'), r'foo\ \,bar')
        self.assertEquals(format_string('foo ,bar,baz=foobar'),
                          r'foo\ \,bar\,baz\=foobar')

    def test_format_value(self):
        self.assertEquals(format_value('foo'), '"foo"')
        self.assertEquals(format_value('foo bar'), '"foo bar"')
        self.assertEquals(format_value('foo "and" bar'),
                          r'"foo \"and\" bar"')
        self.assertEquals(format_value(123), "123i")
        self.assertEquals(format_value(123.123), "123.123")
        self.assertEquals(format_value(True), "True")
        self.assertEquals(format_value(False), "False")

    def test_single_value(self):
        self.assertEquals(
            Line('some_series', 1).to_line_protocol(),
            'some_series value=1i'
        )

    def test_single_value_and_tags(self):
        line = Line('some_series', 1, {'foo': 'bar', 'foobar': 'baz'})
        self.assertEquals(line.to_line_protocol(),
                          'some_series,foo=bar,foobar=baz value=1i')

    def test_multiple_values(self):
        line = Line('some_series', {'value': 232.123, 'value2': 123})
        self.assertEquals(line.to_line_protocol(),
                          'some_series value=232.123,value2=123i')

    def test_multiple_values_and_tags(self):
        line = Line('some_series', {'value': 232.123, 'value2': 123},
                    {'foo': 'bar', 'foobar': 'baz'})
        self.assertEquals(
            line.to_line_protocol(),
            'some_series,foo=bar,foobar=baz value=232.123,value2=123i'
        )

    def test_tags_and_measurement_with_whitespace_and_comma(self):
        line = Line('white space', {'value, comma': 'foo'},
                    {'tag with, comma': 'hello, world'})
        self.assertEquals(
            line.to_line_protocol(),
            (r'white\ space,tag\ with\,\ comma=hello\,\ world '
             r'value\,\ comma="foo"')
        )

    def test_boolean_value(self):
        self.assertEquals(
            Line('some_series', True).to_line_protocol(),
            'some_series value=True'
        )

    def test_value_escaped_and_quoted(self):
        self.assertEquals(
            Line('some_series', 'foo "bar"').to_line_protocol(),
            r'some_series value="foo \"bar\""'
        )

    def test_with_timestamp(self):
        self.assertEquals(
            Line('some_series', 1000, timestamp=1234134).to_line_protocol(),
            'some_series value=1000i 1234134'
        )

    def test_tags_ordered_properly(self):
        line = Line('some_series', 1, {'a': 1, 'baa': 1, 'AAA': 1, 'aaa': 1})
        self.assertEquals(
            line.to_line_protocol(),
            'some_series,AAA=1,a=1,aaa=1,baa=1 value=1i'
        )

    def test_values_ordered_properly(self):
        line = Line('some_series', {'a': 1, 'baa': 1, 'AAA': 1, 'aaa': 1})
        self.assertEquals(line.to_line_protocol(),
                          'some_series AAA=1i,a=1i,aaa=1i,baa=1i')


class TestClientBase(TestCase):

    def test_zero_value(self):
        self.client = ClientBase('localhost', '1234', None)
        self.client.send = mock.Mock()
        self.client.track('some_series', 0)
        self.client.send.assert_called_with('some_series value=0i')

    def test_null_value(self):
        self.client = ClientBase('localhost', '1234', None)
        with self.assertRaises(AssertionError):
            self.client.track('some_series', None)

    def test_empty_values_dict(self):
        self.client = ClientBase('localhost', '1234', None)
        with self.assertRaises(AssertionError):
            self.client.track('some_series', {})

    def test_some_zero_values(self):
        self.client = ClientBase('localhost', '1234', None)
        self.client.send = mock.Mock()
        self.client.track(
            'some_series',
            {'value_one': 1, 'value_zero': 0, 'value_none': None}
        )
        self.client.send.assert_called_with(
            'some_series value_one=1i,value_zero=0i'
        )


class TestTelegraf(TestCase):
    def setUp(self):
        self.host = 'localhost'
        self.port = 8092
        self.addr = (self.host, self.port)

    def test_sending_to_socket(self):
        self.client = TelegrafClient(self.host, self.port)
        self.client.socket = mock.Mock()

        self.client.track('some_series', 1)
        self.client.socket.sendto.assert_called_with(
            b'some_series value=1i\n', self.addr
        )
        self.client.track('cpu', {'value_int': 1},
                          {'host': 'server-01', 'region': 'us-west'})
        self.client.socket.sendto.assert_called_with(
            b'cpu,host=server-01,region=us-west value_int=1i\n', self.addr
        )

    def test_global_tags(self):
        self.client = TelegrafClient(self.host, self.port,
                                     tags={'host': 'host-001'})
        self.client.socket = mock.Mock()

        self.client.track('some_series', 1)
        self.client.socket.sendto.assert_called_with(
            b'some_series,host=host-001 value=1i\n', self.addr
        )

        self.client.track('some_series', 1, tags={'host': 'override-host-tag'})
        self.client.socket.sendto.assert_called_with(
            b'some_series,host=override-host-tag value=1i\n', self.addr
        )

    def test_utf8_encoding(self):
        self.client = TelegrafClient(self.host, self.port)
        self.client.socket = mock.Mock()

        self.client.track(u'meäsurement',
                          fields={u'välue': 1, u'këy': u'valüe'},
                          tags={u'äpples': u'öranges'})
        self.client.socket.sendto.assert_called_with(
            (b'me\xc3\xa4surement,\xc3\xa4pples=\xc3\xb6ranges '
             b'k\xc3\xaby="val\xc3\xbce",v\xc3\xa4lue=1i\n'),
            self.addr
        )
