from .utils import format_string, format_value


class Line(object):
    def __init__(self, name, values, tags={}, timestamp=None):
        assert name, 'Must have measurement name'

        # name of the actual measurement
        self.measurement = name
        # single value or a dict of value_name: value pairs
        self.values = values
        # dict of tags if any
        self.tags = tags
        # user provided timestamp, if any
        self.timestamp = timestamp

    def get_output_values(self):
        """Return an escaped string of comma separated value_name: value
        pairs.
        """

        # Handle primitive values here and implicitly convert them to a dict
        # because it allows the API to be simpler.
        # Also influxDB mandates that each value also has a name so the default
        # name for any non-dict value is "value".
        if not isinstance(self.values, dict):
            metric_values = {'value': self.values}
        else:
            metric_values = self.values

        # sort the values lexicographically by value name
        sorted_values = sorted(metric_values.items())

        return ','.join('{0}={1}'.format(format_string(k), format_value(v))
                        for k, v in sorted_values if v is not None)

    def get_output_tags(self):
        """Return an escaped string of comma separated tag_name: tag_value
        pairs. Tags should be sorted by key before being sent for best
        performance.
        The sort should match that from the Go bytes.
        Compare function (http://golang.org/pkg/bytes/#Compare).
        """

        # sort the tags lexicographically by tag name
        sorted_tags = sorted(self.tags.items())

        # finally render, escape and return the tag string
        return ','.join('{0}={1}'.format(format_string(k), format_string(v))
                        for k, v in sorted_tags)

    def to_line_protocol(self):
        """Converts the given metrics as a single line of InfluxDB
        line protocol.
        """

        tags = self.get_output_tags()

        return '{measurement}{tags} {values}{timestamp}'.format(
            measurement=format_string(self.measurement),
            tags=',' + tags if tags else '',
            values=self.get_output_values(),
            timestamp=' {0}'.format(self.timestamp) if self.timestamp else ''
        )
