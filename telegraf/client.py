from abc import abstractmethod
import socket

from .protocol import Line


class ClientBase(object):

    def __init__(self, host='localhost', port=8094, tags=None):
        self.host = host
        self.port = port
        self.tags = tags or {}

    def track(self, name, values, tags=None, timestamp=None):
        metric = self.prepare_metric(
            name=name, values=values, tags=tags, timestamp=timestamp
        )
        self.send(metric.to_line_protocol())

    def prepare(self, name, values, tags=None, timestamp=None):
        tags = tags or {}

        # Do a shallow merge of the metric tags and global tags
        all_tags = dict(self.tags, **tags)

        # Create a metric line from the input and then send it to socket
        return Line(name, values, all_tags, timestamp)

    def track_prepared(self, *metrics):
        self.send('\n'.join(line.to_line_protocol() for line in metrics))

    @abstractmethod
    def send(self, data):
        pass


class TelegrafClient(ClientBase):

    def __init__(self, host='localhost', port=8092, tags=None):
        super().__init__(host, port, tags)

        # creating the socket immediately should be safe because it's UDP
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send(self, data):
        try:
            self.socket.sendto(data.encode('utf-8') + b'\n',
                               (self.host, self.port))
        except (socket.error, RuntimeError):
            pass
