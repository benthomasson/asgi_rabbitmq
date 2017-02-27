from collections import deque
from functools import partial

import msgpack
from asgiref.base_layer import BaseChannelLayer
from channels.signals import worker_ready
from pika import SelectConnection, URLParameters
from pika.exceptions import ConnectionClosed
from pika.spec import BasicProperties
from twisted.internet import defer, reactor

try:
    from concurrent.futures import Future
except ImportError:
    from futures import Future

try:
    from threading import Thread, Lock, get_ident
except ImportError:
    from threading import Thread, Lock, _get_ident as get_ident

SEND = 0
RECEIVE = 1
NEW_CHANNEL = 2
DECLARE_CHANNEL = 3
GROUP_ADD = 4
GROUP_DISCARD = 5
SEND_GROUP = 6
DECLARE_DEAD_LETTERS = 7
EXPIRE_GROUP_MEMBER = 8
RECEIVE_TWISTED = 9


class Protocol(object):

    dead_letters = 'dead-letters'

    def __init__(self, expiry, group_expiry, capacity, channel_capacity, ident,
                 process):

        self.expiry = expiry
        self.group_expiry = group_expiry
        self.capacity = capacity
        self.channel_capacity = channel_capacity
        self.ident = ident
        self.process = process

        self.methods = {
            SEND: self.send,
            RECEIVE: self.receive,
            NEW_CHANNEL: self.new_channel,
            DECLARE_CHANNEL: self.declare_channel,
            GROUP_ADD: self.group_add,
            GROUP_DISCARD: self.group_discard,
            SEND_GROUP: self.send_group,
            DECLARE_DEAD_LETTERS: self.declare_dead_letters,
            EXPIRE_GROUP_MEMBER: self.expire_group_member,
            RECEIVE_TWISTED: self.receive_twisted,
        }
        self.messages = {}
        self.receive_is_blocked_by = set()

    # Utilities.

    def register_channel(self, method, amqp_channel):

        self.amqp_channel = amqp_channel
        self.apply(*method)

    def apply(self, method_id, args, kwargs):

        self.methods[method_id](*args, **kwargs)

    def get_queue_name(self, channel):

        if '!' in channel:
            return channel.rsplit('!', 1)[-1]
        elif '?' in channel:
            return channel.rsplit('?', 1)[-1]
        else:
            return channel

    # Send.

    def send(self, channel, message):

        queue = self.get_queue_name(channel)
        body = self.serialize(message)
        self.amqp_channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=body,
            properties=self.publish_properties,
        )
        self.resolve.set_result(None)

    @property
    def publish_properties(self):

        expiration = str(self.expiry * 1000)
        properties = BasicProperties(expiration=expiration)
        return properties

    # Declare channel.

    def declare_channel(self, channel):

        self.amqp_channel.queue_declare(
            partial(self.channel_declared, channel),
            queue=channel,
            arguments={'x-dead-letter-exchange': self.dead_letters},
        )

    def channel_declared(self, channel, method_frame):

        self.subscribe_to_channel(channel)

    # Receive.

    def subscribe_to_channel(self, channel):

        self.messages[channel] = deque()
        self.amqp_channel.basic_consume(
            partial(self.accept_message, channel),
            queue=self.get_queue_name(channel),
            no_ack=True,
        )
        self.resolve.set_result(channel)

    def accept_message(self, channel_name, amqp_channel, method_frame,
                       properties, body):

        # FIXME: Literally I don't understand what the FUCK is going on?!
        if channel_name in self.receive_is_blocked_by:
            try:
                resolve = self.receive_is_blocked_by.future
            except AttributeError:
                resolve = self.resolve
            self.receive_is_blocked_by = set()
            resolve.set_result((channel_name, self.deserialize(body)))
        else:
            self.messages[channel_name].append(body)

    def receive(self, channels, block):

        for channel in channels:
            messages = self.messages.get(channel)
            if messages:
                body = messages.popleft()
                message = self.deserialize(body)
                self.resolve.set_result((channel, message))
                break
        else:
            if block:
                self.receive_is_blocked_by = channels
            else:
                self.resolve.set_result((None, None))

    def receive_twisted(self):

        for channel, messages in self.messages.items():
            if messages:
                body = messages.popleft()
                message = self.deserialize(body)
                self.resolve.set_result((channel, message))
                break
        else:
            self.receive_is_blocked_by = AnyChannel(self.resolve)

    # New channel.

    def new_channel(self, pattern):

        self.amqp_channel.queue_declare(
            partial(self.new_channel_declared, pattern),
        )

    def new_channel_declared(self, pattern, method_frame):

        self.subscribe_to_channel(pattern + method_frame.method.queue)

    # Groups.

    def group_add(self, group, channel):

        # FIXME: Is it possible to do this things in parallel?
        self.expire_group_member(group, channel)

        def after_bind(method_frame):

            self.resolve.set_result(None)

        def bind_channel(method_frame):

            self.amqp_channel.queue_bind(
                after_bind,
                queue=self.get_queue_name(channel),
                exchange=channel,
            )

        def bind_group(method_frame):

            self.amqp_channel.exchange_bind(
                bind_channel,
                destination=channel,
                source=group,
            )

        def declare_channel(method_frame):
            if '!' in channel or '?' in channel:
                bind_group(None)
            else:
                self.amqp_channel.queue_declare(
                    bind_group,
                    queue=channel,
                    arguments={'x-dead-letter-exchange': self.dead_letters},
                )

        def declare_member(method_frame):
            self.amqp_channel.exchange_declare(
                declare_channel,
                exchange=channel,
                exchange_type='fanout',
            )

        self.amqp_channel.exchange_declare(
            declare_member,
            exchange=group,
            exchange_type='fanout',
        )

    def group_discard(self, group, channel):

        self.amqp_channel.exchange_unbind(
            lambda method_frame: self.resolve.set_result(None),
            destination=channel,
            source=group,
        )

    def send_group(self, group, message):

        body = self.serialize(message)
        self.amqp_channel.basic_publish(
            exchange=group,
            routing_key='',
            body=body,
            properties=self.publish_properties,
        )

    # Dead letters processing.

    def expire_group_member(self, group, channel):

        ttl = self.group_expiry * 1000
        self.amqp_channel.queue_declare(
            partial(self.push_marker, group, channel),
            queue=self.get_expire_marker(group, channel),
            arguments={
                'x-dead-letter-exchange': self.dead_letters,
                'x-message-ttl': ttl,
                # FIXME: make this delay as little as possible.
                'x-expires': ttl + 500,
                'x-max-length': 1,
            },
        )

    def push_marker(self, group, channel, method_frame):

        body = self.serialize({
            'group': group,
            'channel': channel,
        })
        self.amqp_channel.basic_publish(
            exchange='',
            routing_key=self.get_expire_marker(group, channel),
            body=body,
        )

    def get_expire_marker(self, group, channel):

        return 'expire.bind.%s.%s' % (group, self.get_queue_name(channel))

    def declare_dead_letters(self):

        def consume(method_frame):

            self.amqp_channel.basic_consume(
                self.on_dead_letter,
                queue=self.dead_letters,
            )

        def do_bind(method_frame):

            self.amqp_channel.queue_bind(
                consume,
                queue=self.dead_letters,
                exchange=self.dead_letters,
            )

        def declare_queue(method_frame):

            self.amqp_channel.queue_declare(
                do_bind,
                queue=self.dead_letters,
            )

        self.amqp_channel.exchange_declare(
            declare_queue,
            exchange=self.dead_letters,
            exchange_type='fanout',
        )

    def on_dead_letter(self, amqp_channel, method_frame, properties, body):

        # FIXME: what the hell zero means here?
        queue = properties.headers['x-death'][0]['queue']
        reason = properties.headers['x-death'][0]['reason']
        if self.is_expire_marker(queue) and reason == 'expired':
            message = self.deserialize(body)
            group = message['group']
            channel = message['channel']
            self.group_discard(group, channel)
        elif not self.is_expire_marker(queue):
            amqp_channel.exchange_delete(exchange=queue)

    def is_expire_marker(self, queue):

        return queue.startswith('expire.bind.')

    # Serialization.

    def serialize(self, message):

        value = msgpack.packb(message, use_bin_type=True)
        return value

    def deserialize(self, message):

        return msgpack.unpackb(message, encoding='utf8')


class LayerConnection(SelectConnection):

    def __init__(self, *args, **kwargs):

        self.on_callback_error_callback = kwargs.pop(
            'on_callback_error_callback',
        )
        super(LayerConnection, self).__init__(*args, **kwargs)

    def _process_callbacks(self, frame_value):

        try:
            return super(LayerConnection, self)._process_callbacks(frame_value)
        except Exception as error:
            self.on_callback_error_callback(frame_value, error)
            raise


class RabbitmqConnection(object):

    Parameters = URLParameters
    Connection = LayerConnection
    Protocol = Protocol

    def __init__(self, url, expiry, group_expiry, capacity, channel_capacity):

        self.url = url
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.capacity = capacity
        self.channel_capacity = channel_capacity

        self.protocols = {}
        self.lock = Lock()
        self.lock.acquire()
        self.parameters = self.Parameters(self.url)
        self.connection = self.Connection(
            parameters=self.parameters,
            on_open_callback=self.start_loop,
            on_close_callback=self.notify_futures,
            on_callback_error_callback=self.protocol_error,
            stop_ioloop_on_close=False,
        )

    def run(self):

        self.connection.ioloop.start()

    def start_loop(self, connection):

        self.process(None, (DECLARE_DEAD_LETTERS, (), {}), Future())
        self.lock.release()

    def process(self, ident, method, future):

        if (ident in self.protocols and
                self.protocols[ident].amqp_channel.is_open):
            self.protocols[ident].resolve = future
            self.protocols[ident].apply(*method)
            return
        protocol = self.Protocol(self.expiry, self.group_expiry, self.capacity,
                                 self.channel_capacity, ident, self.process)
        protocol.resolve = future
        self.connection.channel(
            partial(protocol.register_channel, method),
        )
        # FIXME: possible race condition.
        #
        # Second schedule call was made after we create channel for
        # the first call, but before RabbitMQ respond with Ok.  We
        # should not fail with attribute error.
        self.protocols[ident] = protocol

    def notify_futures(self, connection, code, msg):

        try:
            for protocol in self.protocols.values():
                protocol.resolve.set_exception(ConnectionClosed())
        finally:
            self.connection.ioloop.stop()

    def protocol_error(self, frame_value, error):

        for protocol in self.protocols.values():
            protocol.resolve.set_exception(error)

    def schedule(self, f, *args, **kwargs):

        if self.connection.is_closing or self.connection.is_closed:
            raise ConnectionClosed
        future = Future()
        with self.lock:
            self.process(get_ident(), (f, args, kwargs), future)
        return future


class ConnectionThread(Thread):
    """
    Thread holding connection.

    Separate heartbeat frames processing from actual work.
    """

    Connection = RabbitmqConnection

    def __init__(self, url, expiry, group_expiry, capacity, channel_capacity):

        super(ConnectionThread, self).__init__()
        self.daemon = True
        self.connection = self.Connection(url, expiry, group_expiry, capacity,
                                          channel_capacity)

    def run(self):

        self.connection.run()

    def schedule(self, f, *args, **kwargs):

        return self.connection.schedule(f, *args, **kwargs)


class RabbitmqChannelLayer(BaseChannelLayer):

    extensions = ['groups', 'twisted']

    Thread = ConnectionThread

    def __init__(self,
                 url,
                 expiry=60,
                 group_expiry=86400,
                 capacity=100,
                 channel_capacity=None):

        super(RabbitmqChannelLayer, self).__init__(
            expiry=expiry,
            group_expiry=group_expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
        )
        self.thread = self.Thread(url, expiry, group_expiry, capacity,
                                  channel_capacity)
        self.thread.start()

    def send(self, channel, message):

        future = self.thread.schedule(SEND, channel, message)
        return future.result()

    def receive(self, channels, block=False):

        future = self.thread.schedule(RECEIVE, channels, block)
        return future.result()

    @defer.inlineCallbacks
    def receive_twisted(self):
        """Twisted-native implementation of receive."""

        deferred = defer.Deferred()

        def resolve_deferred(future):

            reactor.callFromThread(deferred.callback, future.result())

        future = self.thread.schedule(RECEIVE_TWISTED)
        future.add_done_callback(resolve_deferred)
        defer.returnValue((yield deferred))

    def new_channel(self, pattern):

        assert pattern.endswith('!') or pattern.endswith('?')
        future = self.thread.schedule(NEW_CHANNEL, pattern)
        return future.result()

    def declare_channel(self, channel):

        future = self.thread.schedule(DECLARE_CHANNEL, channel)
        return future.result()

    def group_add(self, group, channel):

        future = self.thread.schedule(GROUP_ADD, group, channel)
        return future.result()

    def group_discard(self, group, channel):

        future = self.thread.schedule(GROUP_DISCARD, group, channel)
        return future.result()

    def send_group(self, group, message):

        self.thread.schedule(SEND_GROUP, group, message)


# TODO: is it optimal to read bytes from content frame, call python
# decode method to convert it to string and than parse it with
# msgpack?  We should minimize useless work on message receive.


def worker_start_hook(sender, **kwargs):
    """Declare necessary queues we gonna listen."""

    layer_wrapper = sender.channel_layer
    layer = layer_wrapper.channel_layer
    if not isinstance(layer, RabbitmqChannelLayer):
        return
    channels = sender.apply_channel_filters(layer_wrapper.router.channels)
    for channel in channels:
        layer.declare_channel(channel)


# FIXME: This must be optional since we don't require channels package
# to be installed.
worker_ready.connect(worker_start_hook)


class AnyChannel(object):

    def __init__(self, future):

        self.future = future

    def __contains__(self, _):
        return True
