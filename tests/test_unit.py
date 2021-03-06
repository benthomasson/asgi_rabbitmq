from __future__ import unicode_literals

import threading
import time
from collections import defaultdict

import pytest
from asgi_ipc import IPCChannelLayer
from asgi_rabbitmq import RabbitmqChannelLayer, RabbitmqLocalChannelLayer
from asgi_rabbitmq.core import EXPIRE_GROUP_MEMBER
from asgi_rabbitmq.test import RabbitmqLayerTestCaseMixin
from asgiref.conformance import ConformanceTestCase
from channels.asgi import ChannelLayerWrapper
from channels.routing import null_consumer, route
from channels.test import ChannelTestCase
from channels.worker import Worker
from django.test import SimpleTestCase, TestCase
from msgpack.exceptions import ExtraData
from pika.exceptions import ConnectionClosed


class RabbitmqChannelLayerTest(RabbitmqLayerTestCaseMixin, SimpleTestCase,
                               ConformanceTestCase):

    def setUp(self):

        self.amqp_url = '%s?heartbeat_interval=%d' % (self.amqp_url,
                                                      self.heartbeat_interval)
        self.channel_layer = self.channel_layer_cls(
            self.amqp_url,
            expiry=1,
            group_expiry=2,
            capacity=self.capacity_limit,
        )
        super(RabbitmqChannelLayerTest, self).setUp()

    @property
    def defined_queues(self):
        """Get queue names defined in current vhost."""

        definitions = self.management.get_definitions()
        queue_definitions = defaultdict(set)
        for queue in definitions['queues']:
            queue_definitions[queue['vhost']].add(queue['name'])
        queues = queue_definitions[self.virtual_host]
        return queues

    channel_layer_cls = RabbitmqChannelLayer
    expiry_delay = 1.1
    capacity_limit = 5
    heartbeat_interval = 15

    def test_send_to_empty_group(self):
        """Send to empty group works as usual."""

        self.skip_if_no_extension('groups')
        self.channel_layer.send_group('tgroup_1', {'value': 'orange'})

    def test_discard_from_empty_group(self):
        """Discard from empty group works as usual."""

        self.skip_if_no_extension('groups')
        self.channel_layer.group_discard('tgroup_2', 'tg_test3')

    def test_receive_from_non_existed_channel(self):
        """
        If we tries to receive messages from channel which queue was not
        declared already, things should works just fine.
        """

        channel, message = self.channel_layer.receive(['foo'])
        assert (channel, message) == (None, None)

    def test_group_persistence_message_expiry(self):
        """
        Discard channel from all its groups when first message expires in
        channel.
        """

        # Setup group membership.
        self.skip_if_no_extension('groups')
        self.channel_layer.group_add('tgme_group1', 'tgme_test')
        self.channel_layer.group_add('tgme_group2', 'tgme_test')
        self.channel_layer.send('tgme_test', {'hello': 'world'})
        # Wait until message in the channel expires.
        time.sleep(self.channel_layer.expiry + 1)
        # Channel lost its membership in the group #1.
        self.channel_layer.send_group('tgme_group1', {'hello': 'world1'})
        channel, message = self.channel_layer.receive(['tgme_test'])
        self.assertIs(channel, None)
        self.assertIs(message, None)
        # Channel lost its membership in the group #2.
        self.channel_layer.send_group('tgme_group2', {'hello': 'world2'})
        channel, message = self.channel_layer.receive(['tgme_test'])
        self.assertIs(channel, None)
        self.assertIs(message, None)

    def test_connection_heartbeats(self):
        """
        We must answer for RabbitMQ heartbeat frames responsively.
        Otherwise connection will be closed by server.
        """

        self.channel_layer.send('x', {'foo': 'bar'})
        channel, message = self.channel_layer.receive(['x'])
        time.sleep(self.heartbeat_interval * 3)
        # Code below will throw an exception if we don't send
        # heartbeat frames during sleep.
        self.channel_layer.send('x', {'baz': 'quux'})
        channel, message = self.channel_layer.receive(['x'])

    @pytest.mark.xfail
    def test_group_channels(self):

        # TODO: figure out how to check group membership.
        super(RabbitmqChannelLayerTest, self).test_group_channels()

    def test_declare_queues_on_worker_ready(self):
        """Declare necessary queues after worker start."""

        wrapper = ChannelLayerWrapper(
            channel_layer=self.channel_layer,
            alias='default',
            # NOTE: Similar to `channels.routing.Router.check_default` result.
            routing=[
                route('http.request', null_consumer),
                route('websocket.connect', null_consumer),
                route('websocket.receive', null_consumer),
            ],
        )
        worker = Worker(channel_layer=wrapper, signal_handlers=False)
        worker.ready()
        assert self.defined_queues.issuperset({
            'http.request',
            'websocket.receive',
            'websocket.connect',
        })

    def test_skip_another_layer_on_worker_ready(self):
        """
        Don't try to declare rabbit queues if worker uses another layer
        implementation.
        """

        wrapper = ChannelLayerWrapper(
            channel_layer=IPCChannelLayer(),
            alias='default',
            # NOTE: Similar to `channels.routing.Router.check_default` result.
            routing=[
                route('http.request', null_consumer),
                route('websocket.connect', null_consumer),
                route('websocket.receive', null_consumer),
            ],
        )
        worker = Worker(channel_layer=wrapper, signal_handlers=False)
        worker.ready()
        assert self.defined_queues.isdisjoint({
            'http.request',
            'websocket.receive',
            'websocket.connect',
        })

    def test_new_channel_declare_queue(self):
        """`new_channel` must declare queue if its name is available."""

        name = self.channel_layer.new_channel('test.foo!')
        queue = 'amq.gen-' + name.rsplit('!', 1)[-1]
        assert queue in self.defined_queues

    def test_new_channel_removes_internal_prefix(self):
        """New channel name shouldn't contain `amq.gen-` after ! or ?."""

        name = self.channel_layer.new_channel('test.foo!')
        assert 'amq.gen-' not in name

    def test_new_channel_capacity(self):
        """Channel created with `new_channel` must support capacity check."""

        name = self.channel_layer.new_channel('test.foo!')
        for _ in range(self.capacity_limit):
            self.channel_layer.send(name, {'hey': 'there'})
        with self.assertRaises(self.channel_layer.ChannelFull):
            self.channel_layer.send(name, {'hey': 'there'})

    def test_per_channel_capacity(self):

        layer = self.channel_layer_cls(
            self.amqp_url,
            expiry=1,
            group_expiry=2,
            capacity=self.capacity_limit,
            channel_capacity={
                'http.response!*': 10,
                'http.request': 30,
            },
        )
        # Test direct match.
        for _ in range(30):
            layer.send('http.request', {'hey': 'there'})
        with pytest.raises(self.channel_layer.ChannelFull):
            layer.send('http.request', {'hey': 'there'})
        # Test regexp match.
        name = layer.new_channel('http.response!')
        for _ in range(10):
            layer.send(name, {'hey': 'there'})
        with pytest.raises(self.channel_layer.ChannelFull):
            layer.send(name, {'hey': 'there'})

    def test_add_reply_channel_to_group(self):
        """
        Reply channel is the most popular candidate for group membership.
        Check we can do it.
        """

        name = self.channel_layer.new_channel('test.foo!')
        self.channel_layer.group_add('test.group', name)
        self.channel_layer.send_group('test.group', {'foo': 'bar'})
        channel, message = self.channel_layer.receive([name])
        assert channel == name
        assert message == {'foo': 'bar'}
        self.channel_layer.group_discard('test.group', name)
        self.channel_layer.send_group('test.group', {'foo': 'bar'})
        channel, message = self.channel_layer.receive([name])
        assert channel is None
        assert message is None

    def test_access_to_the_layer_instance_from_different_threads(self):
        """
        Operations made by one thread shouldn't change connection state
        made from another thread.
        """

        def receive_non_existed():
            time.sleep(0.015)
            self.channel_layer.receive(['bar'])

        thread = threading.Thread(target=receive_non_existed)
        thread.deamon = True
        thread.start()

        # If all things goes fine, this line will be executed
        # successfully.
        self.channel_layer.group_add('my_group', 'foo')

    def test_receive_blocking_mode(self):
        """Check we can wait until message arrives and return it."""

        name = self.channel_layer.new_channel('foo!')

        def wait_and_send():
            time.sleep(1)
            self.channel_layer.send(name, {'bar': 'baz'})

        thread = threading.Thread(target=wait_and_send)
        thread.deamon = True
        thread.start()

        channel, message = self.channel_layer.receive([name], block=True)
        assert channel == name
        assert message == {'bar': 'baz'}

    def test_send_group_message_expiry(self):
        """
        Tests that messages expire correctly when it was sent to group.
        """
        self.channel_layer.group_add('gr_test', 'me_test')
        self.channel_layer.send_group('gr_test', {'value': 'blue'})
        time.sleep(self.expiry_delay)
        channel, message = self.channel_layer.receive(['me_test'])
        self.assertIs(channel, None)
        self.assertIs(message, None)

    def test_group_add_is_idempotent(self):
        """
        Calling group_add continuously should set system into the right
        state.
        """

        name = self.channel_layer.new_channel('ch_test!')
        self.channel_layer.group_add('gr_test', name)
        self.channel_layer.thread.schedule(
            EXPIRE_GROUP_MEMBER,
            'gr_test',
            name,
        )
        time.sleep(0.1)
        # NOTE: Implementation detail.  Dead letters consumer should
        # ignore messages died with maxlen reason.  This messages
        # caused by sequential group_add calls.
        self.channel_layer.send_group('gr_test', {'value': 'blue'})
        channel, message = self.channel_layer.receive([name])
        assert channel == name
        assert message == {'value': 'blue'}

    def test_connection_on_close_notify_futures(self):
        """
        If connection in the connection thread was closed for some reason
        we should notify waiting thread about this error.
        """

        # Wait for connection established.
        while not self.channel_layer.thread.connection.protocols:
            time.sleep(0.5)
        # Get dead letters future.
        future = self.channel_layer.thread.connection.protocols[None].resolve
        # Look into on_close_callback.
        self.channel_layer.thread.connection.connection.close()
        with pytest.raises(ConnectionClosed):
            future.result()

    def test_deny_schedule_calls_to_the_closed_connection(self):
        """
        If connection is already closed, it shouldn't be possible to call
        layer methods on it.
        """

        # Wait for connection established.
        while not self.channel_layer.thread.connection.connection.is_open:
            time.sleep(0.5)
        name = self.channel_layer.new_channel('foo!')
        # Close connection and wait for it.
        self.channel_layer.thread.connection.connection.close()
        while not self.channel_layer.thread.connection.connection.is_closed:
            time.sleep(0.5)
        # Look into is_closed check.
        with pytest.raises(ConnectionClosed):
            self.channel_layer.send(name, {'bar': 'baz'})

    def test_resolve_callbacks_during_connection_close(self):
        """
        Connection can be in closing state.  If during this little time
        frame another thread tries to schedule callback into this
        connection, we should interrupt immediately.
        """

        # Wait for connection established.
        while not self.channel_layer.thread.connection.connection.is_open:
            time.sleep(0.5)
        name = self.channel_layer.new_channel('foo!')
        # Try to call layer send right after connection close frame
        # was sent.
        self.channel_layer.thread.connection.connection.close()
        with pytest.raises(ConnectionClosed):
            self.channel_layer.send(name, {'bar': 'baz'})

    def test_message_cryptography(self):
        """
        We can encrypt messages.  Layer without crypto keys can't read
        messages sent with the layer which has one.
        """

        name = self.channel_layer.new_channel('foo!')
        crypto_layer = self.channel_layer_cls(
            self.amqp_url,
            expiry=1,
            group_expiry=2,
            capacity=self.capacity_limit,
            symmetric_encryption_keys=['test', 'old'],
        )

        crypto_layer.send(name, {'bar': 'baz'})
        with pytest.raises(ExtraData):
            self.channel_layer.receive([name])
        crypto_layer.send(name, {'bar': 'baz'})
        channel, message = crypto_layer.receive([name])
        assert channel == name
        assert message == {'bar': 'baz'}


class RabbitmqLocalChannelLayerTest(RabbitmqChannelLayerTest):

    channel_layer_cls = RabbitmqLocalChannelLayer

    def test_send_normal_channel_to_local_layer(self):
        """If this is usual channel we must use local channel layer."""

        self.channel_layer.send('foo', {'bar': 'baz'})
        assert 'foo' not in self.defined_queues

    def test_send_reply_channel_to_rabbitmq_layer(self):
        """If this is reply channel we must use rabbitmq channel layer."""

        name = self.channel_layer.new_channel('foo!')
        self.channel_layer.send(name, {'bar': 'baz'})
        assert 'amq.gen-' + name[4:] in self.defined_queues

        name = self.channel_layer.new_channel('foo?')
        self.channel_layer.send(name, {'bar': 'baz'})
        assert 'amq.gen-' + name[4:] in self.defined_queues

    def test_groups(self):
        """Tests that basic group addition and send works."""

        self.skip_if_no_extension('groups')
        name1 = self.channel_layer.new_channel('tg_test!')
        name2 = self.channel_layer.new_channel('tg_test!')
        name3 = self.channel_layer.new_channel('tg_test!')
        # Make a group and send to it
        self.channel_layer.group_add('tgroup', name1)
        self.channel_layer.group_add('tgroup', name2)
        self.channel_layer.group_add('tgroup', name3)
        self.channel_layer.group_discard('tgroup', name3)
        self.channel_layer.send_group('tgroup', {'value': 'orange'})
        # Receive from the two channels in the group and ensure messages
        channel, message = self.channel_layer.receive([name1])
        assert channel == name1
        assert message == {'value': 'orange'}
        channel, message = self.channel_layer.receive([name2])
        assert channel == name2
        assert message == {'value': 'orange'}
        # Make sure another channel does not get a message
        channel, message = self.channel_layer.receive([name3])
        assert channel is None
        assert message is None


class InheritanceTest(TestCase):

    def test_mixin_inheritance_verification(self):
        """
        RabbitmqLayerTestCaseMixin should deny multiple inheritance
        together with ChannelTestCaseMixin.  This mixin substitute our
        layer with inmemory one, but our goal here to test real project
        against real broker.
        """

        class Test(RabbitmqLayerTestCaseMixin, ChannelTestCase):

            def runTest(self):

                pass

        # Emulate test run.
        test = Test()
        result = test.defaultTestResult()
        test(result)
        [(_, error)] = result.errors
        assert 'django.core.exceptions.ImproperlyConfigured' in error
