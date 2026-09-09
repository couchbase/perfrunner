import threading
import time
from unittest import TestCase

from perfrunner.remote import executor


class ConnectionPoolTest(TestCase):
    """Session lifecycle in the pool: reuse, probe-on-idle liveness, and fork safety."""

    def setUp(self):
        self.pool = executor.ConnectionPool(
            lambda host, config, gateway=None: executor.FakeSession(host=host)
        )
        self.config = executor.SessionConfig()

    def test_no_probe_when_recently_used(self):
        first = self.pool.session("h1", self.config)
        second = self.pool.session("h1", self.config)
        self.assertIs(first, second)
        self.assertEqual(first.probes, 0)

    def test_probe_after_idle_reuses_healthy_session(self):
        session = self.pool.session("h1", self.config)
        session.last_used -= executor.ConnectionPool.PROBE_AFTER_IDLE + 1
        again = self.pool.session("h1", self.config)
        self.assertIs(session, again)
        self.assertEqual(session.probes, 1)

    def test_dead_idle_session_is_replaced(self):
        session = self.pool.session("h1", self.config)
        session.last_used -= executor.ConnectionPool.PROBE_AFTER_IDLE + 1
        session.probe_error = executor.NetworkError("dropped by NAT")
        replacement = self.pool.session("h1", self.config)
        self.assertIsNot(session, replacement)
        self.assertTrue(session.closed)
        self.assertEqual(session.probes, 1)

    def test_slow_probe_does_not_block_other_hosts(self):
        # The pool lock only guards its dicts; a dead host's probe (up to the channel
        # open timeout) must not stall parallel checkouts of healthy hosts.
        slow = self.pool.session("slow-host", self.config)
        slow.last_used -= executor.ConnectionPool.PROBE_AFTER_IDLE + 1
        slow.probe_delay = 1.0

        prober = threading.Thread(target=self.pool.session, args=("slow-host", self.config))
        prober.start()
        time.sleep(0.1)  # let the probe start and hold slow-host's key lock

        t0 = time.time()
        self.pool.session("healthy-host", self.config)
        elapsed = time.time() - t0
        prober.join()
        self.assertLess(elapsed, 0.5)
        self.assertEqual(slow.probes, 1)

    def test_dead_gateway_session_closed_before_replacement(self):
        first = self.pool.session("kafka-1", self.config, gateway="jump-1")
        gateway_key = ("jump-1", self.config.user, None)
        old_gateway = self.pool._sessions[gateway_key]

        old_gateway.active = False  # gateway died; host session rides it, so it dies too
        first.active = False
        self.pool.session("kafka-1", self.config, gateway="jump-1")

        new_gateway = self.pool._sessions[gateway_key]
        self.assertIsNot(old_gateway, new_gateway)
        self.assertTrue(old_gateway.closed)

    def test_dead_session_that_fails_to_close_is_still_replaced(self):
        session = self.pool.session("h1", self.config)
        session.last_used -= executor.ConnectionPool.PROBE_AFTER_IDLE + 1
        session.probe_error = executor.NetworkError("dropped by NAT")
        session.close_error = EOFError()

        replacement = self.pool.session("h1", self.config)

        self.assertIsNot(session, replacement)
        self.assertTrue(session.closed)

    def test_session_that_failed_in_flight_is_probed_before_reuse(self):
        # Nothing else evicts a session that dies mid-command: the checkout it just made
        # refreshed last_used past the idle gate. It must not be closed from the failing
        # thread either - other threads share the same connection - so the pool probes it.
        session = self.pool.session("h1", self.config)
        session.needs_probe = True
        session.probe_error = executor.NetworkError("transport gone")

        replacement = self.pool.session("h1", self.config)

        self.assertIsNot(session, replacement)
        self.assertEqual(session.probes, 1)

    def test_a_channel_level_failure_keeps_the_shared_connection(self):
        # sshd MaxSessions surfaces as a NetworkError too, but the transport is fine, so the
        # probe passes and the session other threads are still using stays in the pool.
        session = self.pool.session("h1", self.config)
        session.needs_probe = True

        again = self.pool.session("h1", self.config)

        self.assertIs(session, again)
        self.assertEqual(session.probes, 1)
        self.assertFalse(session.needs_probe)
        self.assertFalse(session.closed)

    def test_a_suspect_session_also_flags_its_gateway(self):
        # A dead jump host kills every session tunnelled through it, so replacing only the
        # inner session would rebuild it over the same broken hop.
        session = self.pool.session("kafka-1", self.config, gateway="jump-1")
        session.gateway = self.pool._sessions[("jump-1", self.config.user, None)]

        session.mark_suspect()

        self.assertTrue(session.needs_probe)
        self.assertTrue(session.gateway.needs_probe)

    def test_forked_child_discards_inherited_sessions_without_closing(self):
        # Forked children (e.g. cbagent collector processes) must neither reuse nor close the
        # parent's SSH sockets: sharing the encrypted stream corrupts it,
        # and closing sends disconnects on a socket the parent still owns.
        parent_session = self.pool.session("h1", self.config)
        self.pool._pid -= 1  # simulate being in a forked child
        child_session = self.pool.session("h1", self.config)
        self.assertIsNot(parent_session, child_session)
        self.assertFalse(parent_session.closed)


class SSHSessionTest(TestCase):
    """Construction-time behaviour of the Fabric-backed session (no connection made)."""

    def test_host_key_policy_follows_disable_known_hosts(self):
        from paramiko.client import AutoAddPolicy, RejectPolicy

        default = executor.SSHSession("10.9.9.9", executor.SessionConfig())
        self.assertIsInstance(default._conn.client._policy, AutoAddPolicy)

        strict = executor.SSHSession("10.9.9.9", executor.SessionConfig(disable_known_hosts=False))
        self.assertIsInstance(strict._conn.client._policy, RejectPolicy)
