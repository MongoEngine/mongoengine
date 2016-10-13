from datetime import timedelta
from functools import partial
import unittest
import threading
import time
import traceback
import sys
import Queue

import freezegun
import greenlet
from tornado import ioloop

from mongoengine import pymongo_greenlet


# wrapper function to limit test execution time to 1s
def time_limited(function):

    def wrapped(*args, **kwargs):
        queue = Queue.Queue()

        def exc_wrapper(queue, *args, **kwargs):
            try:
                function(*args, **kwargs)
            except Exception:
                queue.put(sys.exc_info())

        args = (queue,) + args
        f_thread = threading.Thread(target=exc_wrapper, args=args,
                                    kwargs=kwargs)
        f_thread.daemon = True

        # so it doesn't get taken over by freezegun
        # can't join because that uses real time, so sleep 0.01s 100 times
        real_sleep = time.sleep
        f_thread.start()
        for _ in range(100):
            real_sleep(0.01)
            if not f_thread.is_alive():
                break

        assert not f_thread.is_alive(), 'execution took too long'

        # print any exception raised and raise a new exception
        try:
            exc = queue.get(block=False)
            traceback.print_exception(*exc)
            raise Exception
        except Queue.Empty:
            pass
    return wrapped


class GreenletTestCase(unittest.TestCase):
    def setUp(self):
        # don't use instance, since that would allow tests to pollute each
        # other's state
        self.ioloop = ioloop.IOLoop()

        # we don't expect exceptions from callbacks.
        def fail_immediate(callback):
            traceback.print_exception(*sys.exc_info())
            raise Exception('Exception from ioloop callback!')
        self.ioloop.handle_callback_exception = fail_immediate
        self.states = []

    # these tests are kind of weird. we're basically verifying control flow.
    # so instead of traditional assertions, flag parts of the program
    # and check the flags were passed in the correct order
    def assertState(self, max_state):
        # verify that states is 1, 2, ..., max_state
        ideal_states = [_ for _ in range(1, max_state + 1)]

        self.assertEqual(self.states, ideal_states)


class GreenletLockTestCase(GreenletTestCase):
    def setUp(self):
        super(GreenletLockTestCase, self).setUp()
        self.lock = pymongo_greenlet.GreenletLock(self.ioloop)

    @time_limited
    def test_single_acquire(self):
        def run_1():
            self.states.append(1)
            self.lock.acquire()
            self.states.append(2)
            greenlet.getcurrent().parent.switch()
            raise Exception

        def run_2():
            self.states.append(3)
            self.lock.acquire()  # shouldn't proceed
            raise Exception

        green_1 = greenlet.greenlet(run_1)
        green_2 = greenlet.greenlet(run_2)

        green_1.switch()
        green_2.switch()

        self.assertState(3)

    @time_limited
    def test_acquire_release(self):
        def run_1():
            self.states.append(1)
            self.lock.acquire()
            self.states.append(2)
            greenlet.getcurrent().parent.switch()
            self.states.append(4)
            self.lock.release()
            self.states.append(5)

        def run_2():
            self.states.append(3)
            self.lock.acquire()
            self.states.append(6)
            self.ioloop.add_callback(self.ioloop.stop)

        green_1 = greenlet.greenlet(run_1)
        green_2 = greenlet.greenlet(run_2)

        self.ioloop.add_callback(green_1.switch)
        self.ioloop.add_callback(green_2.switch)
        self.ioloop.add_callback(green_1.switch)
        self.ioloop.start()

        self.assertState(6)

    @time_limited
    def test_cannot_release_unheld_lock(self):
        def run():
            self.states.append(1)
            with self.assertRaises(AssertionError):
                self.lock.release()
                self.states.append(2)

        green = greenlet.greenlet(run)

        green.switch()

        self.assertState(1)

    @time_limited
    def test_lock_as_context_manager(self):
        def run_1():
            self.states.append(1)
            with self.lock:
                self.states.append(2)
                greenlet.getcurrent().parent.switch()
                self.states.append(4)
            self.states.append(5)

        def run_2():
            self.states.append(3)
            with self.lock:
                self.states.append(6)
                self.ioloop.add_callback(self.ioloop.stop)

        green_1 = greenlet.greenlet(run_1)
        green_2 = greenlet.greenlet(run_2)

        self.ioloop.add_callback(green_1.switch)
        self.ioloop.add_callback(green_2.switch)
        self.ioloop.add_callback(green_1.switch)
        self.ioloop.start()

        self.assertState(6)


class GreenletConditionTestCase(GreenletTestCase):
    def setUp(self):
        super(GreenletConditionTestCase, self).setUp()
        self.lock = pymongo_greenlet.GreenletLock(self.ioloop)
        self.condition = pymongo_greenlet.GreenletCondition(self.ioloop,
                                                            self.lock)

    @time_limited
    def test_wait_releases_lock(self):
        def run_1():
            self.states.append(1)
            with self.lock:
                self.states.append(2)
                self.condition.wait()

        def run_2():
            self.states.append(3)
            with self.lock:
                self.states.append(4)
                self.ioloop.add_callback(self.ioloop.stop)

        green_1 = greenlet.greenlet(run_1)
        green_2 = greenlet.greenlet(run_2)

        self.ioloop.add_callback(green_1.switch)
        self.ioloop.add_callback(green_2.switch)
        self.ioloop.start()

        self.assertState(4)

    @time_limited
    def test_notify_all_is_queue(self):
        def run_1():
            self.states.append(1)
            with self.lock:
                self.states.append(2)
                self.condition.wait()
                self.states.append(8)

        def run_2():
            self.states.append(3)
            with self.lock:
                self.states.append(4)
                self.condition.wait()
                self.states.append(9)
                self.ioloop.add_callback(self.ioloop.stop)

        def run_3():
            self.states.append(5)
            with self.lock:
                self.states.append(6)
                self.condition.notify_all()
                self.states.append(7)

        green_1 = greenlet.greenlet(run_1)
        green_2 = greenlet.greenlet(run_2)
        green_3 = greenlet.greenlet(run_3)

        self.ioloop.add_callback(green_1.switch)
        self.ioloop.add_callback(green_2.switch)
        self.ioloop.add_callback(green_3.switch)
        self.ioloop.start()

        self.assertState(9)

    @time_limited
    def test_cannot_wait_without_lock(self):
        def run():
            self.states.append(1)
            with self.assertRaises(AssertionError):
                self.condition.wait()
                self.states.append(2)

        green = greenlet.greenlet(run)

        green.switch()

        self.assertState(1)

    @time_limited
    def test_cannot_notify_without_lock(self):
        def run():
            self.states.append(1)
            with self.assertRaises(AssertionError):
                self.condition.notify_all()
                self.states.append(2)

        green = greenlet.greenlet(run)

        green.switch()

        self.assertState(1)

    @time_limited
    def test_has_lock_on_return(self):
        def run_1():
            with self.lock:
                self.condition.wait()
                current = greenlet.getcurrent()
                self.assertIs(self.lock.holder, current)
                self.ioloop.add_callback(self.ioloop.stop)

        def run_2():
            with self.lock:
                self.condition.notify_all()

        green_1 = greenlet.greenlet(run_1)
        green_2 = greenlet.greenlet(run_2)

        self.ioloop.add_callback(green_1.switch)
        self.ioloop.add_callback(green_2.switch)
        self.ioloop.start()

    @time_limited
    def test_notifies_after_timeout(self):
        def run_1():
            self.states.append(1)
            with self.lock:
                self.states.append(2)
                self.condition.wait(5)
                self.states.append(5)
                self.ioloop.add_callback(self.ioloop.stop)

        def run_2(frozen):
            self.states.append(3)
            # the point of doing this as a timeout instead of a straight
            # callback is the order in which it would get called if the other
            # would resume
            iotimeout = time.time() + 2
            self.ioloop.add_timeout(iotimeout, greenlet.getcurrent().switch)
            frozen.tick(delta=timedelta(seconds=4))
            greenlet.getcurrent().parent.switch()
            self.states.append(4)
            frozen.tick(delta=timedelta(seconds=2))
            greenlet.getcurrent().parent.switch()

        green_1 = greenlet.greenlet(run_1)
        green_2 = greenlet.greenlet(run_2)

        with freezegun.freeze_time('12:00') as frozen:
            self.ioloop.add_callback(green_1.switch)
            self.ioloop.add_callback(partial(green_2.switch, frozen))
            self.ioloop.start()

        self.assertState(5)

    @time_limited
    def test_does_not_timeout_if_notified(self):
        def run_1(frozen):
            self.states.append(1)
            with self.lock:
                self.states.append(2)
                self.condition.wait(5)
                self.states.append(5)

            frozen.tick(delta=timedelta(seconds=90))

            self.ioloop.add_callback(self.ioloop.stop)
            greenlet.getcurrent().parent.switch()
            raise Exception

        def run_2():
            self.states.append(3)
            with self.lock:
                self.condition.notify_all()
            self.states.append(4)
            greenlet.getcurrent().parent.switch()

        green_1 = greenlet.greenlet(run_1)
        green_2 = greenlet.greenlet(run_2)

        with freezegun.freeze_time('12:00') as frozen:
            self.ioloop.add_callback(partial(green_1.switch, frozen))
            self.ioloop.add_callback(green_2.switch)
            self.ioloop.start()

        self.assertState(5)


class GreenletPeriodicExecutorTestCase(GreenletTestCase):
    @time_limited
    def test_executor_executes_immediately(self):
        def target():
            self.states.append(2)
            self.ioloop.stop()
            return True

        executor = pymongo_greenlet.GreenletPeriodicExecutor(
            5,
            'dummy',
            target,
            self.ioloop
        )

        with freezegun.freeze_time('12:00'):
            executor.open()
            self.states.append(1)
            self.ioloop.start()

        self.assertState(2)

    @time_limited
    def test_executor_executes_multiple_times(self):
        class State(object):
            executed = 0

        def target():
            if State.executed == 0:
                self.states.append(4)
            if State.executed == 1:
                self.states.append(7)
            if State.executed == 2:
                self.states.append(10)
                self.ioloop.stop()
            State.executed += 1
            return True

        executor = pymongo_greenlet.GreenletPeriodicExecutor(
            5,
            'dummy',
            target,
            self.ioloop
        )

        def run(frozen):
            current = greenlet.getcurrent()
            parent = current.parent
            self.states.append(2)
            executor.open()
            self.states.append(3)
            self.ioloop.add_callback(current.switch)
            parent.switch()
            self.states.append(5)
            frozen.tick(delta=timedelta(seconds=3))
            # three seconds have passed - do not execute
            self.ioloop.add_callback(current.switch)
            parent.switch()
            self.states.append(6)
            frozen.tick(delta=timedelta(seconds=3))
            # 6s, past the 5 second mark - execute
            self.ioloop.add_callback(current.switch)
            parent.switch()
            self.states.append(8)
            frozen.tick(delta=timedelta(seconds=2))
            # 8s, not past a mark, do not execute
            self.ioloop.add_callback(current.switch)
            parent.switch()
            self.states.append(9)
            frozen.tick(delta=timedelta(seconds=3))
            parent.switch()
            raise Exception


        green = greenlet.greenlet(run)

        with freezegun.freeze_time('12:00') as frozen:
            self.states.append(1)
            self.ioloop.add_callback(partial(green.switch, frozen))
            self.ioloop.start()

        self.assertState(10)

    @time_limited
    def test_executor_stops_on_false(self):
        def target():
            self.states.append(4)
            return False

        executor = pymongo_greenlet.GreenletPeriodicExecutor(
            5,
            'dummy',
            target,
            self.ioloop
        )

        def run(frozen):
            current = greenlet.getcurrent()
            parent = current.parent

            self.states.append(2)
            executor.open()
            self.states.append(3)
            self.ioloop.add_callback(current.switch)
            parent.switch()
            self.states.append(5)
            frozen.tick(delta=timedelta(seconds=3))
            self.ioloop.add_callback(current.switch)
            parent.switch()
            self.states.append(6)
            frozen.tick(delta=timedelta(seconds=3))
            self.ioloop.add_callback(current.switch)
            parent.switch()
            self.states.append(7)
            self.ioloop.stop()

        green = greenlet.greenlet(run)

        with freezegun.freeze_time('12:00') as frozen:
            self.states.append(1)
            self.ioloop.add_callback(partial(green.switch, frozen))
            self.ioloop.start()

        self.assertState(7)

    @time_limited
    def test_executor_can_start_twice_safely(self):
        def target():
            self.states.append(2)
            self.ioloop.add_callback(self.ioloop.stop)
            return True

        executor = pymongo_greenlet.GreenletPeriodicExecutor(
            5,
            'dummy',
            target,
            self.ioloop
        )

        with freezegun.freeze_time('12:00'):
            executor.open()
            executor.open()
            self.states.append(1)
            self.ioloop.start()

        self.assertState(2)

    @time_limited
    def test_executor_does_not_execute_if_immediately_closed(self):
        def target():
            raise Exception

        executor = pymongo_greenlet.GreenletPeriodicExecutor(
            5,
            'dummy',
            target,
            self.ioloop
        )

        def run():
            self.states.append(2)
            executor.open()
            self.states.append(3)
            executor.close()
            self.ioloop.add_callback(self.ioloop.stop)

        green = greenlet.greenlet(run)

        with freezegun.freeze_time('12:00'):
            self.states.append(1)
            self.ioloop.add_callback(run)
            self.ioloop.start()

        self.assertState(3)

    @time_limited
    def test_executor_executes_immediately_on_wake(self):
        class State:
            executed = 0

        def target():
            if State.executed == 0:
                self.states.append(2)
            if State.executed == 1:
                self.states.append(4)
            State.executed += 1
            return True

        executor = pymongo_greenlet.GreenletPeriodicExecutor(
            5,
            'dummy',
            target,
            self.ioloop
        )

        def run():
            current = greenlet.getcurrent()
            parent = current.parent

            self.states.append(1)
            executor.open()
            self.ioloop.add_callback(current.switch)
            parent.switch()
            self.states.append(3)
            executor.wake()
            self.ioloop.add_callback(current.switch)
            parent.switch()
            self.ioloop.stop()

        green = greenlet.greenlet(run)

        with freezegun.freeze_time('12:00'):
            self.ioloop.add_callback(green.switch)
            self.ioloop.start()

        self.assertState(4)

    @time_limited
    def test_executor_wake_then_close_does_not_execute(self):
        class State:
            executed = 0

        def target():
            if State.executed == 0:
                self.states.append(2)
            else:
                raise Exception
            State.executed += 1
            return True

        executor = pymongo_greenlet.GreenletPeriodicExecutor(
            5,
            'dummy',
            target,
            self.ioloop
        )

        def run():
            current = greenlet.getcurrent()
            parent = current.parent

            self.states.append(1)
            executor.open()
            self.ioloop.add_callback(current.switch)
            parent.switch()
            self.states.append(3)
            executor.wake()
            executor.close()
            self.ioloop.add_callback(current.switch)
            parent.switch()
            self.states.append(4)
            self.ioloop.stop()

        green = greenlet.greenlet(run)

        with freezegun.freeze_time('12:00'):
            self.ioloop.add_callback(green.switch)
            self.ioloop.start()

        self.assertState(4)

    @time_limited
    def test_executor_stops_on_exception(self):
        class ExpectedException(Exception):
            pass

        def target():
            self.states.append(2)
            raise ExpectedException

        def custom_handler(dummy):
            exc_type, _, _ = sys.exc_info()
            self.states.append(3)
            self.assertEqual(exc_type, ExpectedException)

        self.ioloop.handle_callback_exception = custom_handler

        executor = pymongo_greenlet.GreenletPeriodicExecutor(
            5,
            'dummy',
            target,
            self.ioloop
        )

        def run():
            current = greenlet.getcurrent()
            parent = current.parent

            self.states.append(1)
            executor.open()
            self.ioloop.add_callback(current.switch)
            parent.switch()
            self.states.append(4)
            self.ioloop.stop()

        green = greenlet.greenlet(run)

        with freezegun.freeze_time('12:00'):
            self.ioloop.add_callback(green.switch)
            self.ioloop.start()

        self.assertState(4)

if __name__ == '__main__':
    unittest.main()
