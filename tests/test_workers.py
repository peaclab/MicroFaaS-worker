from datetime import timedelta
import unittest
from time import sleep
import workers


class TestBBBWorker(unittest.TestCase):
    WRKR_ID = 10
    WRKR_PIN = "P3_4"
    TEST_JOBS = [
        '[{"i_id": "TEST", "f_id": "reboot", "f_args":{ }}]',
        "TE5T",
        '{"name": "MicroFaaS"}',
    ]

    def setUp(self):
        self.w = workers.BBBWorker(self.WRKR_ID, self.WRKR_ID)

    def tearDown(self):
        self.w.deactivate()
        self.w = None

    def shorten_delays(self):
        """
        Zero-out internal worker's holdoff values and set all timeouts to small (<10 sec) values.
        Must be called before calling activate().
        """
        self.w._power_up_holdoff = timedelta(seconds=0)
        self.w._power_down_holdoff = timedelta(seconds=0)
        self.w.JOB_TIMEOUT = 6
        self.w.POWER_UP_TIMEOUT = 4
        self.w.UNKNOWN_TIMEOUT = 5

    def test_initial_state(self):
        self.assertTrue(self.w._state, workers.WorkerState.UNKNOWN)
        self.assertFalse(self.w._active)

    def test_activate(self):
        self.w.activate()
        self.assertTrue(self.w._active)
        self.assertTrue(self.w.is_active())
        self.assertTrue(self.w._state_machine_thread.is_alive())
        self.assertTrue(self.w._state_machine_thread.daemon)

    def test_deactivate(self):
        self.shorten_delays()
        self.w.activate()
        sleep(2)
        self.w.deactivate(join=True)
        self.assertFalse(self.w.is_active())
        self.assertFalse(self.w._state_machine_thread.is_alive())

    def test_enqueue_job(self):
        self.w.activate()
        for i, job in enumerate(self.TEST_JOBS):
            self.w.enqueue_job(job)
            self.assertFalse(self.w._job_queue.empty())
            self.assertEqual(self.w._job_queue.qsize(), i + 1)

        for job in self.TEST_JOBS:
            self.assertEqual(self.w._job_queue.get_nowait(), job)

        self.assertTrue(self.w._job_queue.empty())

    def test_state_machine_common(self):
        """Tests the "common" path through the state machine, where we assume a well-behaved BBB"""
        self.shorten_delays()
        self.w.activate()
        # Confirm initial state
        self.assertTrue(self.w.in_state(workers.WorkerState.UNKNOWN))
        # Assume worker is manually powered up 2s later. Should be told to reboot
        sleep(2)
        self.assertEqual(self.w.handle_worker_request(), self.w.reboot_payload())
        sleep(1)
        self.assertTrue(self.w.in_state(workers.WorkerState.REBOOTING))
        # Enqueue a couple jobs while rebooting and check event
        self.w.enqueue_job(self.TEST_JOBS[0])
        self.w.enqueue_job(self.TEST_JOBS[1])
        self.assertTrue(self.w._I.QUEUE_NOT_EMPTY.is_set())
        # Simulate reboot complete. Should receive job
        sleep(1)
        self.assertEqual(
            self.w.handle_worker_request(), self.TEST_JOBS[0].encode("ascii")
        )
        self.assertTrue(self.w.in_state(workers.WorkerState.WORKING))
        sleep(1)
        # Job complete. Should be told to reboot
        self.assertEqual(self.w.handle_worker_request(), self.w.reboot_payload())
        sleep(1)
        self.assertTrue(self.w.in_state(workers.WorkerState.REBOOTING))
        # Rebooted. Should rec'v last job
        sleep(0.5)
        self.assertEqual(
            self.w.handle_worker_request(), self.TEST_JOBS[1].encode("ascii")
        )
        self.assertTrue(self.w.in_state(workers.WorkerState.WORKING))
        sleep(0.5)
        # Final job complete. Should be told to shutdown
        self.assertEqual(self.w.handle_worker_request(), self.w.power_down_payload())
        self.assertFalse(self.w._I.QUEUE_NOT_EMPTY.is_set())
        self.assertTrue(self.w.in_state(workers.WorkerState.OFF))
        sleep(0.5)
        # Let's try enqueing one more job. Should enter power up
        self.w.enqueue_job(self.TEST_JOBS[2])
        sleep(1)
        self.assertTrue(self.w.in_state(workers.WorkerState.POWERING_UP))
        sleep(1)
        self.assertEqual(
            self.w.handle_worker_request(), self.TEST_JOBS[2].encode("ascii")
        )
        self.assertTrue(self.w.in_state(workers.WorkerState.WORKING))

    def test_state_machine_flaky(self):
        """Tests a "flaky" path through the state machine, where the BBB occasionally misbehaves"""
        self.shorten_delays()
        self.w.activate()
        # Confirm initial state
        self.assertTrue(self.w.in_state(workers.WorkerState.UNKNOWN))
        # Assume worker isn't pre-powered up. Should be told to power-up after timeout
        sleep(self.w.UNKNOWN_TIMEOUT + 1)
        self.assertTrue(self.w.in_state(workers.WorkerState.POWERING_UP))
        # Assume worker failed to respond within timeout. Should re-enter POWERING_UP
        sleep(self.w.POWER_UP_TIMEOUT + 1)
        self.assertTrue(self.w.in_state(workers.WorkerState.POWERING_UP))
        # Alright now lets send the worker request with an empty queue
        self.assertEqual(self.w.handle_worker_request(), self.w.power_down_payload())
        # Should enter shutdown
        sleep(1)
        self.assertTrue(self.w.in_state(workers.WorkerState.OFF))
        # Let's imagine worker magically rebooted and requested again.
        self.assertEqual(self.w.handle_worker_request(), self.w.power_down_payload())
        sleep(1)
        self.assertTrue(self.w.in_state(workers.WorkerState.OFF))
        # Now enqueue a job and make sure things go smoothly
        self.w.enqueue_job(self.TEST_JOBS[0])
        self.assertTrue(self.w._I.QUEUE_NOT_EMPTY.is_set())
        # Simulate reboot complete. Should receive job
        sleep(1)
        self.assertEqual(
            self.w.handle_worker_request(), self.TEST_JOBS[0].encode("ascii")
        )
        self.assertTrue(self.w.in_state(workers.WorkerState.WORKING))
        sleep(1)
        # Final job complete. Should be told to shutdown
        self.assertEqual(self.w.handle_worker_request(), self.w.power_down_payload())
        self.assertFalse(self.w._I.QUEUE_NOT_EMPTY.is_set())
        self.assertTrue(self.w.in_state(workers.WorkerState.OFF))

    def test_power_down_holdoff(self):
        """Tests holdoff obedience of BBBWorker's power down sequence"""
        # First ensure holdoff isn't too low
        if self.w._power_down_holdoff.seconds <= 2:
            raise Exception("POWER_DOWN_HOLDOFF_BBB must be >2 seconds for this test to proceed")
        
        # Pre-holdoff
        self.assertEqual(self.w.power_down_payload(), self.w.reboot_payload())
        sleep(self.w._power_down_holdoff.seconds)
        # Post-holdoff
        self.assertNotEqual(self.w.power_down_payload(), self.w.reboot_payload())
