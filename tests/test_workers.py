import unittest
from time import sleep
import workers


class TestBBBWorker(unittest.TestCase):
    WRKR_ID = 10
    WRKR_PIN = "P3_4"

    def setUp(self):
        self.w = workers.BBBWorker(self.WRKR_ID, self.WRKR_ID)

    def tearDown(self):
        self.w.deactivate()
        self.w = None

    def test_initial_state(self):
        self.assertTrue(self.w._state, workers.WorkerState.UNKNOWN)
        self.assertFalse(self.w._active)

    def test_activate(self):
        self.w.activate()
        self.assertTrue(self.w._active)
        self.assertTrue(self.w.is_active())
        self.assertTrue(self.w._state_machine_thread.is_alive())
        self.assertTrue(self.w._state_machine_thread.daemon)

    def test_enqueue_job(self):
        test_jobs = [
            '[{"i_id": "TEST", "f_id": "reboot", "f_args":{ }}]',
            "TE5T",
            '{"name": "MicroFaaS"}',
        ]
        self.w.activate()
        for i, job in enumerate(test_jobs):
            self.w.enqueue_job(job)
            self.assertFalse(self.w._job_queue.empty())
            self.assertEquals(self.w._job_queue.qsize(), i + 1)

        for job in test_jobs:
            self.assertEquals(self.w._job_queue.get_nowait(), job)

        self.assertTrue(self.w._job_queue.empty())

    def test_deactivate(self):
        self.w.activate()
        sleep(2)
        self.w.deactivate(join=True)
        self.assertFalse(self.w.is_active())
        self.assertFalse(self.w._state_machine_thread.is_alive())
