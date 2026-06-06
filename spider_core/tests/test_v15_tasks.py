import os
import tempfile
import unittest

import sys


sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import db_store  # noqa: E402


class TaskQueueTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        db_store.configure(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_create_and_claim_pending_task(self):
        task_id = db_store.create_task(
            "https://javdb.com/actors/QDvG?locale=zh",
            "cookie=value",
            "ua",
            "actor.csv",
            None,
        )

        created = db_store.get_task(task_id)
        self.assertEqual(created["state"], "pending")
        self.assertEqual(created["final_filename"], "actor.csv")

        claimed = db_store.claim_next_pending_task()
        self.assertEqual(claimed["task_id"], task_id)
        self.assertEqual(claimed["state"], "running")
        self.assertTrue(db_store.has_active_task())

    def test_pause_resume_and_cancel_states(self):
        task_id = db_store.create_task("https://example.test", "cookie", "ua", "", None)

        self.assertTrue(db_store.request_task_pause(task_id))
        self.assertEqual(db_store.get_task(task_id)["state"], "paused")

        self.assertTrue(db_store.resume_task_to_pending(task_id))
        self.assertEqual(db_store.get_task(task_id)["state"], "pending")

        db_store.claim_next_pending_task()
        self.assertTrue(db_store.request_task_cancel(task_id))
        self.assertEqual(db_store.get_task(task_id)["state"], "cancel_requested")

        db_store.update_task_status(task_id, state="canceled")
        self.assertFalse(db_store.resume_task_to_pending(task_id))

    def test_checkpoint_is_stored_per_task(self):
        first = db_store.create_task("https://example.test/1", "cookie", "ua", "one.csv", None)
        second = db_store.create_task("https://example.test/2", "cookie", "ua", "two.csv", None)

        db_store.save_task_checkpoint(first, {"phase": 1, "page": 3})
        db_store.save_task_checkpoint(second, {"phase": 2, "current_index": 9})

        self.assertEqual(db_store.load_task_checkpoint(first), {"phase": 1, "page": 3})
        self.assertEqual(db_store.load_task_checkpoint(second), {"phase": 2, "current_index": 9})

        db_store.clear_task_checkpoint(first)
        self.assertIsNone(db_store.load_task_checkpoint(first))
        self.assertEqual(db_store.load_task_checkpoint(second), {"phase": 2, "current_index": 9})

    def test_recover_interrupted_tasks(self):
        running = db_store.create_task("https://example.test/r", "cookie", "ua", "r.csv", None)
        canceling = db_store.create_task("https://example.test/c", "cookie", "ua", "c.csv", None)
        db_store.update_task_status(running, state="running")
        db_store.update_task_status(canceling, state="cancel_requested")

        db_store.recover_interrupted_tasks()

        self.assertEqual(db_store.get_task(running)["state"], "paused")
        self.assertEqual(db_store.get_task(canceling)["state"], "canceled")

    def test_task_logs_are_scoped_to_task(self):
        first = db_store.create_task("https://example.test/1", "cookie", "ua", "one.csv", None)
        second = db_store.create_task("https://example.test/2", "cookie", "ua", "two.csv", None)

        db_store.append_task_log(first, "first-only")
        db_store.append_task_log(second, "second-only")

        self.assertTrue(any("first-only" in log for log in db_store.get_task_logs(first)))
        self.assertFalse(any("second-only" in log for log in db_store.get_task_logs(first)))

    def test_cleanup_finished_tasks_keeps_active_tasks(self):
        pending = db_store.create_task("https://example.test/p", "cookie", "ua", "pending.csv", None)
        running = db_store.create_task("https://example.test/r", "cookie", "ua", "running.csv", None)
        finished = db_store.create_task("https://example.test/f", "cookie", "ua", "finished.csv", None)
        canceled = db_store.create_task("https://example.test/c", "cookie", "ua", "canceled.csv", None)
        failed = db_store.create_task("https://example.test/e", "cookie", "ua", "failed.csv", None)

        db_store.update_task_status(running, state="running")
        db_store.update_task_status(finished, state="finished")
        db_store.update_task_status(canceled, state="canceled")
        db_store.update_task_status(failed, state="failed")
        db_store.append_task_log(finished, "finished log")

        counts = db_store.count_tasks_by_state()
        self.assertEqual(counts["pending"], 1)
        self.assertEqual(counts["finished"], 1)

        self.assertEqual(db_store.cleanup_finished_tasks(), 3)

        self.assertIsNotNone(db_store.get_task(pending))
        self.assertIsNotNone(db_store.get_task(running))
        self.assertIsNone(db_store.get_task(finished))
        self.assertIsNone(db_store.get_task(canceled))
        self.assertIsNone(db_store.get_task(failed))
        self.assertEqual(db_store.get_task_logs(finished), [])


if __name__ == "__main__":
    unittest.main()
