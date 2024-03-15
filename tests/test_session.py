import threading
import unittest

from mongoengine import sessions


class SessionTest(unittest.TestCase):
    def tearDown(self):
        sessions.clear_all()

    def test_set_get_local_session(self):
        session = {"db": "1"}
        sessions.set_local_session("test", session)
        self.assertEqual(session, sessions.get_local_session("test"))

        session2 = {"db": "2"}
        sessions.set_local_session("test2", session2)
        self.assertEqual(session2, sessions.get_local_session("test2"))

        self.assertNotEqual(
            sessions.get_local_session("test2"), sessions.get_local_session("test")
        )

        sessions.clear_local_session("test")
        self.assertIsNone(sessions.get_local_session("test"))

        sessions.clear_local_session("test2")
        self.assertIsNone(sessions.get_local_session("test2"))

    def test_set_get_local_session_multi_threads(self):
        def new_session(i):
            db_alias = "test"
            session = {"db": i}
            sessions.set_local_session(db_alias, session)
            self.assertEqual(i, sessions.get_local_session(db_alias)["db"])
            sessions.clear_local_session(db_alias)

        threads = []
        for i in range(10):
            t = threading.Thread(target=new_session, args=(i,))
            threads.append(t)

        # Start them all
        for thread in threads:
            thread.start()

        # Wait for all to complete
        for thread in threads:
            thread.join()


if __name__ == "__main__":
    unittest.main()
