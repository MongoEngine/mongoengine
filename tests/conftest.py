from pymongo import MongoClient
import pytest

from tests.utils import MONGO_TEST_DB


def cleanup_databases():
    print("Cleaning up test databases...")
    with MongoClient("localhost", 27017) as client:
        db_names = client.list_database_names()

        for db_name in db_names:
            if db_name.startswith(MONGO_TEST_DB):
                print(f"Dropping test database: {db_name}")
                client.drop_database(db_name)


@pytest.fixture(scope="session", autouse=True)
def cleanup_mongoengine_databases():
    """
    Session-scoped fixture that runs automatically after all tests finish.
    Finds and drops all databases starting with 'mongoengine' using MongoClient.
    """
    cleanup_databases()
    yield
    cleanup_databases()
