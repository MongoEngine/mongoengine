# -*- coding: utf-8 -*-
from __future__ import with_statement
import unittest

from mongoengine import *
from mongoengine.base import NotRegistered, InvalidDocumentError, get_document
from mongoengine.queryset import InvalidQueryError
from mongoengine.connection import get_db, get_connection

class GlobalVersionLockTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        self.db = get_db()

        connect("test")

        class Task(EmbeddedDocument):
            meta = {"allow_inheritance": False}
            description = StringField()

        class TodoList(Document):
            meta = {
                "version_locks": ["version"],
                "allow_inheritance": False
            }
            name = StringField()
            version = IntField(db_field = "ver")
            tasks_version = IntField(db_field = "tver")
            tasks = ListField(EmbeddedDocumentField("Task", db_field="t"))

        self.TodoList = TodoList
        self.Task = Task

    def tearDown(self):
        self.TodoList.drop_collection()

    def test_can_create(self):
        """Ensure that a document with a global version lock can be created.
        """
        todo_list = self.TodoList(name='Test').save()

        self.assertIsInstance(todo_list, self.TodoList)
        self.assertEquals(todo_list.tasks, [])
        self.assertEquals(todo_list.name, 'Test')
        self.assertIsNone(todo_list.version)

    def test_can_update(self):
        """Ensure that a document with a global version lock can be updated.
        """
        todo_list = self.TodoList(name='Test').save()
        todo_list.name = "Changed name"
        todo_list.save()

        self.assertEquals(todo_list.tasks, [])
        self.assertEquals(todo_list.name, 'Changed name')
        self.assertEquals(todo_list.version, 1)

    def test_created_object_is_properly_saved(self):
        """Ensure that a document with a global version lock is actually
        saved to mongodb when created.
        """
        todo_list = self.TodoList(name='Test').save()
        todo_list = self.TodoList.objects.get(id = todo_list.id)

        self.assertEquals(todo_list.tasks, [])
        self.assertEquals(todo_list.name, 'Test')
        self.assertIsNone(todo_list.version)

    def test_updated_object_is_properly_saved(self):
        """Ensure that a document with a global version lock is actually
        saved to mongodb when updated.
        """
        todo_list = self.TodoList(name='Test').save()
        todo_list.name = "Changed name"
        todo_list.save()

        todo_list = self.TodoList.objects.get(id = todo_list.id)

        self.assertEquals(todo_list.tasks, [])
        self.assertEquals(todo_list.name, 'Changed name')
        self.assertEquals(todo_list.version, 1)

    def test_version_is_incremented(self):
        """Ensure that a document's global version lock is incremented
        each time the document is saved.
        """
        todo_list = self.TodoList(name='Test').save()
        self.assertIsNone(todo_list.version)
        
        todo_list.name = "First change"
        todo_list.save()
        self.assertEquals(todo_list.version, 1)
        
        todo_list.name = "Second change"
        todo_list.save()
        self.assertEquals(todo_list.version, 2)

        todo_list = self.TodoList.objects.get(id = todo_list.id)
        self.assertEquals(todo_list.version, 2)

    def test_lock_conflict(self):
        """Ensure that a document is *not* saved when the version recorded
        in mongodb does not correspond to the version in the document object.
        A VersionLockError should be raised instead.
        """
        todo_list1 = self.TodoList(name='Test').save()
        todo_list2 = self.TodoList.objects.get(id = todo_list1.id)

        todo_list1.name = "First change"
        todo_list1.save()
        
        todo_list2.name = "Second change (conflicts)"
        self.assertRaises(VersionLockError, todo_list2.save)

class AttributeVersionLockTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        self.db = get_db()

        connect("test")

        class Task(EmbeddedDocument):
            meta = {"allow_inheritance": False}
            description = StringField()

        class TodoList(Document):
            meta = { "allow_inheritance": False }
            name = StringField()
            version = IntField(db_field = "ver")
            tasks = ListField(EmbeddedDocumentField("Task", db_field="t"),
                              version_locks = ["version"])

        self.TodoList = TodoList
        self.Task = Task

    def tearDown(self):
        self.TodoList.drop_collection()

    def test_can_create(self):
        """Ensure that a document with an attribute version lock can be created.
        """
        todo_list = self.TodoList(name='Test').save()

        self.assertIsInstance(todo_list, self.TodoList)
        self.assertEquals(todo_list.tasks, [])
        self.assertEquals(todo_list.name, 'Test')
        self.assertIsNone(todo_list.version)

    def test_can_update(self):
        """Ensure that a document with an attribute version lock can be updated.
        """
        todo_list = self.TodoList(name='Test').save()
        todo_list.name = "Changed name"
        todo_list.save()

        self.assertEquals(todo_list.tasks, [])
        self.assertEquals(todo_list.name, 'Changed name')
        self.assertIsNone(todo_list.version)

        todo_list.tasks = [self.Task(description = "Bake")]
        todo_list.save()

        self.assertEquals(todo_list.tasks, [self.Task(description = "Bake")])
        self.assertEquals(todo_list.name, 'Changed name')
        self.assertEquals(todo_list.version, 1)

    def test_created_object_is_properly_saved(self):
        """Ensure that a document with an attribute version lock is actually
        saved to mongodb when created.
        """
        todo_list = self.TodoList(name='Test').save()
        todo_list = self.TodoList.objects.get(id = todo_list.id)

        self.assertEquals(todo_list.tasks, [])
        self.assertEquals(todo_list.name, 'Test')
        self.assertIsNone(todo_list.version)

    def test_updated_object_is_properly_saved(self):
        """Ensure that a document with an attribute version lock is actually
        saved to mongodb when updated.
        """
        todo_list = self.TodoList(name='Test').save()
        todo_list.name = "Changed name"
        todo_list.save()

        todo_list = self.TodoList.objects.get(id = todo_list.id)

        self.assertEquals(todo_list.tasks, [])
        self.assertEquals(todo_list.name, 'Changed name')
        self.assertIsNone(todo_list.version)

        todo_list.tasks = [self.Task(description = "Bake")]
        todo_list.save()

        todo_list = self.TodoList.objects.get(id = todo_list.id)

        self.assertEquals(todo_list.tasks, [self.Task(description = "Bake")])
        self.assertEquals(todo_list.name, 'Changed name')
        self.assertEquals(todo_list.version, 1)

    def test_version_is_incremented(self):
        """Ensure that a document's attribute version lock is incremented
        each time the document is saved *and* the attribute has been modified.
        """
        todo_list = self.TodoList(name='Test').save()
        self.assertIsNone(todo_list.version)
        
        todo_list.name = "First change"
        todo_list.save()
        self.assertIsNone(todo_list.version)
        
        todo_list.name = "Second change"
        todo_list.save()
        self.assertIsNone(todo_list.version)

        todo_list.name = "Third change"
        todo_list.tasks = [self.Task(description = "Party")]
        todo_list.save()
        self.assertEquals(todo_list.version, 1)

        todo_list.name = "Fourth change"
        todo_list.save()
        self.assertEquals(todo_list.version, 1)

        todo_list.name = "Third change"
        todo_list.tasks[0].description = "Party all night"
        todo_list.save()
        self.assertEquals(todo_list.version, 2)

        todo_list = self.TodoList.objects.get(id = todo_list.id)
        self.assertEquals(todo_list.version, 2)

    def test_lock_conflict(self):
        """Ensure that a document is *not* saved when the version recorded
        in mongodb does not correspond to the version in the document object.
        A VersionLockError should be raised instead.
        """
        todo_list1 = self.TodoList(name='Test').save()
        todo_list2 = self.TodoList.objects.get(id = todo_list1.id)

        self.assertIsNone(todo_list1.version)
        self.assertIsNone(todo_list2.version)

        todo_list1.name = "First change"
        todo_list1.save()
        self.assertIsNone(todo_list1.version)
        self.assertIsNone(todo_list2.version)
        
        todo_list2.name = "Second change (no conflict)"
        todo_list2.save()
        self.assertIsNone(todo_list1.version)
        self.assertIsNone(todo_list2.version)

        todo_list1.tasks = [self.Task(description = "Buy presents")]
        todo_list1.save()
        self.assertEquals(todo_list1.version, 1)
        self.assertIsNone(todo_list2.version)

        todo_list2.name = "Third change (still no conflict)"
        todo_list2.save()
        self.assertEquals(todo_list1.version, 1)
        self.assertIsNone(todo_list2.version)

        todo_list2.tasks = [self.Task(description = "Party")]
        self.assertRaises(VersionLockError, todo_list2.save)
        self.assertEquals(todo_list1.version, 1)
        self.assertIsNone(todo_list2.version)

        todo_list2.reload()
        self.assertEquals(todo_list1.version, 1)
        self.assertEquals(todo_list2.version, 1)

        todo_list2.tasks.append(self.Task(description = "Party"))
        todo_list2.save()
        self.assertEquals(todo_list1.version, 1)
        self.assertEquals(todo_list2.version, 2)

        todo_list1.tasks.append(self.Task(description = "Get drunk"))
        self.assertRaises(VersionLockError, todo_list1.save)

        todo_list1.reload()
        self.assertEquals(todo_list1.version, 2)
        self.assertEquals(todo_list2.version, 2)

        todo_list1.tasks.append(self.Task(description = "Get drunk"))
        todo_list1.save()
        self.assertEquals(todo_list1.version, 3)
        self.assertEquals(todo_list2.version, 2)

if __name__ == '__main__':
    unittest.main()
