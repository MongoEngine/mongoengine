from mongoengine.errors import DoesNotExist, MultipleObjectsReturned

from .base_list import BaseList


class EmbeddedDocumentList(BaseList):
    @classmethod
    def __match_all(cls, embedded_doc, kwargs):
        """Return True if a given embedded doc matches all the filter
        kwargs. If it doesn't return False.
        """
        for key, expected_value in kwargs.items():
            doc_val = getattr(embedded_doc, key)
            if doc_val != expected_value and str(doc_val) != expected_value:
                return False
        return True

    @classmethod
    def __only_matches(cls, embedded_docs, kwargs):
        """Return embedded docs that match the filter kwargs."""
        if not kwargs:
            return embedded_docs
        return [doc for doc in embedded_docs if cls.__match_all(doc, kwargs)]

    def filter(self, **kwargs):
        """
        Filters the list by only including embedded documents with the
        given keyword arguments.

        This method only supports simple comparison (e.g. .filter(name='John Doe'))
        and does not support operators like __gte, __lte, __icontains like queryset.filter does

        :param kwargs: The keyword arguments corresponding to the fields to
         filter on. *Multiple arguments are treated as if they are ANDed
         together.*
        :return: A new ``EmbeddedDocumentList`` containing the matching
         embedded documents.

        Raises ``AttributeError`` if a given keyword is not a valid field for
        the embedded document class.
        """
        values = self.__only_matches(self, kwargs)
        return EmbeddedDocumentList(values, self._instance, self._name)

    def exclude(self, **kwargs):
        """
        Filters the list by excluding embedded documents with the given
        keyword arguments.

        :param kwargs: The keyword arguments corresponding to the fields to
         exclude on. *Multiple arguments are treated as if they are ANDed
         together.*
        :return: A new ``EmbeddedDocumentList`` containing the non-matching
         embedded documents.

        Raises ``AttributeError`` if a given keyword is not a valid field for
        the embedded document class.
        """
        exclude = self.__only_matches(self, kwargs)
        values = [item for item in self if item not in exclude]
        return EmbeddedDocumentList(values, self._instance, self._name)

    def count(self):
        """
        The number of embedded documents in the list.

        :return: The length of the list, equivalent to the result of ``len()``.
        """
        return len(self)

    def get(self, **kwargs):
        """
        Retrieves an embedded document determined by the given keyword
        arguments.

        :param kwargs: The keyword arguments corresponding to the fields to
         search on. *Multiple arguments are treated as if they are ANDed
         together.*
        :return: The embedded document matched by the given keyword arguments.

        Raises ``DoesNotExist`` if the arguments used to query an embedded
        document returns no results. ``MultipleObjectsReturned`` if more
        than one result is returned.
        """
        values = self.__only_matches(self, kwargs)
        if len(values) == 0:
            raise DoesNotExist("%s matching query does not exist." % self._name)
        elif len(values) > 1:
            raise MultipleObjectsReturned(
                "%d items returned, instead of 1" % len(values)
            )

        return values[0]

    def first(self):
        """Return the first embedded document in the list, or ``None``
        if empty.
        """
        if len(self) > 0:
            return self[0]

    def create(self, **values):
        """
        Creates a new instance of the EmbeddedDocument and appends it to this EmbeddedDocumentList.

        .. note::
            the instance of the EmbeddedDocument is not automatically saved to the database.
            You still need to call .save() on the parent Document.

        :param values: A dictionary of values for the embedded document.
        :return: The new embedded document instance.
        """
        name = self._name
        EmbeddedClass = self._instance._fields[name].field.document_type_obj
        self._instance[self._name].append(EmbeddedClass(**values))

        return self._instance[self._name][-1]

    def save(self, *args, **kwargs):
        """
        Saves the ancestor document.

        :param args: Arguments passed up to the ancestor Document's save
         method.
        :param kwargs: Keyword arguments passed up to the ancestor Document's
         save method.
        """
        self._instance.save(*args, **kwargs)

    async def asave(self, *args, **kwargs):
        """
        Saves the ancestor document.

        :param args: Arguments passed up to the ancestor Document's save
         method.
        :param kwargs: Keyword arguments passed up to the ancestor Document's
         save method.
        """
        await self._instance.asave(*args, **kwargs)

    def delete(self):
        """
        Deletes the embedded documents from the database.

        .. note::
            The embedded document changes are not automatically saved
            to the database after calling this method.

        :return: The number of entries deleted.
        """
        values = list(self)
        for item in values:
            self._instance[self._name].remove(item)

        return len(values)

    def update(self, **update):
        """
        Updates the embedded documents with the given replacement values. This
        function does not support mongoDB update operators such as ``inc__``.

        .. note::
            The embedded document changes are not automatically saved
            to the database after calling this method.

        :param update: A dictionary of update values to apply to each
         embedded document.
        :return: The number of entries updated.
        """
        if len(update) == 0:
            return 0
        values = list(self)
        for item in values:
            for k, v in update.items():
                setattr(item, k, v)

        return len(values)


__all__ = ("EmbeddedDocumentList",)
