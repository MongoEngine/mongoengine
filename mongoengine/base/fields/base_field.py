import threading
import weakref

from mongoengine.base.common import UPDATE_OPERATORS, _DocumentRegistry
from mongoengine.common import _import_class
from mongoengine.errors import DeprecatedError, ValidationError, NotRegistered


class BaseField:
    """A base class for fields in a MongoDB document. Instances of this class
    may be added to subclasses of `Document` to define a document's schema.
    """

    name = None  # set in TopLevelDocumentMetaclass
    _geo_index = False
    _auto_gen = False  # Call `generate` to generate a value
    _thread_local_storage = threading.local()

    # These track each time a Field instance is created. Used to retain order.
    # The auto_creation_counter is used for fields that MongoEngine implicitly
    # creates, creation_counter is used for all user-specified fields.
    creation_counter = 0
    auto_creation_counter = -1

    def __init__(
        self,
        db_field=None,
        required=False,
        default=None,
        unique=False,
        unique_with=None,
        primary_key=False,
        validation=None,
        choices=None,
        null=False,
        sparse=False,
        **kwargs,
    ):
        """
        :param db_field: The database field to store this field in
            (defaults to the name of the field)
        :param required: If the field is required. Whether it has to have a
            value or not. Defaults to False.
        :param default: (optional) The default value for this field if no value
            has been set, if the value is set to None or has been unset. It can be a
            callable.
        :param unique: Is the field value unique or not (Creates an index).  Defaults to False.
        :param unique_with: (optional) The other field this field should be
            unique with (Creates an index).
        :param primary_key: Mark this field as the primary key ((Creates an index)). Defaults to False.
        :param validation: (optional) A callable to validate the value of the
            field. The callable takes the value as parameter and should raise
            a ValidationError if validation fails
        :param choices: (optional) The valid choices
        :param null: (optional) If the field value can be null when a default exists. If not set, the default value
            will be used in case a field with a default value is set to None. Defaults to False.
        :param sparse: (optional) `sparse=True` combined with `unique=True` and `required=False`
            means that uniqueness won't be enforced for `None` values (Creates an index). Defaults to False.
        :param **kwargs: (optional) Arbitrary indirection-free metadata for
            this field can be supplied as additional keyword arguments and
            accessed as attributes of the field. Must not conflict with any
            existing attributes. Common metadata includes `verbose_name` and
            `help_text`.
        """
        self.db_field = db_field if not primary_key else "_id"

        self.required = required or primary_key
        self.default = default
        self.unique = bool(unique or unique_with)
        self.unique_with = unique_with
        self.primary_key = primary_key
        self.validation = validation
        self.choices = choices
        self.null = null
        self.sparse = sparse
        self._owner_document = None

        # Make sure db_field is a string (if it's explicitly defined).
        if self.db_field is not None and not isinstance(self.db_field, str):
            raise TypeError("db_field should be a string.")

        # Make sure db_field doesn't contain any forbidden characters.
        if isinstance(self.db_field, str) and (
            "." in self.db_field
            or "\0" in self.db_field
            or self.db_field.startswith("$")
        ):
            raise ValueError(
                'field names cannot contain dots (".") or null characters '
                '("\\0"), and they must not start with a dollar sign ("$").'
            )

        # Detect and report conflicts between metadata and base properties.
        conflicts = set(dir(self)) & set(kwargs)
        if conflicts:
            raise TypeError(
                "%s already has attribute(s): %s"
                % (self.__class__.__name__, ", ".join(conflicts))
            )

        # Assign metadata to the instance
        # This efficient method is available because no __slots__ are defined.
        self.__dict__.update(kwargs)

        # Adjust the appropriate creation counter, and save our local copy.
        if self.db_field == "_id":
            self.creation_counter = BaseField.auto_creation_counter
            BaseField.auto_creation_counter -= 1
        else:
            self.creation_counter = BaseField.creation_counter
            BaseField.creation_counter += 1

    def __get__(self, instance, owner):
        """Descriptor for retrieving a value from a field in a document."""
        if instance is None:
            # Document class being used rather than a document object
            return self

        # Get value from document instance if available
        return instance._data.get(self.name)

    def __set__(self, instance, value):
        """Descriptor for assigning a value to a field in a document."""
        # If setting to None and there is a default value provided for this
        # field, then set the value to the default value.
        if value is None:
            if self.null:
                value = None
            elif self.default is not None:
                value = self.default
                if callable(value):
                    value = value()

        if instance._initialised:
            try:
                value_has_changed = (
                    self.name not in instance._data
                    or instance._data[self.name] != value
                )
                if value_has_changed:
                    instance._mark_as_changed(self.name)
            except Exception:
                # Some values can't be compared and throw an error when we
                # attempt to do so (e.g. tz-naive and tz-aware datetimes).
                # Mark the field as changed in such cases.
                instance._mark_as_changed(self.name)

        EmbeddedDocument = _import_class("EmbeddedDocument")
        if isinstance(value, EmbeddedDocument):
            value._instance = weakref.proxy(instance)
        elif isinstance(value, (list, tuple)):
            for v in value:
                if isinstance(v, EmbeddedDocument):
                    v._instance = weakref.proxy(instance)

        instance._data[self.name] = value

    def error(self, message="", errors=None, field_name=None):
        """Raise a ValidationError."""
        field_name = field_name if field_name else self.name
        raise ValidationError(message, errors=errors, field_name=field_name)

    def to_python(self, value):
        """Convert a MongoDB-compatible type to a Python type."""
        return value

    def to_mongo(self, value):
        """Convert a Python type to a MongoDB-compatible type."""
        return self.to_python(value)

    def _to_mongo_safe_call(self, value, use_db_field=True, fields=None):
        """Helper method to call to_mongo with proper inputs."""
        f_inputs = self.to_mongo.__code__.co_varnames
        ex_vars = {}
        if "fields" in f_inputs:
            ex_vars["fields"] = fields

        if "use_db_field" in f_inputs:
            ex_vars["use_db_field"] = use_db_field

        return self.to_mongo(value, **ex_vars)

    def prepare_query_value(self, op, value):
        """Prepare a value that is being used in a query for PyMongo."""
        if op in UPDATE_OPERATORS:
            self.validate(value)
        return value

    def validate(self, value, clean=True):
        """Perform validation on a value."""
        pass

    def _validate_choices(self, value):
        Document = _import_class("Document")
        EmbeddedDocument = _import_class("EmbeddedDocument")
        GenericReferenceField = _import_class("GenericReferenceField")

        choice_list = []
        for choice in self.choices:
            if isinstance(self, GenericReferenceField) and isinstance(choice, str):
                try:
                    choice_list.append(_DocumentRegistry.get(choice))
                except NotRegistered:
                    self.error(
                        f"{choice} has not been registered in the document registry."
                    )
            else:
                choice_list.append(choice)
        choice_list = tuple(choice_list)

        if isinstance(next(iter(choice_list)), (list, tuple)):
            # next(iter) is useful for sets
            choice_list = [k for k, _ in choice_list]

        # Choices which are other types of Documents
        if isinstance(value, (Document, EmbeddedDocument)):
            if not any(isinstance(value, c) for c in choice_list):
                self.error(f"Value must be an instance of {choice_list}")
        # Choices which are types other than Documents
        else:
            values = value if isinstance(value, (list, tuple)) else [value]
            if len(set(values) - set(choice_list)):
                self.error(
                    "Value must be one of %s"
                    % str(
                        choice_list,
                    )
                )

    def _validate(self, value, **kwargs):
        # Check the Choices Constraint
        if self.choices:
            self._validate_choices(value)

        # check validation argument
        if self.validation is not None:
            if callable(self.validation):
                try:
                    # breaking change of 0.18
                    # Get rid of True/False-type return for the validation method
                    # in favor of having validation raising a ValidationError
                    ret = self.validation(value)
                    if ret is not None:
                        raise DeprecatedError(
                            "validation argument for `%s` must not return anything, "
                            "it should raise a ValidationError if validation fails"
                            % self.name
                        )
                except ValidationError as ex:
                    self.error(str(ex))
            else:
                raise ValueError(
                    'validation argument for `"%s"` must be a callable.' % self.name
                )

        self.validate(value, **kwargs)

    @property
    def owner_document(self):
        return self._owner_document

    def _set_owner_document(self, owner_document):
        self._owner_document = owner_document

    @owner_document.setter
    def owner_document(self, owner_document):
        self._set_owner_document(owner_document)


__all__ = ("BaseField",)
