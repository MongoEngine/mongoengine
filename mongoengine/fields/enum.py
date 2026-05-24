from mongoengine.base import BaseField


class EnumField(BaseField):
    """Enumeration Field. Values are stored underneath as is,
    so it will only work with simple types (str, int, etc) that
    are bson encodable

    Example usage:

    .. code-block:: python

        class Status(Enum):
            NEW = 'new'
            ONGOING = 'ongoing'
            DONE = 'done'

        class ModelWithEnum(Document):
            status = EnumField(Status, default=Status.NEW)

        ModelWithEnum(status='done')
        ModelWithEnum(status=Status.DONE)

    Enum fields can be searched using enum or its value:

    .. code-block:: python

        ModelWithEnum.objects(status='new').count()
        ModelWithEnum.objects(status=Status.NEW).count()

    The values can be restricted to a subset of the enum by using the ``choices`` parameter:

    .. code-block:: python

        class ModelWithEnum(Document):
            status = EnumField(Status, choices=[Status.NEW, Status.DONE])
    """

    def __init__(self, enum, **kwargs):
        self._enum_cls = enum
        if kwargs.get("choices"):
            invalid_choices = []
            for choice in kwargs["choices"]:
                if not isinstance(choice, enum):
                    invalid_choices.append(choice)
            if invalid_choices:
                raise ValueError("Invalid choices: %r" % invalid_choices)
        else:
            kwargs["choices"] = list(self._enum_cls)  # Implicit validator
        super().__init__(**kwargs)

    def validate(self, value, clean=True):
        if isinstance(value, self._enum_cls):
            return super().validate(value)
        try:
            self._enum_cls(value)
        except ValueError:
            self.error(f"{value} is not a valid {self._enum_cls}")

    def to_python(self, value):
        value = super().to_python(value)
        if not isinstance(value, self._enum_cls):
            try:
                return self._enum_cls(value)
            except ValueError:
                return value
        return value

    def __set__(self, instance, value):
        return super().__set__(instance, self.to_python(value))

    def to_mongo(self, value):
        if isinstance(value, self._enum_cls):
            return value.value
        return value

    def prepare_query_value(self, op, value):
        if value is None:
            return value
        return super().prepare_query_value(op, self.to_mongo(value))


__all__ = ("EnumField",)
