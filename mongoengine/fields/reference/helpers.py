"""Helper functions for reference fields."""


def _unsaved_object_error(document):
    return (
        f"The instance of the document '{document}' you are "
        "trying to reference has an empty 'id'. You can only reference "
        "documents once they have been saved to the database"
    )


__all__ = ("_unsaved_object_error",)
