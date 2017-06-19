from .document import Document
from .fields import ObjectIdField
import bson

__all__ = 'NoConflictDocument',


class NoConflictDocument(Document):
    doc_ver = ObjectIdField(default=bson.ObjectId)
    meta = {
        'indexes': ['doc_ver'],
        'allow_inheritance': True,
    }

    def save(self, force_insert=False, validate=True, clean=True,
             write_concern=None, cascade=None, cascade_kwargs=None,
             _refs=None, save_condition=None, signal_kwargs=None, **kwargs):

        # If there are no changes, don't increment the version and force an actual save.
        if not hasattr(self, '_changed_fields') or not self._changed_fields:
            return super(NoConflictDocument, self).save(force_insert, validate, clean,
                                                        write_concern, cascade, cascade_kwargs,
                                                        _refs, save_condition, signal_kwargs, **kwargs)

        # Grab the doc_ver and use it in the save statement.
        # Ensure that we have not seen a change by using the save condition.
        op_save_condition = {'doc_ver': self.doc_ver}

        # Refresh doc_ver for this document's state, to be saved with changes.
        self.doc_ver = bson.ObjectId()

        if save_condition:
            if not isinstance(save_condition, dict):
                raise ValueError("save_condition must be empty or a dictionary")

            # If there is a passed save condition, merge the two dictionaries
            # giving precedence to our doc_ver statement.
            save_condition.update(op_save_condition)
            op_save_condition = save_condition

        return super(NoConflictDocument, self).save(force_insert, validate, clean,
                                                    write_concern, cascade, cascade_kwargs,
                                                    _refs, op_save_condition, signal_kwargs, **kwargs)
