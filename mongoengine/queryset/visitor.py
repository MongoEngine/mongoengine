import copy

from mongoengine.errors import InvalidQueryError
from mongoengine.python_support import product, reduce

from mongoengine.queryset import transform

__all__ = ('Q',)


class QNodeVisitor(object):
    """Base visitor class for visiting Q-object nodes in a query tree.
    """

    def visit_combination(self, combination):
        """Called by QCombination objects.
        """
        return combination

    def visit_query(self, query):
        """Called by (New)Q objects.
        """
        return query


class SimplificationVisitor(QNodeVisitor):
    """Simplifies query trees by combinging unnecessary 'and' connection nodes
    into a single Q-object.
    """

    def visit_combination(self, combination):
        if combination.operation == combination.AND:
            # The simplification only applies to 'simple' queries
            if all(isinstance(node, Q) for node in combination.children):
                queries = [node.query for node in combination.children]
                return Q(**self._query_conjunction(queries))
        return combination

    def _query_conjunction(self, queries):
        """Merges query dicts - effectively &ing them together.
        """
        query_ops = set()
        combined_query = {}
        for query in queries:
            ops = set(query.keys())
            # Make sure that the same operation isn't applied more than once
            # to a single field
            intersection = ops.intersection(query_ops)
            if intersection:
                msg = 'Duplicate query conditions: '
                raise InvalidQueryError(msg + ', '.join(intersection))

            query_ops.update(ops)
            combined_query.update(copy.deepcopy(query))
        return combined_query


class QueryTreeTransformerVisitor(QNodeVisitor):
    """Transforms the query tree in to a form that may be used with MongoDB.
    """

    def visit_combination(self, combination):
        if combination.operation == combination.AND:
            # MongoDB doesn't allow us to have too many $or operations in our
            # queries, so the aim is to move the ORs up the tree to one
            # 'master' $or. Firstly, we must find all the necessary parts (part
            # of an AND combination or just standard Q object), and store them
            # separately from the OR parts.
            or_groups = []
            and_parts = []
            for node in combination.children:
                if isinstance(node, QCombination):
                    if node.operation == node.OR:
                        # Any of the children in an $or component may cause
                        # the query to succeed
                        or_groups.append(node.children)
                    elif node.operation == node.AND:
                        and_parts.append(node)
                elif isinstance(node, Q):
                    and_parts.append(node)

            # Now we combine the parts into a usable query. AND together all of
            # the necessary parts. Then for each $or part, create a new query
            # that ANDs the necessary part with the $or part.
            clauses = []
            for or_group in product(*or_groups):
                q_object = reduce(lambda a, b: a & b, and_parts, Q())
                q_object = reduce(lambda a, b: a & b, or_group, q_object)
                clauses.append(q_object)
            # Finally, $or the generated clauses in to one query. Each of the
            # clauses is sufficient for the query to succeed.
            return reduce(lambda a, b: a | b, clauses, Q())

        if combination.operation == combination.OR:
            children = []
            # Crush any nested ORs in to this combination as MongoDB doesn't
            # support nested $or operations
            for node in combination.children:
                if (isinstance(node, QCombination) and
                    node.operation == combination.OR):
                    children += node.children
                else:
                    children.append(node)
            combination.children = children

        return combination


class QueryCompilerVisitor(QNodeVisitor):
    """Compiles the nodes in a query tree to a PyMongo-compatible query
    dictionary.
    """

    def __init__(self, document):
        self.document = document

    def visit_combination(self, combination):
        if combination.operation == combination.OR:
            return {'$or': combination.children}
        elif combination.operation == combination.AND:
            return self._mongo_query_conjunction(combination.children)
        return combination

    def visit_query(self, query):
        return transform.query(self.document, **query.query)

    def _mongo_query_conjunction(self, queries):
        """Merges Mongo query dicts - effectively &ing them together.
        """
        combined_query = {}
        for query in queries:
            for field, ops in query.items():
                if field not in combined_query:
                    combined_query[field] = ops
                else:
                    # The field is already present in the query the only way
                    # we can merge is if both the existing value and the new
                    # value are operation dicts, reject anything else
                    if (not isinstance(combined_query[field], dict) or
                        not isinstance(ops, dict)):
                        message = 'Conflicting values for ' + field
                        raise InvalidQueryError(message)

                    current_ops = set(combined_query[field].keys())
                    new_ops = set(ops.keys())
                    # Make sure that the same operation isn't applied more than
                    # once to a single field
                    intersection = current_ops.intersection(new_ops)
                    if intersection:
                        msg = 'Duplicate query conditions: '
                        raise InvalidQueryError(msg + ', '.join(intersection))

                    # Right! We've got two non-overlapping dicts of operations!
                    combined_query[field].update(copy.deepcopy(ops))
        return combined_query


class QNode(object):
    """Base class for nodes in query trees.
    """

    AND = 0
    OR = 1

    def to_query(self, document):
        query = self.accept(SimplificationVisitor())
        query = query.accept(QueryTreeTransformerVisitor())
        query = query.accept(QueryCompilerVisitor(document))
        return query

    def accept(self, visitor):
        raise NotImplementedError

    def _combine(self, other, operation):
        """Combine this node with another node into a QCombination object.
        """
        if getattr(other, 'empty', True):
            return self

        if self.empty:
            return other

        return QCombination(operation, [self, other])

    @property
    def empty(self):
        return False

    def __or__(self, other):
        return self._combine(other, self.OR)

    def __and__(self, other):
        return self._combine(other, self.AND)


class QCombination(QNode):
    """Represents the combination of several conditions by a given logical
    operator.
    """

    def __init__(self, operation, children):
        self.operation = operation
        self.children = []
        for node in children:
            # If the child is a combination of the same type, we can merge its
            # children directly into this combinations children
            if isinstance(node, QCombination) and node.operation == operation:
                self.children += node.children
            else:
                self.children.append(node)

    def accept(self, visitor):
        for i in range(len(self.children)):
            if isinstance(self.children[i], QNode):
                self.children[i] = self.children[i].accept(visitor)

        return visitor.visit_combination(self)

    @property
    def empty(self):
        return not bool(self.children)


class Q(QNode):
    """A simple query object, used in a query tree to build up more complex
    query structures.
    """

    def __init__(self, **query):
        self.query = query

    def accept(self, visitor):
        return visitor.visit_query(self)

    @property
    def empty(self):
        return not bool(self.query)
