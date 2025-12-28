from __future__ import annotations

import re
from typing import Any, Dict, Tuple, Optional


class QueryNormalizer:
    """
    - Converts python regex objects into mongo $regex format.
    - Converts $where into a $function expression (returned separately).
    """

    def normalize(self, query: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        query = self._walk_and_convert_regex(query)
        return self._convert_where_to_function(query)

    @staticmethod
    def _convert_where_to_function(query: Dict[str, Any]):
        if "$where" not in query:
            return query, None

        raw_js = query["$where"].strip()
        m = re.match(r"function\s*\(\s*\)\s*\{(.*)\}", raw_js, re.S)
        inner = m.group(1).strip() if m else raw_js
        inner = re.sub(r"\bthis\b", "doc", inner)

        cleaned = {k: v for k, v in query.items() if k != "$where"}
        function_expr = {
            "$expr": {
                "$function": {
                    "body": f"function(doc) {{ {inner} }}",
                    "args": ["$$ROOT"],
                    "lang": "js",
                }
            }
        }
        return cleaned, function_expr

    @staticmethod
    def _convert_regex(value: Any):
        if isinstance(value, re.Pattern):
            pattern = value.pattern
            opts = ""
            if value.flags & re.IGNORECASE:
                opts += "i"
            if value.flags & re.MULTILINE:
                opts += "m"
            if value.flags & re.DOTALL:
                opts += "s"
            return {"$regex": pattern, "$options": opts} if opts else {"$regex": pattern}
        return value

    def _walk_and_convert_regex(self, obj: Any):
        if isinstance(obj, dict):
            return {k: self._walk_and_convert_regex(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._walk_and_convert_regex(v) for v in obj]
        return self._convert_regex(obj)
