from collections import defaultdict
import json
import pandas as pd

class PatternChunk(object):
    def __init__(self, items):
        self._items = items
    def __add__(left, right):
        return PatternChunk(left._items + right._items)
    def __repr__(self):
        return " + ".join(repr(i) for i in self._items)
    def __iter__(self):
        return iter(self._items)

class Index(PatternChunk):
    def __init__(self, name):
        PatternChunk.__init__(self, [self])
        self._name = name
    def __repr__(self):
        return "Index(%s)" % repr(self._name)

class Glob(PatternChunk):
    def __init__(self):
        PatternChunk.__init__(self, [self])
    def __repr__(self):
        return "Glob()"

class Columns(PatternChunk):
    def __init__(self, *column_names):
        PatternChunk.__init__(self, [self])
        self._column_names = tuple(column_names)
    def __repr__(self):
        return "Columns(%s)" % ",".join(repr(c) for c in self._column_names)

class Literal(PatternChunk):
    def __init__(self, key):
        PatternChunk.__init__(self, [self])
        self._key = key
    def __repr__(self):
        return "Literal(%s)" % repr(self._key)

class Join(object):
    def __init__(self, *chunks):
        self._chunks = tuple(chunks)
    def __repr__(self):
        return "Join(%s)" % ",".join(repr(c) for c in self._chunks)
    def __iter__(self):
        return iter(self._chunks)


def itemize(data):
    if hasattr(data, 'items'):
        return data.items()
    elif isinstance(data, (tuple, list)):
        return enumerate(data)
    else:
        return []

class Transformer(object):
    def __init__(self, pattern_definition, column_name=None):
        if column_name is not None:
            if isinstance(column_name, str):
                self._column_name = column_name
            else:
                self._column_name = tuple(column_name)
        else:
            self._column_name = None

        self._parse_pattern(pattern_definition)
        self._data_matrix=defaultdict(dict)

    def _parse_pattern(self, pattern_definition):
        parsed_chunks = []
        index_columns = set()

        for chunk in pattern_definition.split(" "):
            dot, rest = chunk[0], chunk[1:]
            if dot != '.':
                raise RuntimeError("Invalid pattern chunk: <%s>" % chunk)

            parsed_chunk = PatternChunk([])
            if rest:
                for part in rest.split('.'):
                    if part == '*':
                        parsed_chunk += Glob()
                    elif part[0] == '<' and part[-1] == '>':
                        parsed_chunk += Index(part[1:-1])
                    elif part[0] == '{' and part[-1] == '}':
                        column_names = part[1:-1].split(',')
                        parsed_chunk += Columns(*column_names)
                    elif part[0] == '[' and part[-1] == ']':
                        try:
                            integer_value = int(part[1:-1])
                        except TypeError:
                            raise RuntimeError("Invalid pattern chunk: %s is not an integer" % part)
                        parsed_chunk += Literal(integer_value)
                    else:
                        parsed_chunk += Literal(part)
            index_columns.add(tuple(item._name for item in parsed_chunk if isinstance(item, Index)))
            parsed_chunks.append(parsed_chunk)

        self._parsed_chunks = Join(*parsed_chunks)
        self._index_column_names = index_columns.pop()
        if(index_columns):
            raise RuntimeError("Invalid pattern: the different pattern chunks have different index columns")

    def add_json(self, json_string):
        self.add_data(json.loads(json_string))

    def add_data(self, data):
        column_names = self._column_name
        if column_names is None:
            column_names= ""
        if isinstance(column_names, str):
            column_names = [column_names for _ in self._parsed_chunks]
        column_names = list(column_names)
        for chunk in self._parsed_chunks:
            if any(isinstance(item, Columns) or isinstance(item, Glob) for item in chunk):
                self._recurse_pattern(data, chunk, [], [])
            elif column_names:
                self._recurse_pattern(data, chunk, [], [], column_names.pop(0))
            else:
                self._recurse_pattern(data, chunk, [], [], None)

    def _recurse_pattern(self, data, chunk, column_name_prefix, index_prefix, default_column_name=None):
        if(chunk._items):
            # would be nicer to make the 'PatternChunk' object support indexing.
            # Easiest is subclassing collections.UserList, but that doesn't exist
            # in python 2.
            cur_item, next_chunk = chunk._items[0], PatternChunk(chunk._items[1:])
            try:
                if isinstance(cur_item, Glob):
                    for k, v in itemize(data):
                        self._recurse_pattern(v, next_chunk, column_name_prefix + [k], index_prefix, default_column_name)
                elif isinstance(cur_item, Columns):
                    columns = cur_item._column_names
                    if isinstance(data, (dict, list, tuple)):
                        for column in columns:
                            try:
                                self._recurse_pattern(data[column], next_chunk, column_name_prefix + [column], index_prefix, default_column_name)
                            except (IndexError, KeyError):
                                pass
                elif isinstance(cur_item, Index):
                    index_name = cur_item._name
                    for k, v in itemize(data):
                        self._recurse_pattern(v, next_chunk, column_name_prefix, index_prefix + [k], default_column_name)
                elif isinstance(cur_item, Literal):
                    key_name = cur_item._key
                    if isinstance(data, (dict, list, tuple)):
                        try:
                            self._recurse_pattern(data[key_name], next_chunk, column_name_prefix, index_prefix, default_column_name)
                        except (IndexError, KeyError):
                            pass
                else:
                    raise AssertionError("Unknown chunk type")
            except Exception as ex:
                msg = "While applying pattern chunk %s: %s" % (cur_item, ex.args[0])
                ex.args = (msg,) + ex.args[1:]
                raise
        else:
            column_tuple = column_name_prefix if column_name_prefix else [default_column_name]
            self._data_matrix[tuple(index_prefix)][tuple(column_tuple)] = data

    def dataframe(self):
        data_matrix = self._data_matrix
        columns = sorted(set(col for row in data_matrix.values() for col in row.keys()))
        records = [
            ix + tuple( column_data.get(col, None) for col in columns )
            for ix, column_data in sorted(data_matrix.items())
        ]
        index_column_names = self._index_column_names
        data_column_names = tuple("_".join(str(c) for c in col) for col in columns)
        all_column_names = index_column_names + data_column_names

        kwds = {}
        if index_column_names:
            kwds['index'] = index_column_names
        if all_column_names:
            kwds['columns'] = all_column_names
        return pd.DataFrame.from_records(records, **kwds)

def to_dataframe(pattern, data, column_name=None):
    t =  Transformer(pattern, column_name)
    t.add_data(data)
    return t.dataframe()
