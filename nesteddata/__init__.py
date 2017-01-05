from collections import defaultdict
import json
#import pandas as pd

def itemize(data):
    if hasattr(data, 'items'):
        return data.items()
    else:
        return enumerate(data)

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

            parsed_chunk = []
            for part in rest.split('.'):
                if part == '*':
                    parsed_chunk.append(('glob',))
                elif part[0] == '<' and part[-1] == '>':
                    parsed_chunk.append(('index', part[1:-1]))
                elif part[0] == '{' and part[-1] == '}':
                    column_names = part[1:-1].split(',')
                    parsed_chunk.append(('columns', column_names))
                else:
                    parsed_chunk.append(('literal_key', part))
            index_columns.add(tuple(item[1] for item in parsed_chunk if item[0] == 'index'))
            parsed_chunks.append(parsed_chunk)

        self._parsed_chunks = parsed_chunks
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
        for column_name, chunk in zip(column_names, self._parsed_chunks):
            self._recurse_pattern(data, chunk, [], [], column_name)

    def _recurse_pattern(self, data, chunk, column_name_prefix, index_prefix, default_column_name):
        if(chunk):
            cur_item, next_chunk = chunk[0], chunk[1:]
            try:
                what = cur_item[0]
                if what == 'glob':
                    for k, v in itemize(data):
                        self._recurse_pattern(v, next_chunk, column_name_prefix + [k], index_prefix, default_column_name)
                elif what == 'columns':
                    columns == cur_item[1:]
                    for column in columns:
                        if column in data:
                            self._recurse_pattern(data[column], next_chunk, column_name_prefix + [k], index_prefix, default_column_name)
                elif what == 'index':
                    index_name = cur_item[1]
                    for k, v in itemize(data):
                        self._recurse_pattern(v, next_chunk, column_name_prefix, index_prefix + [k], default_column_name)
                elif what == 'literal_key':
                    key_name = cur_item[1]
                    self._recurse_pattern(data[key_name], next_chunk, column_name_prefix, index_prefix, default_column_name)
            except Exception as ex:
                msg = "While applying pattern chunk %s: %s" % (cur_item, ex.args[0])
                ex.args = (msg,) + ex.args[1:]
                raise
        else:
            column_tuple = column_name_prefix if column_name_prefix else [default_column_name]
            self._data_matrix[tuple(index_prefix)][tuple(column_name_prefix)] = data

def to_dataframe(pattern, data, column_name=None):
    t =  Transformer(pattern, column_name)
    t.add_data(data)
    print(t._data_matrix)