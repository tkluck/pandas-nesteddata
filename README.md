pandas-nesteddata version 0.1
=============================

This module transforms hierarchical data (nested arrays/hashes) to
a pandas DataFrame according to a compact, readable, user-specified pattern.

For example, the pattern `.<index>.*` transforms a data structure
of the form

    >>> data = [{ 'a': 1, 'b': 2 }, { 'a': 3, 'b': 4 }]

to the DataFrame

           a  b
    index
    0      1  2
    1      3  4

Or, in code:

    >>> from nesteddata import to_dataframe
    >>> to_dataframe('.<index>.*', data)
           a  b
    index      
    0      1  2
    1      3  4

The pattern `.*.*` applied to the same data gives the output

    >>> to_dataframe('.*.*', data)
       0_a  0_b  1_a  1_b
    0    1    2    3    4

The pattern `.*.<key>` gives the output

    >>> to_dataframe('.*.<key>', data)
         0  1
    key      
    a    1  3
    b    2  4

It is hoped that the pattern specification is sufficiently powerful for this
module to replace a lot of simple boiler-plate data transformations.

PATTERN SPECIFICATION
---------------------

The dot-separated components represent the following:

- `<name>` represents that the keys at that position should be put in a column
  named name in the csv output. The values belonging to those keys become rows;
- `*` represents that the keys at that position in the pattern should be
  interpreted as column names; their values should be the values for that
  column, all beloning to the same row;
- `{column_name}` or `{column_name_1,column_name_2,...}` is similar to `*`, but
  instead of capturing all the keys at that level of the hierarchy, it only
  captures the named columns.
- `[<number>]` represents a numerical literal key, for indexing arrays or
  dictionaries with keys of type `int`.
- anything else represents a literal key name.
- If your pattern does not contain `*` or `{...}`, you need to pass an
  additional `column_name=` parameter to `to_dataframe` to specify the name
  for the single column where the value will go.

For the purposes of this description, an array should be seen as a collection
of index => value pairs.

It is possible to specify several dot-separated paths in a single pattern,
separated by spaces. In that case, all the paths need to have the same primary
key (that is, the same set of names in `<...>`). Rows will be formed by joining
the columns resulting from the different paths.

ESCAPING SPECIAL CHARACTERS
---------------------------

The characters `<>{}*[].` have a special meaning and as such, cannot be part
of a literal key. More precisely, if they are in such position that they can
be interpreted with their special meaning, this takes precedence.

Allowing a way to escape these special characters will be part of a future
release. For now, look at 'Building the pattern from data structures' below.

BUILDING THE PATTERN FROM DATA STRUCTURES
-----------------------------------------

As an alternative to passing the pattern as a string that needs to be parsed,
it is also possible to pass the pattern as a data structure. For example, the
pattern

    .*.<key>

can also be represented as

    >>> from nesteddata import Glob, Index
    >>> pattern = Glob() + Index('key')
    >>> pattern
    Glob() + Index('key')
    >>> pattern.to_dataframe(data)
         0  1
    key      
    a    1  3
    b    2  4

The constructor functions are:

- `Index(name)` (correponds to `<name>`)
- `Glob()` (corresponds to `*`)
- `Columns(*column_names)` (corresponds to `{column_name_1,..,column_name_n}`)
- `Literal(key)` (correponds to a literal string key or a `[<number>]` integer key)
- `Join(*chunks)` (corresponds to space-separated pattern chunks)


INSTALLATION
------------

To install this module type the following:

    python setup.py
    sudo python setup.py install

DEPENDENCIES
------------

This module requires these other modules and libraries:

    pandas

COPYRIGHT AND LICENCE
---------------------

Copyright (C) 2017 by Timo Kluck

This library is free software; you can redistribute it and/or modify
it under the terms of the General Public License, version 3 or later.
A copy of this license can be found in LICENSE.


