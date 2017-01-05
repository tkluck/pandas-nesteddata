Let's define a few nested structures:

    >>> from nesteddata import to_dataframe
    >>> value= 42;
    >>> empty_data= {};
    >>> data= dict(
    ...     calculus= dict(
    ...         Newton= dict(
    ...             birth= dict(
    ...                 year= 1642,
    ...                 month= 12,
    ...             ),
    ...             death= dict(
    ...                 year= 1726,
    ...                 month= 3,
    ...             ),
    ...             nationality= "English",
    ...             apple= "yes",
    ...         ),
    ...         Leibniz= dict(
    ...             birth= dict(
    ...                 year= 1646,
    ...                 month= 6,
    ...             ),
    ...             death= dict(
    ...                 year= 1716,
    ...                 month= 11,
    ...             ),
    ...             nationality= "German",
    ...             apple= "no",
    ...         ),
    ...     ),
    ... )

And test a few patterns:

    >>> to_dataframe('.calculus.<name>.*.year', data=data)
             birth  death
    name                 
    Leibniz   1646   1716
    Newton    1642   1726

    >>> to_dataframe('.calculus.*.<event>.year', data=data)
           Leibniz  Newton
    event                 
    birth     1646    1642
    death     1716    1726

    >>> to_dataframe('.calculus.<name>.*.*', data=data)
             birth_month  birth_year  death_month  death_year
    name                                                     
    Leibniz            6        1646           11        1716
    Newton            12        1642            3        1726

    >>> to_dataframe('.', data=value)
    <BLANKLINE>
    0  42

    >>> to_dataframe('.', column_name="value", data=value)
       value
    0     42

    >>> to_dataframe('.calculus.Newton.birth.year', data=data)
    <BLANKLINE>
    0  1642

    >>> to_dataframe('.', data=empty_data)
    <BLANKLINE>
    0  {}

    >>> to_dataframe('.nonexistent_key', data=empty_data)
    Empty DataFrame
    Columns: []
    Index: []

    >>> to_dataframe('.nonexistent_key', column_name="value", data=empty_data)
    Empty DataFrame
    Columns: []
    Index: []

    >>> to_dataframe('.nonexistent_key.nested', data=empty_data)
    Empty DataFrame
    Columns: []
    Index: []

    >>> to_dataframe('.calculus.Newton.<event>.<what>', column_name="value", data=data)
                 value
    event what        
    birth month     12
          year    1642
    death month      3
          year    1726

    >>> to_dataframe('.calculus.Newton.<event>.year', column_name="year", data=data)
           year
    event      
    birth  1642
    death  1726

    >>> to_dataframe('.calculus.Newton.<event>.{year}', data=data)
           year
    event      
    birth  1642
    death  1726

    >>> to_dataframe('.calculus.{Newton,Leibniz}.birth.year', data=data)
       Leibniz  Newton
    0     1646    1642

    >>> to_dataframe('.calculus.<who>.birth.{year} .calculus.<who>.birth.{month}', data=data)
             month  year
    who                 
    Leibniz      6  1646
    Newton      12  1642

    >>> to_dataframe(
    ...    '.calculus.<who>.*.year .calculus.<who>.nationality .calculus.<who>.apple',
    ...    column_name=['nationality', 'apple'],
    ...    data=data
    ... )
            apple  birth  death nationality
    who                                    
    Leibniz    no   1646   1716      German
    Newton    yes   1642   1726     English

    >>> to_dataframe('.<index>', column_name="value", data=list(range(1,6)))
           value
    index       
    0          1
    1          2
    2          3
    3          4
    4          5

    >>> to_dataframe('.string_with_newlines', column_name="value", data=dict(string_with_newlines="Line 1\nLine 2"))
                value
    0  Line 1\nLine 2

    >>> to_dataframe('.string_with_quotes', column_name="value", data=dict(string_with_quotes='"Run!" he said'))
                value
    0  "Run!" he said

    # false data
    >>> to_dataframe('.false_data', column_name="value", data=dict(false_data=0))
       value
    0      0

    >>> jagged_data= {
    ...     1: dict( b= 3, c= 4, d= 5 ),
    ...     2: dict( b= 6, d= 7 )
    ... };

    >>> to_dataframe('.<a>.*', data=jagged_data)
       b   c  d
    a          
    1  3   4  5
    2  6 NaN  7

