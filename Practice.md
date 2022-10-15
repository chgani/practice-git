```python
from pyspark.sql import SparkSession
```


```python
spark=SparkSession.builder.appName("DataframeExample").getOrCreate()
```


```python
spark
```





    <div>
        <p><b>SparkSession - in-memory</b></p>

<div>
    <p><b>SparkContext</b></p>

    <p><a href="http://Ganesh:4040">Spark UI</a></p>

    <dl>
      <dt>Version</dt>
        <dd><code>v3.3.0</code></dd>
      <dt>Master</dt>
        <dd><code>local[*]</code></dd>
      <dt>AppName</dt>
        <dd><code>DataframeExample</code></dd>
    </dl>
</div>

    </div>





```python
df_pyspark=spark.read.\
option('header','true').option('inferSchema',True).\
csv('C:/Users/ganes/OneDrive/Documents/pyspark/sample.csv')
```


```python
df_pyspark.printSchema()
```

    root
     |-- Name: string (nullable = true)
     |-- Age: integer (nullable = true)
     |-- Experience: integer (nullable = true)
    
    


```python
df_spark=spark.read.csv('C:/Users/ganes/OneDrive/Documents/pyspark/sample.csv' \
                        ,header=True, inferSchema=True)
```


```python
df_pyspark.printSchema()
```

    root
     |-- Name: string (nullable = true)
     |-- Age: integer (nullable = true)
     |-- Experience: integer (nullable = true)
    
    


```python
df_spark.show()
```

    +---------+---+----------+
    |     Name|Age|Experience|
    +---------+---+----------+
    |    Krish| 31|        10|
    |Sudhanshu| 30|         8|
    |    Sunny| 29|         4|
    +---------+---+----------+
    
    


```python
type(df_spark)
```




    pyspark.sql.dataframe.DataFrame




```python
df_spark.columns
```




    ['Name', 'Age', 'Experience']




```python
df_spark.head(3)
```




    [Row(Name='Krish', Age=31, Experience=10),
     Row(Name='Sudhanshu', Age=30, Experience=8),
     Row(Name='Sunny', Age=29, Experience=4)]




```python
df_spark.select('Name').show()
```

    +---------+
    |     Name|
    +---------+
    |    Krish|
    |Sudhanshu|
    |    Sunny|
    +---------+
    
    


```python
df_spark.select(['Name','Age']).show()
```

    +---------+---+
    |     Name|Age|
    +---------+---+
    |    Krish| 31|
    |Sudhanshu| 30|
    |    Sunny| 29|
    +---------+---+
    
    


```python
df_pyspark['Name']
```




    Column<'Name'>




```python
df_spark.dtypes
```




    [('Name', 'string'), ('Age', 'int'), ('Experience', 'int')]



df_spark.describe().show()


```python
df_pyspark.describe().show()
```

    +-------+-----+----+-----------------+
    |summary| Name| Age|       Experience|
    +-------+-----+----+-----------------+
    |  count|    3|   3|                3|
    |   mean| null|30.0|7.333333333333333|
    | stddev| null| 1.0|3.055050463303893|
    |    min|Krish|  29|                4|
    |    max|Sunny|  31|               10|
    +-------+-----+----+-----------------+
    
    


```python
###Adding column
```


```python
sc=df_pyspark.withColumn('Experience after 2 years',df_pyspark['Experience']+2)
```


```python
sc.show()
```

    +---------+---+----------+------------------------+
    |     Name|Age|Experience|Experience after 2 years|
    +---------+---+----------+------------------------+
    |    Krish| 31|        10|                      12|
    |Sudhanshu| 30|         8|                      10|
    |    Sunny| 29|         4|                       6|
    +---------+---+----------+------------------------+
    
    


```python
###Drop the column

sc1=sc.drop('Experience after 2 years')
sc1.show()
```

    +---------+---+----------+
    |     Name|Age|Experience|
    +---------+---+----------+
    |    Krish| 31|        10|
    |Sudhanshu| 30|         8|
    |    Sunny| 29|         4|
    +---------+---+----------+
    
    


```python
##reanming column

sc1.withColumnRenamed('Name','New Name').show()
```

    +---------+---+----------+
    | New Name|Age|Experience|
    +---------+---+----------+
    |    Krish| 31|        10|
    |Sudhanshu| 30|         8|
    |    Sunny| 29|         4|
    +---------+---+----------+
    
    


```python
df1=spark.read.format("csv").option("header",True)\
.option("inferSchema",True).option("path","sample1.csv").load()
```


```python
df1.show()
```

    +-------+----+----------+------+
    |   Name| Age|Experience|Salary|
    +-------+----+----------+------+
    | Ganesh|  28|         3| 50000|
    |shankar|  31|         2| 30000|
    |   Ramu|  26|         8| 50000|
    |   Babu|  25|         9| 35000|
    | Dinesh|  24|         1| 20000|
    | Sekhar|  28|        10| 10000|
    |  Pandu|  38|         6| 60000|
    |   Nagu|  40|         9| 70000|
    |    Sai|  41|         8| 36000|
    | Chandi|null|      null| 40000|
    |   null|null|         2|  null|
    |   null|  34|         3| 38000|
    |   null|  16|      null|  null|
    +-------+----+----------+------+
    
    


```python
df1.na.drop().show()
```

    +-------+---+----------+------+
    |   Name|Age|Experience|Salary|
    +-------+---+----------+------+
    | Ganesh| 28|         3| 50000|
    |shankar| 31|         2| 30000|
    |   Ramu| 26|         8| 50000|
    |   Babu| 25|         9| 35000|
    | Dinesh| 24|         1| 20000|
    | Sekhar| 28|        10| 10000|
    |  Pandu| 38|         6| 60000|
    |   Nagu| 40|         9| 70000|
    |    Sai| 41|         8| 36000|
    +-------+---+----------+------+
    
    


```python
df1.na.drop(how='any').show()
```

    +-------+---+----------+------+
    |   Name|Age|Experience|Salary|
    +-------+---+----------+------+
    | Ganesh| 28|         3| 50000|
    |shankar| 31|         2| 30000|
    |   Ramu| 26|         8| 50000|
    |   Babu| 25|         9| 35000|
    | Dinesh| 24|         1| 20000|
    | Sekhar| 28|        10| 10000|
    |  Pandu| 38|         6| 60000|
    |   Nagu| 40|         9| 70000|
    |    Sai| 41|         8| 36000|
    +-------+---+----------+------+
    
    


```python
df1.na.drop(how='all').show()
```

    +-------+----+----------+------+
    |   Name| Age|Experience|Salary|
    +-------+----+----------+------+
    | Ganesh|  28|         3| 50000|
    |shankar|  31|         2| 30000|
    |   Ramu|  26|         8| 50000|
    |   Babu|  25|         9| 35000|
    | Dinesh|  24|         1| 20000|
    | Sekhar|  28|        10| 10000|
    |  Pandu|  38|         6| 60000|
    |   Nagu|  40|         9| 70000|
    |    Sai|  41|         8| 36000|
    | Chandi|null|      null| 40000|
    |   null|null|         2|  null|
    |   null|  34|         3| 38000|
    |   null|  16|      null|  null|
    +-------+----+----------+------+
    
    

Signature:
df1.na.drop(
    how: str = 'any',
    thresh: Optional[int] = None,
    subset: Union[str, Tuple[str, ...], List[str], NoneType] = None,
) -> pyspark.sql.dataframe.DataFrame
Docstring:
Returns a new :class:`DataFrame` omitting rows with null values.
:func:`DataFrame.dropna` and :func:`DataFrameNaFunctions.drop` are aliases of each other.

.. versionadded:: 1.3.1

Parameters
----------
how : str, optional
    'any' or 'all'.
    If 'any', drop a row if it contains any nulls.
    If 'all', drop a row only if all its values are null.
thresh: int, optional
    default None
    If specified, drop rows that have less than `thresh` non-null values.
    This overwrites the `how` parameter.
subset : str, tuple or list, optional
    optional list of column names to consider.

Examples
--------
>>> df4.na.drop().show()
+---+------+-----+
|age|height| name|
+---+------+-----+
| 10|    80|Alice|
+---+------+-----+
File:      c:\users\ganes\anaconda3\lib\site-packages\pyspark\sql\dataframe.py
Type:      method


```python
df1.na.drop(how='any',thresh=2).show()\
#removes rows with lessthan 2 non null values
```

    +-------+----+----------+------+
    |   Name| Age|Experience|Salary|
    +-------+----+----------+------+
    | Ganesh|  28|         3| 50000|
    |shankar|  31|         2| 30000|
    |   Ramu|  26|         8| 50000|
    |   Babu|  25|         9| 35000|
    | Dinesh|  24|         1| 20000|
    | Sekhar|  28|        10| 10000|
    |  Pandu|  38|         6| 60000|
    |   Nagu|  40|         9| 70000|
    |    Sai|  41|         8| 36000|
    | Chandi|null|      null| 40000|
    |   null|  34|         3| 38000|
    +-------+----+----------+------+
    
    


```python
df1.na.drop(how='any',subset=['Experience']).show()
#if experience column having nulls it will remove the row
```

    +-------+----+----------+------+
    |   Name| Age|Experience|Salary|
    +-------+----+----------+------+
    | Ganesh|  28|         3| 50000|
    |shankar|  31|         2| 30000|
    |   Ramu|  26|         8| 50000|
    |   Babu|  25|         9| 35000|
    | Dinesh|  24|         1| 20000|
    | Sekhar|  28|        10| 10000|
    |  Pandu|  38|         6| 60000|
    |   Nagu|  40|         9| 70000|
    |    Sai|  41|         8| 36000|
    |   null|null|         2|  null|
    |   null|  34|         3| 38000|
    +-------+----+----------+------+
    
    

Signature:
df1.na.fill(
    value: Union[ForwardRef('LiteralType'), Dict[str, ForwardRef('LiteralType')]],
    subset: Optional[List[str]] = None,
) -> pyspark.sql.dataframe.DataFrame
Docstring:
Replace null values, alias for ``na.fill()``.
:func:`DataFrame.fillna` and :func:`DataFrameNaFunctions.fill` are aliases of each other.

.. versionadded:: 1.3.1

Parameters
----------
value : int, float, string, bool or dict
    Value to replace null values with.
    If the value is a dict, then `subset` is ignored and `value` must be a mapping
    from column name (string) to replacement value. The replacement value must be
    an int, float, boolean, or string.
subset : str, tuple or list, optional
    optional list of column names to consider.
    Columns specified in subset that do not have matching data type are ignored.
    For example, if `value` is a string, and subset contains a non-string column,
    then the non-string column is simply ignored.

Examples
--------
>>> df4.na.fill(50).show()
+---+------+-----+
|age|height| name|
+---+------+-----+
| 10|    80|Alice|
|  5|    50|  Bob|
| 50|    50|  Tom|
| 50|    50| null|
+---+------+-----+

>>> df5.na.fill(False).show()
+----+-------+-----+
| age|   name|  spy|
+----+-------+-----+
|  10|  Alice|false|
|   5|    Bob|false|
|null|Mallory| true|
+----+-------+-----+

>>> df4.na.fill({'age': 50, 'name': 'unknown'}).show()
+---+------+-------+
|age|height|   name|
+---+------+-------+
| 10|    80|  Alice|
|  5|  null|    Bob|
| 50|  null|    Tom|
| 50|  null|unknown|
+---+------+-------+
File:      c:\users\ganes\anaconda3\lib\site-packages\pyspark\sql\dataframe.py
Type:      method


```python
#filling missing values

```


```python
df1.na.fill(value='missing value').show()
```

    +-------------+----+----------+------+
    |         Name| Age|Experience|Salary|
    +-------------+----+----------+------+
    |       Ganesh|  28|         3| 50000|
    |      shankar|  31|         2| 30000|
    |         Ramu|  26|         8| 50000|
    |         Babu|  25|         9| 35000|
    |       Dinesh|  24|         1| 20000|
    |       Sekhar|  28|        10| 10000|
    |        Pandu|  38|         6| 60000|
    |         Nagu|  40|         9| 70000|
    |          Sai|  41|         8| 36000|
    |       Chandi|null|      null| 40000|
    |missing value|null|         2|  null|
    |missing value|  34|         3| 38000|
    |missing value|  16|      null|  null|
    +-------------+----+----------+------+
    
    


```python
df1.na.fill(0,['Age','Experience']).show()
```

    +-------+---+----------+------+
    |   Name|Age|Experience|Salary|
    +-------+---+----------+------+
    | Ganesh| 28|         3| 50000|
    |shankar| 31|         2| 30000|
    |   Ramu| 26|         8| 50000|
    |   Babu| 25|         9| 35000|
    | Dinesh| 24|         1| 20000|
    | Sekhar| 28|        10| 10000|
    |  Pandu| 38|         6| 60000|
    |   Nagu| 40|         9| 70000|
    |    Sai| 41|         8| 36000|
    | Chandi|  0|         0| 40000|
    |   null|  0|         2|  null|
    |   null| 34|         3| 38000|
    |   null| 16|         0|  null|
    +-------+---+----------+------+
    
    


```python
df1.show()
```

    +-------+----+----------+------+
    |   Name| Age|Experience|Salary|
    +-------+----+----------+------+
    | Ganesh|  28|         3| 50000|
    |shankar|  31|         2| 30000|
    |   Ramu|  26|         8| 50000|
    |   Babu|  25|         9| 35000|
    | Dinesh|  24|         1| 20000|
    | Sekhar|  28|        10| 10000|
    |  Pandu|  38|         6| 60000|
    |   Nagu|  40|         9| 70000|
    |    Sai|  41|         8| 36000|
    | Chandi|null|      null| 40000|
    |   null|null|         2|  null|
    |   null|  34|         3| 38000|
    |   null|  16|      null|  null|
    +-------+----+----------+------+
    
    


```python
from pyspark.ml.feature import Imputer

imputer =Imputer(
    inputCols=['Age','Experience','Salary'],
    outputCols=["{}_imputed".format(c)\
                for c in ['Age','Experience','Salary']]
).setStrategy("mean")
```


```python

df2=imputer.fit(df1).transform(df1)
```

##filter


```python
df3=df2.na.drop(how='any')
```


```python
#drop multiple columns
df4=df3.drop(*['Age','Experience','Salary'])
```


```python
##rename multiple columns
df5=df4.withColumnRenamed('Age_imputed','Age').\
withColumnRenamed('Experience_imputed','Experience').\
withColumnRenamed('Salary_imputed','Salary')
```


```python
df5.show()
```

    +-------+---+----------+------+
    |   Name|Age|Experience|Salary|
    +-------+---+----------+------+
    | Ganesh| 28|         3| 50000|
    |shankar| 31|         2| 30000|
    |   Ramu| 26|         8| 50000|
    |   Babu| 25|         9| 35000|
    | Dinesh| 24|         1| 20000|
    | Sekhar| 28|        10| 10000|
    |  Pandu| 38|         6| 60000|
    |   Nagu| 40|         9| 70000|
    |    Sai| 41|         8| 36000|
    +-------+---+----------+------+
    
    


```python
df5.filter("Salary>50000").show()
```

    +-----+---+----------+------+
    | Name|Age|Experience|Salary|
    +-----+---+----------+------+
    |Pandu| 38|         6| 60000|
    | Nagu| 40|         9| 70000|
    +-----+---+----------+------+
    
    


```python
df5.filter("Salary>50000").select(['Name','Age']).show()
```

    +-----+---+
    | Name|Age|
    +-----+---+
    |Pandu| 38|
    | Nagu| 40|
    +-----+---+
    
    


```python
df5.filter("Salary>30000 and age <30").select(['Name','Age']).show()
```

    +------+---+
    |  Name|Age|
    +------+---+
    |Ganesh| 28|
    |  Ramu| 26|
    |  Babu| 25|
    +------+---+