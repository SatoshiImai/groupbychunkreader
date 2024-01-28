# groupbychunkreader

A group by chunk reading wrapper for Pandas DataFrame.

## GroupByChunkReader

An iterator wrapper for Pandas DataFrame based on a control-break algorithm.
It expects chunked pandas dataframe which is returned from `read_csv` or `read_sql` with `chunksize` parameter.
It supports only one key column. So you need merge multiple keys into one key column.
The key column has to be sorted BEFORE pandas read. It is expected to be sorted by SQL level or pre-process for csv.
It pools at least 2 keys' dataframe. So you need to manage break key size to be able to load at once 2 keys into a memory. And you need to manage pandas read `chunksize` to be able to load into a memory, too.

```python
import pandas as pd
from groupbychunkreader import GroupByChunkReader

type_def = {'value': int, 'key': 'object'}

# test data
test_csv_df = pd.DataFrame([[x, '01'] for x in range(10)] + 
                           [[x, '02'] for x in range(20)], 
                           columns=list(type_def))
test_csv_df.to_csv('...data path')

# load by the grouped chunk
test_csv_df = pd.read_csv(
    '...data path',
    dtype=type_def, chunksize=6)

my_reader = GroupByChunkReader(test_csv_df, 'key')
count = 0
for this_df in my_reader:
    # do operation by group
```

## LICENSE

I inherited BSD 3-Clause License from [pandas](https://pypi.org/project/pandas/)
