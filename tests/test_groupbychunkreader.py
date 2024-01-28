# coding:utf-8
# ---------------------------------------------------------------------------
# __author__ = 'Satoshi Imai'
# __credits__ = ['Satoshi Imai']
# __version__ = "0.9.0"
# ---------------------------------------------------------------------------

import logging
import random
import shutil
import tempfile
from logging import Logger, StreamHandler
from pathlib import Path
from typing import Generator

import pandas as pd
import pytest

from src.groupbychunkreader import GroupByChunkReader

test_csv_format = 'test_csv_{0:0>3}.csv'
chunksize = 3
type_def = {'value': int, 'key': 'object'}


@pytest.fixture(scope='session', autouse=True)
def setup_and_teardown():
    # setup

    yield

    # teardown
    # end def


@pytest.fixture(scope='session')
def logger() -> Generator[Logger, None, None]:
    log = logging.getLogger(__name__)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s : %(message)s')
    s_handler = StreamHandler()
    s_handler.setLevel(logging.INFO)
    s_handler.setFormatter(formatter)
    log.addHandler(s_handler)

    yield log
    # end def


@pytest.fixture(scope='session')
def tempdir() -> Generator[Path, None, None]:

    tempdir = Path(tempfile.mkdtemp())
    yield tempdir
    shutil.rmtree(tempdir, ignore_errors=True)
    # end def


@pytest.mark.run(order=10)
def test_key1_patterns(tempdir: Path, logger: Logger):
    logger.info('key1_patterns')

    # key1 chunk<1
    test_csv_01_df = pd.DataFrame([[x, '01']
                                  for x in range(chunksize - 1)], columns=list(type_def))
    test_csv_01_df.to_csv(tempdir.joinpath(test_csv_format.format(1)))

    # key1 chunk=1
    test_csv_02_df = pd.DataFrame([[x, '01']
                                  for x in range(chunksize)], columns=list(type_def))
    test_csv_02_df.to_csv(tempdir.joinpath(test_csv_format.format(2)))

    # key1 chunk>1
    test_csv_03_df = pd.DataFrame([[x, '01']
                                  for x in range(chunksize * 2 - 1)], columns=list(type_def))
    test_csv_03_df.to_csv(tempdir.joinpath(test_csv_format.format(3)))

    # key1 chunk>2
    test_csv_04_df = pd.DataFrame([[x, '01']
                                  for x in range(chunksize * 3 + 1)], columns=list(type_def))
    test_csv_04_df.to_csv(tempdir.joinpath(test_csv_format.format(4)))

    # key1 chunk<1
    test_csv_01_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(1)),
        dtype=type_def, chunksize=chunksize)

    my_reader = GroupByChunkReader(test_csv_01_df, 'key')
    count = 0
    for this_df in my_reader:
        count += 1
        assert len(this_df) == chunksize - 1
        # end for

    assert count == 1
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with

    # key1 chunk=1
    test_csv_02_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(2)),
        dtype=type_def, chunksize=chunksize)

    my_reader = GroupByChunkReader(test_csv_02_df, 'key')
    count = 0
    for this_df in my_reader:
        count += 1
        assert len(this_df) == chunksize
        # end for

    assert count == 1
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with

    # key1 chunk>1
    test_csv_03_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(3)),
        dtype=type_def, chunksize=chunksize)

    my_reader = GroupByChunkReader(test_csv_03_df, 'key')
    count = 0
    for this_df in my_reader:
        count += 1
        assert len(this_df) == chunksize * 2 - 1
        # end for

    assert count == 1
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with

    # key1 chunk>2
    test_csv_04_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(4)),
        dtype=type_def, chunksize=chunksize)

    my_reader = GroupByChunkReader(test_csv_04_df, 'key')
    count = 0
    for this_df in my_reader:
        count += 1
        assert len(this_df) == chunksize * 3 + 1
        # end for

    assert count == 1
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with
    # end def


@pytest.mark.run(order=20)
def test_key2_patterns(tempdir: Path, logger: Logger):
    logger.info('key2_patterns')

    # key2 chunk<1
    test_csv_01_df = pd.DataFrame([[x, '01'] for x in range(
        chunksize - 1)] + [[x, '02'] for x in range(chunksize - 1)], columns=list(type_def))
    test_csv_01_df.to_csv(tempdir.joinpath(test_csv_format.format(11)))

    # key2 chunk=1
    test_csv_02_df = pd.DataFrame([[x, '01'] for x in range(
        chunksize)] + [[x, '02'] for x in range(chunksize)], columns=list(type_def))
    test_csv_02_df.to_csv(tempdir.joinpath(test_csv_format.format(12)))

    # key2 chunk>1
    test_csv_03_df = pd.DataFrame([[x,
                                    '01'] for x in range(chunksize * 2 - 1)] + [[x,
                                                                                 '02'] for x in range(chunksize * 2 - 1)],
                                  columns=list(type_def))
    test_csv_03_df.to_csv(tempdir.joinpath(test_csv_format.format(13)))

    # key2 chunk>2
    test_csv_04_df = pd.DataFrame([[x,
                                    '01'] for x in range(chunksize * 3 + 1)] + [[x,
                                                                                 '02'] for x in range(chunksize * 3 + 1)],
                                  columns=list(type_def))
    test_csv_04_df.to_csv(tempdir.joinpath(test_csv_format.format(14)))

    # key2 chunk<1
    test_csv_01_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(11)),
        dtype=type_def, chunksize=chunksize)

    my_reader = GroupByChunkReader(test_csv_01_df, 'key')
    count = 0
    for this_df in my_reader:
        count += 1
        assert len(this_df) == chunksize - 1
        # end for

    assert count == 2
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with

    # key2 chunk=1
    test_csv_02_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(12)),
        dtype=type_def, chunksize=chunksize)

    my_reader = GroupByChunkReader(test_csv_02_df, 'key')
    count = 0
    for this_df in my_reader:
        count += 1
        assert len(this_df) == chunksize
        # end for

    assert count == 2
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with

    # key2 chunk>1
    test_csv_03_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(13)),
        dtype=type_def, chunksize=chunksize)

    my_reader = GroupByChunkReader(test_csv_03_df, 'key')
    count = 0
    for this_df in my_reader:
        count += 1
        assert len(this_df) == chunksize * 2 - 1
        # end for

    assert count == 2
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with

    # key2 chunk>2
    test_csv_04_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(14)),
        dtype=type_def, chunksize=chunksize)

    my_reader = GroupByChunkReader(test_csv_04_df, 'key')
    count = 0
    for this_df in my_reader:
        count += 1
        assert len(this_df) == chunksize * 3 + 1
        # end for

    assert count == 2
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with

    # key1,2 chunk<1
    test_csv_05_df = pd.DataFrame([[x, '01'] for x in range(
        chunksize - 2)] + [[x, '02'] for x in range(chunksize - 2)], columns=list(type_def))
    test_csv_05_df.to_csv(tempdir.joinpath(test_csv_format.format(15)))

    test_csv_05_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(15)),
        dtype=type_def, chunksize=chunksize)

    my_reader = GroupByChunkReader(test_csv_05_df, 'key')
    count = 0
    for this_df in my_reader:
        count += 1
        assert len(this_df) == chunksize - 2
        # end for

    assert count == 2
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with

    # key1 chunk<1, key2 chunk=1
    test_csv_06_df = pd.DataFrame([[x, '01'] for x in range(
        chunksize - 2)] + [[x, '02'] for x in range(chunksize)], columns=list(type_def))
    test_csv_06_df.to_csv(tempdir.joinpath(test_csv_format.format(16)))

    test_csv_06_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(16)),
        dtype=type_def, chunksize=chunksize)

    my_reader = GroupByChunkReader(test_csv_06_df, 'key')
    count = 0
    expected = {1: chunksize - 2, 2: chunksize}
    for this_df in my_reader:
        count += 1
        assert len(this_df) == expected[count]
        # end for

    assert count == 2
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with
    # end def


@pytest.mark.run(order=30)
def test_key3_patterns(tempdir: Path, logger: Logger):
    logger.info('key2_patterns')

    # key2 chunk<1
    test_csv_01_df = pd.DataFrame([[x, '01'] for x in range(chunksize - 1)] +
                                  [[x, '02'] for x in range(chunksize - 1)] +
                                  [[x, '03'] for x in range(chunksize - 1)],
                                  columns=list(type_def))
    test_csv_01_df.to_csv(tempdir.joinpath(test_csv_format.format(21)))

    # key2 chunk=1
    test_csv_02_df = pd.DataFrame([[x, '01'] for x in range(chunksize)] +
                                  [[x, '02'] for x in range(chunksize)] +
                                  [[x, '03'] for x in range(chunksize)],
                                  columns=list(type_def))
    test_csv_02_df.to_csv(tempdir.joinpath(test_csv_format.format(22)))

    # key2 chunk>1
    test_csv_03_df = pd.DataFrame([[x, '01'] for x in range(chunksize * 2 - 1)] +
                                  [[x, '02'] for x in range(chunksize * 2 - 1)] +
                                  [[x, '03']
                                      for x in range(chunksize * 2 - 1)],
                                  columns=list(type_def))
    test_csv_03_df.to_csv(tempdir.joinpath(test_csv_format.format(23)))

    # key2 chunk>2
    test_csv_04_df = pd.DataFrame([[x, '01'] for x in range(chunksize * 3 + 1)] +
                                  [[x, '02'] for x in range(chunksize * 3 + 1)] +
                                  [[x, '03']
                                      for x in range(chunksize * 3 + 1)],
                                  columns=list(type_def))
    test_csv_04_df.to_csv(tempdir.joinpath(test_csv_format.format(24)))

    # key2 chunk<1
    test_csv_01_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(21)),
        dtype=type_def, chunksize=chunksize)

    my_reader = GroupByChunkReader(test_csv_01_df, 'key')
    count = 0
    for this_df in my_reader:
        count += 1
        assert len(this_df) == chunksize - 1
        # end for

    assert count == 3
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with

    # key2 chunk=1
    test_csv_02_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(22)),
        dtype=type_def, chunksize=chunksize)

    my_reader = GroupByChunkReader(test_csv_02_df, 'key')
    count = 0
    for this_df in my_reader:
        count += 1
        assert len(this_df) == chunksize
        # end for

    assert count == 3
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with

    # key2 chunk>1
    test_csv_03_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(23)),
        dtype=type_def, chunksize=chunksize)

    my_reader = GroupByChunkReader(test_csv_03_df, 'key')
    count = 0
    for this_df in my_reader:
        count += 1
        assert len(this_df) == chunksize * 2 - 1
        # end for

    assert count == 3
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with

    # key2 chunk>2
    test_csv_04_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(24)),
        dtype=type_def, chunksize=chunksize)

    my_reader = GroupByChunkReader(test_csv_04_df, 'key')
    count = 0
    for this_df in my_reader:
        count += 1
        assert len(this_df) == chunksize * 3 + 1
        # end for

    assert count == 3
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with

    # key1,2,3 chunk<1
    test_csv_05_df = pd.DataFrame([[x, '01'] for x in range(chunksize - 2)] +
                                  [[x, '02'] for x in range(chunksize - 2)] +
                                  [[x, '03'] for x in range(chunksize - 2)], columns=list(type_def))
    test_csv_05_df.to_csv(tempdir.joinpath(test_csv_format.format(25)))

    test_csv_05_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(25)),
        dtype=type_def, chunksize=4)

    my_reader = GroupByChunkReader(test_csv_05_df, 'key')
    count = 0
    for this_df in my_reader:
        count += 1
        assert len(this_df) == chunksize - 2
        # end for

    assert count == 3
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with

    # key1 chunk<1, key2 chunk=1, key3 chunk>1
    test_csv_06_df = pd.DataFrame([[x, '01'] for x in range(chunksize - 2)] +
                                  [[x, '02'] for x in range(chunksize)] +
                                  [[x, '03']
                                      for x in range(chunksize * 10 + 1)],
                                  columns=list(type_def))
    test_csv_06_df.to_csv(tempdir.joinpath(test_csv_format.format(26)))

    test_csv_06_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(26)),
        dtype=type_def, chunksize=chunksize)

    my_reader = GroupByChunkReader(test_csv_06_df, 'key')
    count = 0
    expected = {1: chunksize - 2, 2: chunksize, 3: chunksize * 10 + 1}
    for this_df in my_reader:
        count += 1
        assert len(this_df) == expected[count]
        # end for

    assert count == 3
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with

    # key1 chunk<1, key2 chunk<1, key3 chunk<1
    test_csv_07_df = pd.DataFrame([[x, '01'] for x in range(chunksize - 2)] +
                                  [[x, '02'] for x in range(chunksize - 2)] +
                                  [[x, '03'] for x in range(chunksize - 2)],
                                  columns=list(type_def))
    test_csv_07_df.to_csv(tempdir.joinpath(test_csv_format.format(27)))

    test_csv_07_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(27)),
        dtype=type_def, chunksize=10)

    my_reader = GroupByChunkReader(test_csv_07_df, 'key')
    count = 0
    expected = {1: chunksize - 2, 2: chunksize - 2, 3: chunksize - 2}
    for this_df in my_reader:
        count += 1
        assert len(this_df) == expected[count]
        # end for

    assert count == 3
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with
    # end def


@pytest.mark.run(order=40)
def test_random_patterns(tempdir: Path, logger: Logger):
    logger.info('random_patterns')

    key_size = random.randint(1, 100)
    source_record = []
    expected = {}
    for key in range(key_size):
        record_size = random.randint(1, 100)
        expected[key + 1] = record_size
        source_record = source_record + \
            [[x, f'{key:0>3}'] for x in range(record_size)]
        # end for

    # key1 chunk<1
    test_csv_01_df = pd.DataFrame(source_record, columns=list(type_def))
    test_csv_01_df.to_csv(tempdir.joinpath(test_csv_format.format(31)))

    # key1 chunk<1
    test_csv_01_df = pd.read_csv(
        tempdir.joinpath(
            test_csv_format.format(31)),
        dtype=type_def, chunksize=chunksize)

    my_reader = GroupByChunkReader(test_csv_01_df, 'key')
    count = 0
    for this_df in my_reader:
        count += 1
        assert len(this_df) == expected[count]
        # end for

    assert count == key_size
    with pytest.raises(StopIteration):
        next(my_reader)
        # end with
    # end def
