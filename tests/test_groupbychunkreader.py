# coding:utf-8
# ---------------------------------------------------------------------------
# __author__ = 'Satoshi Imai'
# __credits__ = ['Satoshi Imai']
# __version__ = "0.9.0"
# ---------------------------------------------------------------------------

import logging
import shutil
import tempfile
from logging import Logger, StreamHandler
from pathlib import Path
from typing import Generator

import pandas as pd
import pytest

from src.groupbychunkreader import GroupByChunkReader

test_csv_format = 'test_csv_{0:0>3}.csv'
columns = ['value', 'key']
chunksize = 3
type_def = {'value': int, 'key': 'object'}


@pytest.fixture(scope='session', autouse=True)
def setup_and_teardown(tempdir: Path, logger: Logger):
    # setup

    logger.info(tempdir)

    yield

    # teardown
    shutil.rmtree(tempdir, ignore_errors=True)
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
    if tempdir.exists():
        shutil.rmtree(tempdir)
        # end if
    # end def


@pytest.mark.run(order=10)
def test_key1_patterns(tempdir: Path, logger: Logger):
    logger.info('key1_patterns')

    # key1 chunk<1
    test_csv_01_df = pd.DataFrame([[x, '01']
                                  for x in range(chunksize - 1)], columns=columns)
    test_csv_01_df.to_csv(tempdir.joinpath(test_csv_format.format(1)))

    # key1 chunk=1
    test_csv_02_df = pd.DataFrame([[x, '01']
                                  for x in range(chunksize)], columns=columns)
    test_csv_02_df.to_csv(tempdir.joinpath(test_csv_format.format(2)))

    # key1 chunk>1
    test_csv_03_df = pd.DataFrame([[x, '01']
                                  for x in range(chunksize * 2 - 1)], columns=columns)
    test_csv_03_df.to_csv(tempdir.joinpath(test_csv_format.format(3)))

    # key1 chunk>2
    test_csv_04_df = pd.DataFrame([[x, '01']
                                  for x in range(chunksize * 3 + 1)], columns=columns)
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
