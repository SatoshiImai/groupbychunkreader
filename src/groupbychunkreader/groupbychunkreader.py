# coding:utf-8
# ---------------------------------------------------------------------------
# __author__ = 'Satoshi Imai'
# __credits__ = ['Satoshi Imai']
# __version__ = '0.9.1'
# ---------------------------------------------------------------------------

import pandas as pd


class GroupByChunkReader:

    def __init__(self, source_df: pd.DataFrame, group_by: str):
        self.source_df = source_df
        self.source_closed = False
        self.group_by = group_by
        self.df_pool = {}
        # end def

    def __iter__(self):
        return self
        # end def

    def __next__(self):
        if self.source_closed:
            raise StopIteration
        else:
            while not self.source_closed and len(self.df_pool) <= 1:
                try:
                    this_df = next(self.source_df)
                except StopIteration:
                    this_df = pd.DataFrame([], columns=[self.group_by])
                    self.source_closed = True
                    # end try

                if len(self.df_pool) == 0 and len(this_df) == 0:
                    self.source_closed = True
                    return this_df
                    # end if

                for split_key in list(this_df[self.group_by].unique()):
                    split_df = this_df.loc[
                        this_df[self.group_by] == split_key].copy()

                    if split_key in self.df_pool:
                        split_df = pd.concat(
                            [self.df_pool[split_key], split_df], axis=0)
                        # end if
                    self.df_pool[split_key] = split_df
                    # end for
                # end while
            return self.df_pool.pop(sorted(list(self.df_pool))[0])
            # end if
        # end def

    # end class
