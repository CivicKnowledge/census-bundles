""""""

from ambry.bundle.loader import CsvBundle


class Bundle(CsvBundle):
    @staticmethod
    def clean_na(v):

        if 'N/A' in v:
            return None
        else:
            return v

    @staticmethod
    def strip(v):
        return v.strip()

    def build_modify_row(self, row_gen, p, source, row):
        """
        Modify a row just before being inserted into the partition

        :param row_gen: Row generator that created the row
        :param p: Partition the row will be inserted into
        :param source: Source record of the original data
        :param row: A dict of the row
        :return:
        """

        # If the table has an empty year, and the soruce has a time that converts to an int,
        # set the time as a year.
        stcou = row['stcou'].strip('="')
        row['stcou'] = stcou
        row['state_fps'], row['county_fips'] = stcou[:2], stcou[2:]
        super(Bundle, self).build_modify_row(row_gen, p, source, row)