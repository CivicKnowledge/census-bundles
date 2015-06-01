""""""

from ambry.bundle.loader import ExcelBuildBundle
from ambry.util import memoize

class Bundle(ExcelBuildBundle):

    """"""
    
    @staticmethod
    def int_caster(v):
        """Remove commas from numbers and cast to int"""

        try:
            v = v.replace(',', '').replace('.','').strip()
            
            if not bool(v):
                return None
            
            return int(v)
        except AttributeError:
            return v
            
    @staticmethod
    def real_caster(v):
        """Remove commas and periods from numbers and cast to float"""

        try:
            v = v.replace(',', '')
            
            if not bool(v.replace('.','').strip()):
                return None
            
            return float(v)
        except AttributeError:
            return v
            
    @property
    @memoize
    def county_map(self):

        return { (int(r['state']), int(r['county'])) : r['gvid'] for r in  self.library.dep('counties').partition.rows }
            
    def build_modify_row(self, row_gen, p, source, row):
   
        # If the table has an empty year, and the soruce has a time that converts to an int,
        # set the time as a year.
        if not row.get('year', False) and source.time:
            try:
                row['year'] = int(source.time)
            except ValueError:
                pass
             
        if 'postal_code' in row:
            pass 
            
        row['county_gvid'] =  self.county_map.get((int(row['state_fips']), int(row['county_fips'])), None)
            
            
    def mangle_header(self, header):
        """Transform the header as it comes from the raw row generator into a column name"""
        
        lh = None
        new_header = []
        for i,n in enumerate(header):
            
            if '90' not in n:
                ln = n
            else:
                n = n+'_'+ln
            
            new_header.append(self.mangle_column_name(i, n))
    
        return new_header


