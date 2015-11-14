# Transforms for values, casters, etc.

from ambry.util import memoize

class JamValueMixin(object):
    
     jam_map = {
         '.': 'm', # Missing or suppressed value
         ' ': 'g',
         None: 'N',
         '': 'N'
     }
    
     def jam_float(self,v,errors, row):
         """Convert jam values into a code in the jam_values field and write a None"""
         from ambry.valuetype.types import nullify
         v = nullify(v)
        
         try:
             return float(v)
         except:
             if not 'jams' in errors:
                 errors['jams'] = ''
                
             try:
                 errors['jams'] += self.jam_map[v]
             except KeyError:
                 self.error(row)
                 raise
                
             return None
        
     
     def jam_values(self, errors, row):
         """Write the collected jam codes to the jam_value field."""
         from itertools import chain, groupby
        
         jams =  errors.get('jams')
        
         def rle(s):
             "Run-length encoded"
             return ''.join(str(e) for e in chain(*[(len(list(g)), k) 
                                             for k,g in groupby(s)]))
        
       
         return rle(jams) if jams else None

    
class JoinGeofileMixin(object):
    
     @property
     @memoize
     def geofile(self):
         with self.dep('geofile').datafile.reader as r:
             return { (row.stusab, row.logrecno): (row.geoid, row.sumlevel)  
                      for row in r }
        
     def join_geoid(self, row):
         """Add a geoid to the row, from the geofile partition, linked via the
         state abbreviation and logrecno"""
         return self.geofile[(row.stusab.upper(), int(row.logrecno))][0]
 
 