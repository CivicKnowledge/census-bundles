 # -*- coding: utf-8 -*-
import ambry.bundle 

from censuslib import ACS2009Bundle
from censuslib import ACS09TableRowGenerator as TableRowGenerator

class Bundle(ACS2009Bundle):
    pass 
    
    
    def write_csv(self):
        """Write CSV extracts to a remote"""
        from collections import defaultdict
        import csv
        
        
        remote = self.library.remote('census-extracts')
        s3 = remote.fs
        
        year = self.year
        release = self.release
    
        for p in self.partitions:
            

            rows = defaultdict(list)
            
            table_name = p.table.name

            print 'Loading: ', year, release, table_name
            p.localize()
            
            for i, row in enumerate(p):
                rows[row.sumlevel].append(row.values())
                
                if i > 100: 
                    break
                
            for i, sumlevel in enumerate(sorted(rows.keys())):
                sl_rows = rows[sumlevel]

                file_name = "{}/{}/{}/{}.csv".format(year, release, table_name, sumlevel)
                print 'Writing ', i, file_name, len(sl_rows)
                
                with s3.open(file_name, 'wb') as f:
                    w = csv.writer(f)
                    print [ unicode(c.name) for c in p.table.columns]
                    w.writerow([ unicode(c.name) for c in p.table.columns])
                    for row in sl_rows:
                        w.writerow(row)
            
                
                
    
