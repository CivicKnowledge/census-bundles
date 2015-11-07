'''

'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)


    @staticmethod
    def nocommas(v):
        """Removes commans from numbers. References in the d_caster column in the schema. """
        
        return int(str(v).replace(',',''))

        
    def gen_rows(self, map=None):
        import csv
        fn  = self.filesystem.download('vetpop')
        
        self.log("Opening {} ".format(fn))
        
        with open(fn) as f:
            r = csv.DictReader(f);
            
            for row in r:
                if map:
                    yield {map[k]:v for k, v in row.items()}
                else:
                    yield row
        
    def meta_gen_schema(self):
        
        self.database.create()
    
        self.schema.update('vetpop', self.gen_rows(), logger = self.init_log_rate(500))

    def meta(self):
        self.meta_gen_schema()
        
        return True

    def build(self):
        
        t = self.schema.table('vetpop')
        map = {c.data['header']:c.name for c in t.columns}
        
        p = self.partitions.find_or_new(table='vetpop')
        p.clean()
        
        lr = self.init_log_rate(1000)
        
        with p.inserter() as ins:
            for i,row in enumerate(self.gen_rows(map=map)):
                try:
                    fips = int(row['fips'])
                except ValueError: # a "Grand Total" row
                    continue
                    
                row['state'] = int(fips/1000)
                row['county'] = fips%1000
                
                ins.insert(row)
                
                
                lr()
                
        return True
        
        
        
        
        
        
        
        
        
        
        

