""""""

from ambry.bundle.loader import CsvBundle


class Bundle(CsvBundle):

    def build_modify_row(self, row_gen, p, source, row): 
        row['year'] = int(source.time)
        
    def build(self):
        super(Bundle, self).build()
        
    def make_schema(self):
        
        p = self.partitions.find(table ='geofile_schema')
        
        t = self.schema.add_table('geofile')
        
        for row in p.query('SELECT * FROM geofile_schema ORDER BY seq'):
            print dict(row)
        