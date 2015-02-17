'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
from ambry.util import memoize


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    @property
    @memoize
    def states(self):
        '''Maps state appreviations to state names'''
        import yaml

        with open(self.filesystem.path('meta','states.yaml')) as f:
            return yaml.load(f)

    def get_url(self, geo, stusab):
        '''Return a URL for a segment file'''
        config = dict(self.metadata.build)
        
        state = self.states[stusab.upper()]
        
        t = str(self.metadata.sources.root.url)+str(self.metadata.sources[geo].url)
        
        url = t.format(
            span = self.metadata.build.end_year - self.metadata.build.start_year + 1, 
            lc_utstab = stusab.lower(), 
            state=state.title().replace(' ',''), 
            **config
        )
        
        return url

    def gen_rows(self, as_dict = False ):
        import csv
        from geoid  import generate_all, summary_levels
        
        full_header = [ c.name for c in self.schema.table('geofile').columns ]
        
        prefix, header = full_header[:3], full_header[3:]
        
        #Summary levels that will produce gvids
        sls = [ e[0] for e in summary_levels]
        
        for stusab, state in self.states.items():

            
            # Only using the 'large' geofiels, since they aere the same as the smalls
            url = self.get_url('large',stusab)
            file = self.filesystem.download(url)
            #self.log("Generating from {}".format(url))
            
            with open(file, 'r', buffering=1*1024*1024) as f:
                r = csv.reader(f)
                
                for row in r:
                    
                    drow =  dict(zip(header, row))
                    
                    try:
                        geoids = generate_all(drow['sumlevel'], drow)

                    except Exception as e:
                        self.error("Failed to create geoids for: {}: {}".format(drow['sumlevel'], e))
                        geoids = {}
                        print drow
                        raise
                 
                    if bool(drow['sumlevel'] in sls) and not bool(geoids.get('gvid',False)): # Sort of an xor
                        self.error("Got no givd for sl = {}. Row = {}".format(drow['sumlevel'], drow))
                 
                 
                    if as_dict:
                        yield dict(zip(header, row)+geoids.items())
                    else:
                        yield full_header, [None, geoids.get('gvid',None), geoids.get('geoidt',None)] + row
    

    def meta_schema(self):
        """Load the schema from the geofile header from 2012. The file is intended to be the first two rows
        of an excel spreadsheet. It's missing from 2013 5 year, so we are using the one from 2013 3 year. """
        from xlrd import open_workbook
        
        self.prepare()
        
        fn  =  self.source('geofile_header_2013')
        
        wb = open_workbook(fn)
        
        s = wb.sheets()[0]
        
        def srow_to_list(row_num, s):
            """Convert a sheet row to a list"""

            values = []

            for col in range(s.ncols):
                values.append(s.cell(row_num, col).value)

            return values
                   
        with self.session:
            table = self.schema.add_table('geofile', description='2013 ACS geofile')
            self.schema.add_column(table, 'id', datatype='integer', is_primary_key=True)
            self.schema.add_column(table, 'gvid', datatype='varchar')
            self.schema.add_column(table, 'geoidt', datatype='varchar')
            
            for i, (col_name, description) in enumerate(zip(srow_to_list(0,s),srow_to_list(1,s))):
                
                # The multiple instances of the column name 'blank' will only get added once, 
                # leaving holes in the header. 
                if col_name.lower() == 'blank':
                    col_name = col_name + str(i)
                
                self.schema.add_column(table, col_name, datatype='integer', description = description)

        self.schema.write_schema()

    def meta_update(self):
     
        self.prepare()

        self.schema.update_from_iterator('geofile', 
                                   header = self.gen_rows().next()[0],
                                   iterator=self.gen_rows(),
                                   max_n=100000,
                                   logger=self.init_log_rate(print_rate=10))


    def meta_augment(self):
        """Add proto ids to the table"""
        
        protos = self.library.dep('protos')
        
        pmap = {}
        
        for table in ('blockgroups', 'tracts','places','counties','states','regions','divisions','censusarea'):
            cat = protos.schema.table(table)
            for c in cat.columns:
                if c.name == 'id':
                    continue
                pmap[c.name] = c.id_ if not c.proto_vid else c.proto_vid

        if 'blockgroup' in pmap and 'blkgrp' not in pmap:
            pmap['blkgrp'] = pmap['blockgroup']

        with self.session:
            
            for c in self.schema.table('geofile').columns:
                if c.name in pmap:
                    c.proto_vid = pmap[c.name]
                    
        self.schema.write_schema()
                
            
    def meta(self):
        self.meta_schema()
        self.meta_update()
        self.meta_augment()
        
        return True


    def build(self):
        from geoid  import generate_all

        p = self.partitions.find_or_new(table='geofile') 
        p.clean()
    
        lr = self.init_log_rate(print_rate = 10)
        
        
        
        with p.inserter() as ins:
            
            for row in self.gen_rows(as_dict = True):
                row['stusab'] = row['stusab'].lower()
                row['name'] = row['name'].decode('latin1')
                lr(row['stusab'])
               
                
                e = ins.insert(row)
                
                if e:
                    self.error(e)
                    
        return True
                