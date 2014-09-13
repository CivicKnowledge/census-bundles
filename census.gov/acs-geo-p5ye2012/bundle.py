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
        config = dict(self.metadata.build.config)
        
        state = self.states[stusab.upper()]
        
        t = str(self.metadata.sources.root.url)+str(self.metadata.sources[geo].url)
        
        url = t.format(
            end_year= 2012, span = 5, start_year = 2008,
            lc_utstab = stusab.lower(), 
            state=state.title().replace(' ',''), 
            **config
        )
        
        return url

    def meta(self):
     
        

        self.database.create()
        self._prepare_load_schema()
        import os
        
        header, cregex, regex = self.schema.table('geofile').get_fixed_regex()
        
        lr = self.init_log_rate(10000)
        
        def gen_rows():
            for stusab, state in self.states.items():
                for geo in ('large','small'):

                    file = self.filesystem.download(self.get_url(geo,stusab))

                    with open(file, 'r', buffering=1*1024*1024) as f:
                        for line in f:
                        
                            g =  cregex.search(line)
                            if g:
                                yield dict(zip(header, g.groups()))
            
        
        self.schema.update('geofile', gen_rows(), n=200000, logger=lr)

    def build(self):
        import uuid
        import random
        import os

        files = 0
        
        p = self.partitions.find_or_new(table='geofile') #, grain='large')
        p.clean()
        
        #p = self.partitions.find_or_new(table='geofile', grain='small')
        #p.clean()


        # Using only the 'large' files
        # From the structure of the FTP site, it looks like the geo files are split on the same lines
        # as the data tables, but instead, the large and small geo files are identical, including all of the
        # records. 
        segs = [ ('large', stusab)  for  stusab, state in self.states.items()  ]
        
        if self.run_args.test:
            segs = segs[:4]

        # Can't run this MP, since there is only one partition. 
        for seg in segs:
            self.build_segment(*seg)
            
        return True
        
    def build_segment(self, geo, stusab):
        import os
        
        self.error("Segment: {} {} ".format(stusab, geo))
        
        lr = self.init_log_rate(10000)
        
        p = self.partitions.find_or_new(table='geofile') #, grain=geo)
        
        header, cregex, regex = p.table.get_fixed_regex()

        url = self.get_url(geo,stusab)
        file = self.filesystem.download(url)

        with p.inserter() as ins:
            with open(file, 'r', buffering=1*1024*1024) as f:
            
                for i, line in enumerate(f):
                
                    g =  cregex.search(line)
                
                    if not g:
                        raise Exception("Failed to process line:\n{}\nLength: {}\nIn file: {}"
                        .format(line, len(line),file))

                    row = dict(zip(header, [ x.strip() for x in g.groups()]))
      
                    row['stusab'] = row['stusab'].lower()
                
                    lr("Segment: {} {} ".format(stusab, geo))
                    ins.insert(row)
        
        



