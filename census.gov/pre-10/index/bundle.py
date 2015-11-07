'''

'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)


    def meta(self):
        
        self.prepare()
        
        with self.session:
            for table in  self.library.dep('proto').schema.tables:
                if 'summary_level' in table.data and table.data['summary_level']:
                    local_table = self.schema.copy_table(table)
                    local_table.add_column('name', datatype = 'varchar', description='Name of area')
        
        self.schema.write_schema()
        
        return True
        

    def levels(self):
        for table in  self.library.dep('proto').schema.tables:
            if ('summary_level' in table.data and table.data['summary_level'] 
                 and int(table.data['summary_level']) >= 40
                 and int(table.data['summary_level']) != 60): # Ignore cosubs
                yield dict(
                    sl=table.data['summary_level'],
                    name=table.name,
                    id=table.id_)


    def build(self):

        self.build_non_years()
        
        self.build_county_years()
        
        return True
        
    def build_non_years(self):
        """Build indexes that don't have year values"""
        year = self.metadata.build.year

        geofile = self.library.dep('geofile').partition

        lr = self.init_log_rate(10000)

        for level in self.levels():
            self.log("Processing {}".format(level['name']))
            p = self.partitions.find_or_new(table = level['name'])
            p.clean()
            
            with p.inserter() as ins:
                for row in geofile.query("SELECT * FROM geofile WHERE sumlevel = ? AND component = '00' ",level['sl']):
                    row = dict(row)
                    lr(str(level['name']))
                    assert bool(row['gvid'])
                    row['year'] = year
                    ins.insert(row)
            
        
        return True
        
    def build_county_years(self):
        """For each state, all of the counties, over trailing 25 years."""
        import datetime
        states = self.partitions.find(table = 'states')
        counties = self.partitions.find(table = 'counties', space=None, time=None)
        
        current_year = datetime.datetime.now().year
        years =  [None] +range(current_year - 25, current_year+1)
        
        lr = self.init_log_rate(10000)

        for state in states.rows:
            p = self.partitions.find_or_new(table = 'counties', space=state.stusab, time='p25ye'+str(current_year))
            p.clean()
            with p.inserter() as ins:
                for county in counties.query("SELECT * FROM counties WHERE state = ?", state.state):
                    d = dict(county)
                    del d['id']
                    for year in years:
                        lr(str(p.identity.name))

                        d['year'] = year

                        ins.insert(d)
                    
                