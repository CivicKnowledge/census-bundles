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
            if 'summary_level' in table.data and table.data['summary_level'] and int(table.data['summary_level']) >= 40:
                yield dict(
                    sl=table.data['summary_level'],
                    name=table.name,
                    id=table.id_)


    def build(self):

        geofile = self.library.dep('geofile').partition

        lr = self.init_log_rate(10000)

        for level in self.levels():
            self.log("Processing {}".format(level['name']))
            p = self.partitions.find_or_new(table = level['name'])
            p.clean()
            
            with p.inserter() as ins:
                for row in geofile.query("SELECT * FROM geofile WHERE sumlevel = ? AND component = '00' ",level['sl']):
                    lr(str(level['name']))
                    ins.insert(row)
            
        
        
        return True

