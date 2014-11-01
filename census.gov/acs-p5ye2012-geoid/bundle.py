'''

'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)



    def meta_make_schema(self):
        
        """Copy the schema from the source, removing old values, and adding new ones. """
        self.prepare()
        
        acs = self.library.dep('acs')
        
        excludes = ['fileid', 'filetype', 'stusab','sequence','logrecno', 'chariter' ]
        
        with self.session as s:
            for i, t in enumerate(acs.schema.tables):
                print t.name

                nt = self.schema.add_table(t.name)
            
                columns = []
                for c in t.columns:

                    if c.name in excludes:
                        continue

                    d = c.dict
                    d['proto_vid'] = d['vid']
                    del d['t_vid']
                    del d['vid']
                    del d['id_']
                    del d['sequence_id']

                    if c.name == 'id':
                        d['proto_vid'] = None
                        columns.append(d)
                        columns.append(dict(name='geoid',datatype='varchar', description='ACS Geoid'))
                        columns.append(dict(name='gvid',datatype='varchar', description='Civic Knowledge Geoid'))
                    else:
                        columns.append(d)
                        
                
                for c in columns:
                    self.schema.add_column(nt, **c)

        self.schema.write_schema()
       
    def meta(self):
        
        self.meta_make_schema()
        
        return True
        
    def load_geofile(self):
        import cPickle as pickle
        import os
        
        gff = self.filesystem.build_path('geofile_cache.pkl')
        
        if os.path.exists(gff):
            
            with open(gff) as f:
                return pickle.load(f)
                
        
        gf = self.library.dep('acs_geofile').partition
        sumlevels = self.metadata.build.summary_levels
        
        geoids = {}
        
        lr = self.init_log_rate(25000)
        i = 0
        for  row in gf.rows:
            if row['sumlevel'] in sumlevels:
                stusab = row['stusab']
                lrn = row['logrecno']
                
                if stusab not in geoids:
                    geoids[stusab]  = {}
                
                lr("Add logrecno row")
                geoids[stusab][lrn] = (row['geoid'], row['geoidt'], row['gvid'])
                i += 1
                
            if self.run_args.test and i > 50000:
                break
                
        with open(gff,'w') as f:
            pickle.dump(geoids, f, -1)
                
        return  geoids
        

    def build(self):
        
        b = self.library.dep('acs')
        gf = self.load_geofile()
        
        if int(self.run_args.get('multi')) > 1:
            self.database.close()
            self.run_mp(self.build_partition, [ (p.vid,) for p in b.partitions] )

            
        else:
            for in_p in b.partitions:
                self.build_partition(in_p.vid)
            
        return True
 
            
    def build_partition(self, p_vid):

        p = self.library.dep('acs').partitions.get(p_vid)
        p.get() # Load from remote
        
        
        self.log("Load geoid cache")
        gf = self.load_geofile()
        self.log("Fetching: {}".format(str(p)))
        
        self.log("Copying: {}".format(str(p)))
        
        
        out_p = self.partitions.find_or_new(table = p.table.name, grain = p.grain)
        out_p.clean()
    
        lr = self.init_log_rate(5000)
        with out_p.inserter() as ins:
            for row in p.rows:
                row = dict(row)
                lr()
                
                try:
                    gfr = gf[row['stusab']][row['logrecno']]
                    row['gvid'] = gfr[2]
                    row['geoidt'] = gfr[1]
                    row['geoid'] = gfr[0]
                except KeyError:
                    pass
                
                ins.insert(row)
                
        p.close()
        out_p.close()
                
                
                
            
            
    
    
        
    
            
        
        
        
