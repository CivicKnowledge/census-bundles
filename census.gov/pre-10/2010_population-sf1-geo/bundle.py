'''
'''

from  ambry.bundle import BuildBundle
 
class glist(list): # a list that auto expands
    def __setitem__(self, index, value):
        if index >= len(self):
            self.extend([None]*(index + 1 - len(self)))
        list.__setitem__(self, index, value)
        
class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
 
    def meta(self):
        """Copy the schema for tables from the original import of SF1"""
        self.database.create()
        self.meta_copy_schema()
        
        return True
        
    def meta_implemented_tables(self):
        """Return a list of the tables in the SF1 file that are actually implemented"""
 
        sf1 = self.library.dep('sf1')
        
        q="""SELECT t_name, t_vid, p_vid
        FROM partitions, tables  
        WHERE t_id = p_t_id AND p_format = 'db' 
        ORDER BY t_name"""
        
        return sf1.database.query(q)
        
 
    def meta_copy_schema(self):
        '''Copy the tables from the SF1 schem, but omit the stusab and
        logrecno columns and add a geoid column'''
        self.schema.clean()
        
        sf1 = self.library.dep('sf1')
        
        it = self.meta_implemented_tables()
        
        with self.session as s:
            for te in it:
                orig_table = sf1.schema.table(te['t_name'])
                d  = orig_table.to_dict()
                del d['name']
                t = self.schema.add_table(te['t_name'], **d)

                for c in orig_table.columns:
                    d = c.to_dict()

                    if c.name not in ['stusab', 'logrecno']:
                        t.add_column(d['name'], 
                                    datatype = d['datatype'],
                                    size = (d['size'] if d['size'] 
                                            else d.get('width',None)),
                                    description = d['description'],
                                    is_primary_key = c.name == 'id'
                                    )
                                    
                    if c.name == 'id': # Add geoid after id
                        t.add_column('geoid', datatype='VARCHAR', uindexes='ui1')
                                    

                self.log("Copy table schema: {}".format(orig_table.name))
        
        self.schema.write_schema()

    def prepare(self):
        '''Re-load all of the partitions. This could be really slow. '''

        super(Bundle, self).prepare()

        self.log("Getting geodim")
        self.library.dep('geodim')

        self.log("Getting sf1")
        sf1 = self.library.dep('sf1')
        
        self.log("Getting geofile")
        geofile = self.library.dep('geofile')
        
        for table_name in self.metadata.build.tables:
            # .get() retrieves the partition from the library; otherwise it's
            # just an unmaterialized record
            p = sf1.partitions.find(table=table_name)
            self.log("Getting: {}".format(p.identity.vname))
            p.get()
       
        return True

    def build_geoid_templates(self):
    
        t_to_sl = self.metadata.build.sumlevels
    
        geodim = self.library.dep('geodim')
        templates = {}
        for table in geodim.schema.tables:
            a = glist()
            for c in table.columns:
                if c.data['geoid']:
                    a[int(c.data['geoid'])-1] = (c.name, c.data['geoidl'])
            
            if not a:
                continue
                
            template = ''
            
            for e in a:
                template += "{{{name}:0{length}d}}".format(name=e[0], length=e[1])

            templates[t_to_sl[table.name]] = template
            
        return templates
   

    def build_index(self):
        '''Build a really, really big hash for mapping stusab + logrecno to 
        geoids'''
        from time import sleep

        import cPickle as pickle
        import os.path


        geofile = self.library.dep('geofile').partition
        
        lrr = self.init_log_rate(20000)
        lrc = self.init_log_rate(100000)
        
        templates = self.build_geoid_templates()

        all_states = [ r[0] for r in geofile.query(
            "SELECT DISTINCT stusab FROM geofile ")]

        f = self.filesystem.path('build','indexes','all_states.pkl')
        
        if os.path.exists(f):
            self.log("Index is already built; skipping")
            return 

        with open(f,'w') as f:
            pickle.dump(all_states,f)        

        self.log("Building index from {}".format(geofile.database.path))

        for j, stusab in enumerate(all_states):
            stusab = stusab.lower()
            
            indexes = {}
            
            self.log("Load indexes for state  {}".format(stusab))
            for i,row in enumerate(geofile.query("""SELECT * FROM geofile 
                                    WHERE stusab = ? and  geocomp = '00'""", 
                                    stusab.upper())):

                lrc("Read")

                try:
                    template = templates[row['sumlev']]
                except KeyError:
                    continue
                
                lrn = int(row['logrecno'])
          
                # Can't use defaultdict b/c we need it to throw KeyError
                # in build_partition()
                if not row['sumlev'] in indexes:
                   indexes[row['sumlev']] = {}

                indexes[row['sumlev']][lrn] = template.format(**dict(row))
                
                lrr()


            for sumlevel, index in indexes.items():
                f = self.filesystem.path('build','indexes',stusab, str(sumlevel))
                self.log("Writing index file: {}".format(f))

                with open(f,'w') as f:
                    pickle.dump(index,f)

    def load_index(self, states, sumlevels):
        '''Load index files based on state abbreviation and summary level'''
        import cPickle as pickle

        index = {}
        
        if not isinstance(sumlevels, list):
            sumlevels = [sumlevels]
       
        if not isinstance(states, list):
            states = [states]       

        self.log("Load indexes states = {}, sumlevels = {} ".format(states, sumlevels))

        for stusab in states:
            
            stusab = stusab.lower()
            
            for sl in sumlevels:
                fn = self.filesystem.path('build','indexes',stusab, str(sl))
                
                with open(fn) as f:
                    o = pickle.load(f)

                    if not stusab in index:
                        index[stusab] = {}

                    index[stusab] =  dict( index[stusab].items() + o.items())

        return index

    def all_states(self):
        import cPickle as pickle
        
        with open(self.filesystem.path('build','indexes','all_states.pkl')) as f:
            return pickle.load(f)
        
    def build(self):
        self.build_index()
        
        self.build_partitions()
        
        return True
        
    def build_partitions(self):
        from ambry.partition import PartitionIdentity
        
        geodim = self.library.dep('geodim')
        

        # Setup  hash to build the right queries for the
        # grain of the geography partitions. 
        
        t_to_sl = dict(self.metadata.build.sumlevels)

        args = []

        for gp in geodim.partitions:
            
            # Setup the parameters to create a local partition that
            # parches the geography partitions
            grain  = gp.identity.table 

            if grain in ['states','regions','divisons']:
                continue

            try:
                sl = t_to_sl[grain]
            except KeyError:
                continue
                
            if grain == 'blocks':
                space = gp.identity.space
            else:
                space = None

            for table_name in self.metadata.build.tables:      
                args.append( (self.bundle_dir,table_name,grain, space, sl) )          

        n = self.run_args.multi if self.run_args.multi else 1

        if n == 1:
            for bundle_dir, table_name,grain, space, sl in args:
                self.build_partition(table_name, grain, space, sl)
        else:
            from multiprocessing import Pool

            pool = Pool(n)
    
            r = pool.map(run_build_partition, args)
     
    def build_partition(self,table_name, grain, space, sl):
        """Break up all of the input tables into the partitions associated with 
        a single geo partition"""
        
        
        self.log("Run loop for: {} {} {} {}".format(table_name, grain, space, sl))
        
        # Load in the appropriate pickled index files to map the
        # state abbreviation and logrecno to a geoid
        if not space:
            states = self.all_states()
        else:
            states = space
            
        index = self.load_index(states, sl)

        sf1 = self.library.dep('sf1')

        # .get() retrieves the partition from the library; otherwize 
        # it's just an unmaterialized record

        p_in = sf1.partitions.find(table=table_name).get()
        self.log("Getting: {}".format(p_in.identity.vname))

        p_out = self.partitions.find_or_new(grain=grain, 
                            space = space, table=table_name)


        p_out.drop_indexes()

        cols = [ 't.'+c.name for c in p_in.table.columns if c.name not in 
                ('id','stusab','logrecno')]


        lrc = self.init_log_rate(print_rate=5)

        gt = self.build_geoid_templates()

        p_name = p_out.identity.name
        
        p_out.query("DELETE FROM {}".format(table_name))
        
        seen_geoids = set()
        
        q = 'SELECT * FROM {}'.format(table_name)
        
        if space:
            q += " WHERE stusab = '{}'".format(space.upper())

        with p_out.inserter() as ins:
            
            for row in p_in.query(q):

                # If the record is for a summary level that we aren't covering
                # There won't be an entry in the index
                
                lrn_dict = index[row['stusab'].lower()]
                
                try:
                    
                    geoid = lrn_dict[int(row['logrecno'])]
                    
                except KeyError:
                    continue

                row = {k:v.strip() for k,v in row.items() if v }
                row['geoid'] = geoid
  
                if geoid in seen_geoids:
                    raise Exception("Dupe geoid: {}".format(geoid))
  
                seen_geoids.add(geoid)
  
                lrc("Copy to {}".format(p_name))
    
                ins.insert(row)

        p_out.create_indexes()

def run_build_partition( fargs):
    '''Like Bundle.build_state, but create the bundle after the thread is lanuched, because
    the sqlite driver can't deal with multithreading. '''
    import time, random,traceback, os
    
    bundle_dir, table_name, grain, space, sl = fargs
    
    st_id = "table={}, grain={} space={}, sl={}".format(
                                table_name, grain, space, sl)
    try:
        b = Bundle(bundle_dir)
        b.library.database.close()
        
        b.log("MP Run: {} {} ".format(os.getpid(),st_id))
        b.build_partition(table_name, grain, space, sl)
    except:
        tb = traceback.format_exc()
        print '==========vvv Segment: {}==========='.format(st_id)
        print tb
        print '==========^^^ Segment: {}==========='.format(st_id)
        raise



            