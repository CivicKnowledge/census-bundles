# coding=utf-8
'''

@author: eric
'''
from  ambry.bundle import BuildBundle

import os.path
import yaml
 
class Bundle(BuildBundle):
    '''
    Bundle code for US 2010 Census geo files. 
    '''

    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

        self._states = None
        
        bg = self.metadata.build
        self.geoschema_file = self.filesystem.path(bg.geoschemaFile)
        self.states_file =  self.filesystem.path(bg.statesFile)
        
    def meta(self):
        
        '''Generate a series of local geofiles, for each of the summary state and national
        files on the census.gov servers. '''
        import re, yaml, json
        from collections import defaultdict
        
        self.stateTemplates = self.metadata.build.stateFileTemplates

        with open(self.states_file, 'r') as f:
            states =  yaml.load(f) 

        out = defaultdict(dict)

        for releaseid,url in self.metadata.build.nationalFiles.items():
            out[releaseid]['US'] = url

        for releaseid in [3601, 3604]:
            for state_fips, (stateabr, state)  in states.items():
                template = self.stateTemplates[releaseid]
                url = template.format( state=state.replace(' ','_'), stateabr=stateabr.lower())
                
                out[releaseid][stateabr] = url

        with open(self.metadata.build.urlsFile, 'wb') as f:
            # json bit is a deep-copy conversion from defailtdict to dict. 
            f.write(json.dumps(out, indent=4, sort_keys=True)) # json is also yaml

        
        return True

        
    def prepare(self):
        '''Scrape the URLS into the urls.yaml file and load all of the geo data
        into partitions, without transformation'''

        super(Bundle, self).prepare()

        # Create the All partition now. The others are created after we know what
        # summary levels we have. 

        p = self.partitions.new_db_partition(table='geofile', grain='all')
        p.create_with_tables('geofile')          
    
        with open(self.filesystem.path(self.metadata.build.urlsFile), 'r') as f:
            # json bit is a deep-copy conversion from defailtdict to dict. 
            urls = yaml.load(f)
    
        for state in urls['3601'].keys():
            self.partitions.new_csv_partition(table='geofile', space=state.lower())

        return True
 
    @staticmethod
    def georecid(releaseid, stateid, logrecno):
        return ((((int(releaseid) * 10**2)
                  + int(stateid)) * 10**7) 
                  + int(logrecno))
 
    def generate_geofiles(self):
        '''Generate a series of local geofiles, for each of the summary state and national
        files on the census.gov servers. '''
        import re, yaml
        
        self.stateTemplates = self.metadata.build.stateFileTemplates

        with open(self.states_file, 'r') as f:
            states =  yaml.load(f) 
            
        def test_zip_file(f):
            import zipfile
            try:
                with zipfile.ZipFile(f) as zf:
                    return zf.testzip() is None
            except zipfile.BadZipfile:
                return False

        #for releaseid,url in self.metadata.build.nationalFiles.items():
        #    zip_file = self.filesystem.download(url, test_zip_file)
        #    
        #    grf = self.filesystem.unzip(zip_file, re.compile('\w\wgeo2010.sf*'))
        #    
        #    yield releaseid,'us',  grf    

        for releaseid in [3601]:
            for state_fips, (stateabr, state)  in states.items():
                template = self.stateTemplates[releaseid]
                url = template.format( state=state.replace(' ','_'), stateabr=stateabr.lower())
                
                zip_file = self.filesystem.download(url, test_zip_file)
                
                grf = self.filesystem.unzip(zip_file, re.compile('\w\wgeo2010.sf*'))
                
                yield releaseid,stateabr,  grf

        return 

    def build(self):

        self.load()
        self.run_stats()
        self.combine_csv()
        #self.csvize()
        self.split()
        
        self.split_101()
         
        return True
        
    def install(self, force=False):
        
        self.clean_partitions()
        
        return super(Bundle, self).install(force=force)
          
    def load(self):
        
        import yaml
        
        release = 3601
        
        with open(self.filesystem.path(self.metadata.build.urlsFile), 'r') as f:
            # json bit is a deep-copy conversion from defailtdict to dict. 
            all = yaml.load(f)[str(release)]

        n = self.run_args.multi if self.run_args.multi else 1


        if n == 1:
            for state in all.keys():
                self.load_csv(release, state)
        else:
            #from multiprocessing.pool import ThreadPool as Pool
            from multiprocessing import Pool
            # Break the states up into n lists
            procs = []
            
            pool = Pool(n)
    
            r = pool.map(load_csv_mp, [ (i, self.bundle_dir, release,state) for i,state in enumerate(all.keys()) ])


    def run_stats(self):
        '''Calculate the min, max and count for each of the partitions. This would be mar efficielntly done in 
        load_csv, but then we'd have distributed access to the sqlite database. '''
        from ambry.identity import Identity, NameQuery
        
        for p in self.partitions.find_all(space=NameQuery.ANY, table=NameQuery.ANY, format='csv'):
            self.log("Stats for {}".format(p.name))
            p.write_stats()


    def combine_csv(self):
        '''Calculate the min, max and count for each of the partitions. This 
        would be mar efficielntly done in load_csv, but then we'd have 
        distributed access to the sqlite database. '''
        from ambry.identity import Identity, NameQuery

        with self.session:
            all = self.partitions.find(table='geofile', grain='all')
            all.database.session.execute("DELETE FROM geofile")
            all.database.session.commit()

            lr = self.init_log_rate(40000)
        
            for p in sorted(self.partitions.find_all(space=NameQuery.ANY, table=NameQuery.ANY, format='csv'), 
                           key = lambda x: x.record.min_key):

                self.log("Load to sqlite: {}".format(p.identity.space))
                count, tdiff = all.database.load(p, table=p.table, logger=lr)

        

    def load_csv(self, releaseid, state):

        import re, struct
        
        with open(self.filesystem.path(self.metadata.build.urlsFile), 'r') as f:
            # json bit is a deep-copy conversion from defailtdict to dict. 
            all = yaml.load(f)
            url = all[str(releaseid)][state.upper()] 

        zip_file = self.filesystem.download(url, 'zip')
        
        grf = self.filesystem.unzip(zip_file, re.compile('\w\wgeo2010.sf*'))

        table = self.schema.table('geofile')

        unpack_f, header, unpack_str, length = table.get_fixed_unpack()
    
        lr = self.init_log_rate(40000)
        rows = 0

        header = ['geofile_id'] + header
        
        p = self.partitions.find(format='csv',table='geofile', space=state.lower())
        
        with open(grf, 'rbU', buffering=1*1024*1024) as geofile:
            with p.inserter(encoding='utf-8', write_header=False) as ins:
                for line in geofile.readlines():
                    lr('Loading {}'.format(state))
                    rows  += 1

                    try:
                        geo = unpack_f(line[:-1])
                    except struct.error as e:
                        self.error("Struct error for state={}, file={}, line_len={}, row={}, \nline={}"
                                   .format(state,file,len(line),rows, line))
                        raise e
                     
                    if not geo:
                        raise ValueError("Failed to match regex on line: "+line) 
        
                    try:
                        geo =  [None] + list(geo)
                    except ValueError as e:
                        print geo
                        raise e
                        
                    geod = dict(zip(header,geo))

                    georecid = self.georecid(releaseid, 
                                             int(geod['state']) if geod['state'] else 0,
                                             geod['logrecno'])
    
                    geod['name'] = geod['name'].decode('latin1').strip() # The Puerto Rico files has 8-bit names
 
                    geod['geofile_id'] = georecid
                  
                    try:         
                        ins.insert(geod)
                    except Exception as e:
                        self.error("FAILED: {}; {} ".format(geod, e))
                        raise
        

    def csvize(self):
        ''' The first CSV process was for speed, and to allow multi-processing. This one is for use in loading
        data warehouses '''
        
        partition = self.partitions.find(table='geofile', grain='all')
        partition.write_stats()
        partition.csvize(logger=self.init_log_rate(15000), write_header = False)

    def split(self):
        '''Split the SF1 file into blocks and non-blocks components. This will reduce the
        size below the 5gb limit for storing files on Amazon. '''
  
        #
        # Generate a list of unique summary levels 
        # 
        self.log("Compile unique summary levels")
        all = self.partitions.find(table='geofile', grain='all')

        # We're not actually going to use the geofile table, just need it to create th partition
        slpart = self.partitions.find_or_new(table='geofile', grain='sumlevs')
        
        #
        # Create a list of al of the summary levels
        # 
        
        self.log("Create summary levels summary table")
        
        slpart.database.connection.execute('DROP TABLE IF EXISTS sumlev');
    
        n1 = slpart.database.attach(all);
        q="""CREATE TABLE sumlev AS  
        SELECT DISTINCT trim(sumlev) as sumlev, trim(fileid) as fileid FROM {}.geofile;
        """.format(n1)

        slpart.database.connection.execute(q);
        
        slpart.database.detach(n1)


        self.log("Build summary levels file table")
        slpart.database.connection.execute('DROP TABLE IF EXISTS slfiles');
        q="""
CREATE TABLE slfiles AS
SELECT DISTINCT  s0.sumlev, 
s1.fileid as sf1_file, s2.fileid as sf2_file, s1us.fileid as sf1us_file
FROM sumlev as s0
LEFT JOIN sumlev s1us ON s1.sumlev = s0.sumlev AND s1us.fileid = 'SF1US'
LEFT JOIN sumlev s1 ON s1.sumlev = s0.sumlev AND s1.fileid = 'SF1ST'
LEFT JOIN sumlev s2 ON s2.sumlev = s0.sumlev AND s2.fileid = 'SF2ST'
;
        """
        slpart.database.connection.execute(q);
     
        sumlevs = []
        for row in slpart.database.connection.execute('SELECT DISTINCT sumlev FROM sumlev'):
            sumlevs.append(row[0])
            
        #
        # Split by sumary levels
        #

        for sumlev in sumlevs:
            self.log("Splitting summary level {}".format(sumlev))
            partition = self.partitions.find_or_new(table='geofile', grain=str(sumlev))

            db = partition.database

            # Really important! The attachment of the all.db database must be done after this
            # delete. Sometimes all.db gets deleted -- it looks like when the sumlev database
            # is empty, the all.db is deleted instead. 
            db.connection.execute("DELETE FROM geofile")
           
            name = db.attach(all);
            q='INSERT INTO geofile  SELECT * FROM {}.geofile WHERE sumlev = ?'.format(name)
            db.connection.execute(q, sumlev)
            db.detach(name)

 
    def split_101(self):
        '''Split the SF1 file into blocks and non-blocks components. This will reduce the
        size below the 5gb limit for storing files on Amazon. '''

        #
        # Generate a list of states from the 40 sum level file, which is much smaller and
        # faster than the 101 file. 
        # 
        self.log("Compile unique state numbers and names levels")

        sl40 = self.partitions.find(table='geofile', grain='40')
        sl101 = self.partitions.find(table='geofile', grain='101')

        states = {}

        for srow in sl40.query("SELECT DISTINCT state, stusab, name FROM geofile where stusab != 'US' "):

            states[int(srow['state'])] = [str(srow['stusab']), str(srow['name'])]
            
            self.log("Splitting 101 for state {}".format(srow['name']))
            
            pid = dict(table='geofile', space=srow['stusab'].lower(), grain='101')  
            partition = self.partitions.find(**pid)
            if not partition:
                partition = self.partitions.new_partition(**pid)
                partition.create_with_tables('geofile')  
            else:
                partition.clean()
                
            db = partition.database
        
            # Really important! The attachment of the all.db database must be done after this
            # delete. Sometimes all.db gets deleted -- it looks like when the sumlev database
            # is empty, the all.db is deleted instead. 
            db.connection.execute("DELETE FROM geofile")
        
            attach_name = db.attach(sl101.database)
            
            q='INSERT INTO geofile SELECT * FROM {}.geofile WHERE state = ?'.format(attach_name)
            db.connection.execute(q, srow['state'])
            
            db.detach(attach_name)
      
      
        import yaml
        sf = self.filesystem.path('meta','states2.yaml')
        
        with open(sf,"w") as f:
            f.write(yaml.dump(states, indent=4, default_flow_style=False))

    def clean_partitions(self):
        '''Remove the CSV partitions that were used to build the all.db file, since they were just for 
        building all.db in multi-processing.'''
        
        
        for p in self.partitions:
            if p.identity.format == 'csv' and p.identity.space is not None:
                self.log("Removing partition {}".format(p.identity.name))
                
                p.delete()


def load_csv_mp( fargs):
    '''Like Bundle.build_state, but create the bundle after the thread is lanuched, because
    the sqlite driver can't deal with multithreading. '''
    import time, random
    
    i, bundle_dir, release,state = fargs
    
    b = Bundle(bundle_dir)
    
    b.log("Loading MP #{}, {}".format(i, state))
    b.load_csv(release, state)
    

    
