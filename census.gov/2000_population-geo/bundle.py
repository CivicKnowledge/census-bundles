'''

@author: eric
'''
from  ambry.bundle import BuildBundle
import os.path
import yaml
 
class Bundle(BuildBundle):
    '''
    Bundle code for US 2000 Census, Summary File 2
    '''
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

        self._states = None
        
        bg = self.metadata.build
        self.geoschema_file = self.filesystem.path(bg.geoschemaFile)
        self.states_file =  self.filesystem.path(bg.statesFile)
        self.urls_file =  self.filesystem.path(bg.urlsFile)

    def generate_urls(self, space,index):
        '''
        Generate URLS for one of the summary files. The file number is index.
        space is either 'state' or 'national'
        '''

        with open(self.states_file, 'r') as f:
            states =  yaml.load(f) 
 
        template = self.metadata.build.urlTemplate

        if space == 'state':
            for stateabr, state in states.items():
                url = template.format(index=index, state=state, stateabr=stateabr)
                yield stateabr, url

        elif space == 'national':
            nationals = self.metadata.build.nationalFiles

            yield 'us',  nationals[index]

        return 
 
    def build(self):
        self.load()
        self.combine()
        #self.csvize()
        self.split()
        return True
           
    def load(self):
        

        n = self.run_args.multi if self.run_args.multi else 1
        
        spaces = ['national','state']
        indexes = [1,2,3,4]
        num_files = len(spaces) * len(indexes)
        
        n = min(n, num_files)

        if n == 1:
            for space in spaces:
                for index in indexes:
                     self.load_file(space, index)
        else:
            from multiprocessing import Pool
            pool = Pool(n, maxtasksperchild=1)
    
            r = pool.map(run_load_file, 
                    [ (self.bundle_dir, space, index) 
                        for space in spaces 
                        for index in indexes])
            
    def load_file(self, space, index):
        
        table = 'geofile'
        
        partition = self.partitions.find_or_new(
                        table=table, 
                        space=space,
                        grain='sf{}'.format(index)) 

        self.log('LOADING: {}'.format(partition.name))

        partition.database.query("DELETE FROM {}".format(table))

        lr = self.init_log_rate(50000)

        with partition.database.inserter(table) as ins:        
            for  state, url in  self.generate_urls(space, index):
                log_message = "{} {} {}".format(space, state, index)
                for i, row in enumerate(self.generate_rows(state, url)):

                    lr(log_message)

                    row['name'] = row['name'].decode('latin1') # The Puerto Rico files has 8-bit names
            
                    row = { k:v.strip() for k,v in row.items()}

                    try:
                        ins.insert(row)  
                    except Exception as e:
                        self.error("ERROR: {}".format(e.message))        

    def combine(self):
        
        
        
        all = self.partitions.find_or_new(table='geofile', grain='all')
        
        try: all.database.query("DELETE FROM geofile")
        except: pass

        columns = [ c.name for c in all.get_table().columns ]

        columns.pop(0) # Remove the id. Its needs to be regenerated to be unique
        columns = ['NULL'] + columns
        
        q = "INSERT INTO geofile SELECT {} FROM {{}}.geofile".format(','.join(columns))
        
        for p in self.partitions:
            
            if p.identity.grain =='all' or p.identity.format == 'csv':
                continue
            
            self.log("Copying records from {} ".format(p.name))
            attach_name = all.database.attach(p)

            all.query(q.format(attach_name))

            all.database.detach(attach_name)
    
    def csvize(self):

        for partition in self.partitions:
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

        if not all:
            self.fatal("Could not locate 'all' partition " )

        # We're not actually going to use the geofile table, just need it to create th partition
        slpart = self.partitions.find_or_new(table='geofile', grain='sumlevs')
        
        #
        # Create a list of al of the summary levels
        # 
        
        self.log("Create summary levels summary table")
        
        slpart.database.connection.execute('DROP TABLE IF EXISTS sumlev');
    
        n1 = slpart.database.attach(all);
        q="""CREATE TABLE sumlev AS  
        SELECT DISTINCT cast(trim(sumlev) AS INTEGER) as sumlev, trim(fileid) as fileid FROM {}.geofile;
        """.format(n1)

        slpart.database.connection.execute(q);
        
        slpart.database.detach(n1)
  
        #
        # For each summary level, comple all of the files that the summary level appears in
        # 
  
        self.log("Build summary levels file table")
        
        slpart.database.connection.execute('DROP TABLE IF EXISTS slfiles');
        q="""
CREATE TABLE slfiles AS
SELECT DISTINCT  cast(s0.sumlev as INTEGER) as sumlev, 
s1.fileid as sf1_file, s2.fileid as sf2_file, s3.fileid as sf3_file, s4.fileid as sf4_file,
s1us.fileid as sf1us_file, s2us.fileid as sf2us_file, s3us.fileid as sf3us_file, s4us.fileid as sf4us_file
FROM sumlev as s0
LEFT JOIN sumlev s1 ON s1.sumlev = s0.sumlev AND s1.fileid = 'uSF1'
LEFT JOIN sumlev s2 ON s2.sumlev = s0.sumlev AND s2.fileid = 'uSF2'
LEFT JOIN sumlev s3 ON s3.sumlev = s0.sumlev AND s3.fileid = 'uSF3'
LEFT JOIN sumlev s4 ON s4.sumlev = s0.sumlev AND s4.fileid = 'uSF4'
LEFT JOIN sumlev s1us ON s1.sumlev = s0.sumlev AND s1us.fileid = 'uSF1F'
LEFT JOIN sumlev s2us ON s2.sumlev = s0.sumlev AND s2us.fileid = 'uSF2F'
LEFT JOIN sumlev s3us ON s3.sumlev = s0.sumlev AND s3us.fileid = 'uSF3F'
LEFT JOIN sumlev s4us ON s4.sumlev = s0.sumlev AND s4us.fileid = 'uSF4F'
;
        """
        
        slpart.database.connection.execute(q);

        #
        # Now we can break out all of the levels into seperate partitions
        #
     
        sumlevs = []
        for row in slpart.database.connection.execute('SELECT DISTINCT sumlev FROM sumlev'):
            sumlevs.append(row[0])    
        
        for sumlev in sumlevs:
            
            partition = self.partitions.find_or_new(table='geofile', grain=str(sumlev))
            self.log("Splitting summary level {} to {}".format(sumlev, partition.identity.name))
            db = partition.database
            
            db.connection.execute("DELETE FROM geofile")
            name = db.attach(all);
            q='INSERT INTO geofile  SELECT * FROM {}.geofile WHERE sumlev = ?'.format(name)
            db.connection.execute(q, sumlev)
            
            db.detach(name)
  
    def generate_rows(self, state, geo_source):
        '''A generator that yields rows from the state geo files. It will 
        unpack the fixed width file and return a dict'''
        import struct
        import zipfile

        table = self.schema.table('geofile')
        unpack_f, header, unpack_str, length = table.get_fixed_unpack() 
        rows = 0;
        
        geo_zip_file = self.filesystem.download(geo_source, 'zip')

        grf = self.filesystem.unzip(geo_zip_file)

        geofile = open(grf, 'rbU', buffering=1*1024*1024)

        for line in geofile.readlines():
            
            rows  += 1

            try:
                geo = unpack_f(line[:-1])
            except struct.error as e:
                self.error("Struct error for state={}, file={}, line_len={}, row={}, \nline={}"
                           .format(state,grf,len(line),rows, line))
             
            if not geo:
                raise ValueError("Failed to match regex on line: "+line) 

            yield dict(zip(header,geo))

        geofile.close()

def run_load_file(a):
    import traceback
     
    dir_, space, index = a

    try:
        b = Bundle(dir_)
        b.log("MP Run for {} {}".format(space, index))
        b.load_file(space, index)
        
    except:
        tb = traceback.format_exc()
        print '==========vvv Segment: {} {}==========='.format(space, index)
        print tb
        print '==========^^^ Segment: {} {}==========='.format(space, index)
        pass

    
