'''

@author: eric
'''
from  ambry.bundle import BuildBundle


import os.path
import yaml
 
class Bundle(BuildBundle):
    '''
    Bundle code for US 2010 Census, Summary File 1
    '''

    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        
        self._table_id_cache = {}
        self._states = None
        self._urls_cache = None
        self._segments_cache = None
    
    ############
    # Meta
    ##########

    def meta(self):
        self.meta_scrape_urls()
        self.meta_read_packing_list()
        self.meta_create_schema()
        self.meta_build_states()

        return True

    def meta_scrape_urls(self):
        '''Extract all of the URLS from the Census website and store them.'''
        
        import urllib
        import urlparse
        import yaml
        import re
        from bs4 import BeautifulSoup


        b = self.library.dep('states')
            
        states = {}
        for row in b.partition.query("SELECT DISTINCT  stusab, name  FROM geofile WHERE fileid = 'SF1ST' "):
            states[row[0].lower()] = row[1]


        state_names = [ s.replace(' ','_') for s in states.values()]


        rootUrl = self.metadata.build.rootUrl
    
        log = self.log
        tick = self.ptick

        # Root URL for downloading files. 
       
        doc = urllib.urlretrieve(rootUrl)
        
        log('Getting URLS from '+rootUrl)
        # Get all of the links
        log('S = state, T = segment table, g = geo')
        urls = {}

        with open(doc[0]) as bsf:
            for link in BeautifulSoup(bsf).find_all('a'):

                if not link.get('href') or not link.string or not link.contents:
                    continue;# Didn't get a sensible link
                # Only descend into links that name a state
                
                tick('S')
                
                state = link.get('href').strip('/')
    
                if link.string and link.contents[0] and state in state_names :
                    stateUrl = urlparse.urljoin(rootUrl, link.get('href'))
                    stateIndex = urllib.urlretrieve(stateUrl)
                    # Get all of the zip files in the directory
                    
                    for link in  BeautifulSoup(open(stateIndex[0])).find_all('a'):
                        
                        if link.get('href') and  '.zip' in link.get('href'):
                            final_url = urlparse.urljoin(stateUrl, link.get('href')).encode('ascii', 'ignore')
                       
                            tick('T')
                            
                            m = re.match('.*/(\w{2})2010.sf1.zip', final_url)
        
                            if  m:
                                urls[m.group(1)] = str(final_url)
                            else:
                                raise Exception("Regex failed for : "+final_url)

        tick('\n')

        urls_file =  self.filesystem.path(self.metadata.build.urlsFile)

        with open(urls_file,'wb') as out_f:
            yaml.dump(urls, out_f,default_flow_style=False)
       
        return True
 
    def summary_files(self,state):
        '''Download and unpack the summary files in a directory. Will cache the files, so 
        it can be called multiple times'''
        from zipfile import BadZipfile
        
        url = self.urls[state]

        state_file = self.filesystem.download(url)
        
        out = set()
        
        try:
            files = self.filesystem.unzip_dir(state_file)
            for file in files:
                out.add(file)
                pass ; # Need to iterate to unpack all of the files. 
        except BadZipfile:
            self.error("Caught error; re-downloading")
            state_file = self.filesystem.download(url)
            files = self.filesystem.unzip_dir(state_file)
            for file in files:
                out.add(file)
                pass ; # Need to iterate to unpack all of the files. 
        
        return out
    
    def meta_read_packing_list(self):
        '''The packing list is a file, in every state extract directory, 
        that has a section that describes how the tables are packed into segments.
        it appears to be the same for every state'''
        import re
    
        # Descend into the first extract directory. The part of the packing list
        # we need is the same for every state. 
      
        files = self.summary_files('ri')
        
        for f in files:
            if f.endswith("2010.sf1.prd.packinglist.txt"):
                pack_list = f
                break
             
                
        segments = {}        
        with open(pack_list) as f:
            for line in f:
                
                if re.search('^\w+\d+\w*\|', line):
                    parts = line.strip().split('|')
                    segment, length = parts[1].split(':')
                    table = parts[0]
                    segment = int(segment)
                    length = int(length)
                    
                    if not segment in segments:
                        segments[segment] = []
                    
                    segments[segment].append([table, length])

        packing_file =  self.filesystem.path(self.metadata.build.packingFile)

        with open(packing_file,'wb') as out_f:
            yaml.dump(segments, out_f,default_flow_style=False)
  

    def meta_generate_schema_rows(self):
        '''This generator yields schema rows from the schema definition
        files. This one is specific to the files produced by dumping the Access97
        shell for the 2010 census '''
        import csv
        
        headers_file = self.metadata.build.headersFile
        
        with open(headers_file, 'rbU') as rf:
            reader  = csv.DictReader(rf)
            last_seg = None
            table = None
            for row in reader:
                if not row['TABLE NUMBER']:
                    continue
                
                if row['SEGMENT'] and row['SEGMENT'] != last_seg:
                    last_seg = row['SEGMENT']
                
                # The first two rows for the table give information about the title
                # and population universe, but don't have any column info. 
                if( not row['FIELD CODE']):
                    if  row['FIELD NAME'].startswith('Universe:'):
                        table['universe'] = row['FIELD NAME'].replace('Universe:','').strip()  
                    else:
                        table = {'type': 'table', 
                                 'name':row['TABLE NUMBER'],
                                 'description':row['FIELD NAME'],
                                 'segment':row['SEGMENT'],
                                 'data':  {'segment':row['SEGMENT'], 'fact':True}
                                 }
                else:
                    
                    # The whole table will exist in one segment ( file number ) 
                    # but the segment id is not included on the same lines ast the
                    # table name. 
                    if table:
                        yield table
                        table  = None
                        
                    col_pos = int(row['FIELD CODE'][-3:])
                    
                    yield {
                           'type':'column','name':row['FIELD CODE'], 
                           'description':row['FIELD NAME'].strip(),
                           'segment':int(row['SEGMENT']),
                           'col_pos':col_pos,
                           'decimal':int(row['DECIMAL'] if row['DECIMAL'] else 0)
                           }
         
    def meta_create_schema(self):
        '''Uses the generate_schema_rows() generator to creeate rows for the fact table
        The geo split table is created in '''
        from ambry.orm import Column
        from collections import OrderedDict
     
        log = self.log
        tick = self.ptick
        
        log("Generating main table schemas")
    
        table = None
        s_rows = []
        blank_row = OrderedDict((('table', None),('column', None),('is_pk', None),('i1', None),
                                ('is_fk', None), ('type', None), ('size', None),
                                ('description', None),
                                ('d_segment',  None)
                                ))

        def make_row(**kwargs):
            r = dict(blank_row)
            for k,v in kwargs.items():
                r[k] = v
                
            return r
        
        for row in self.generate_schema_rows():
    
            if row['type'] == 'table':
                
                table  = row['name'].lower()
                
                # First 5 fields for every record      
                # FILEID           Text (6),  uSF1, USF2, etc. 
                # STUSAB           Text (2),  state/U.S. abbreviation
                # CHARITER         Text (3),  characteristic iteration, a code for race / ethic group
                #                             Prob only applies to SF2. 
                # CIFSN            Text (2),  characteristic iteration file sequence number
                #                             The number of the segment file             
                # LOGRECNO         Text (7),  Logical Record Number
    
                s_rows.append(make_row(table=table, column = 'id', type = 'INTEGER', size = 6, is_pk = 1))
                s_rows.append(make_row(table=table, column = 'stusab', type = 'VARCHAR', size = 6, i1=1))
                s_rows.append(make_row(table=table, column = 'logrecno', type = 'VARCHAR', size = 7, i1=1))

            else:
    
                if row['decimal'] and int(row['decimal']) > 0:
                    dt = 'REAL'
                else:
                    dt = 'INTEGER'
           
                s_rows.append(make_row(table=table, column = row['name'].lower(), type = dt,description=row['description'] ))
           
    
        fn = self.filesystem.path('meta','schema.csv')
        with open(fn,'wb') as f:
            import csv
            w = csv.DictWriter(f, blank_row.keys())
            w.writeheader()
            w.writerows(s_rows)
            
    
        tick("\n")
        

    def meta_build_states(self):
        p = self.library.dep('states').partition
            
        states = {}
        for row in p.query("SELECT DISTINCT  stusab, state  FROM geofile WHERE fileid = 'SF1ST' "):
            states[str(row[0].upper())] = int(row[1])       
        

        states['US'] = 0

        states_file =  self.filesystem.path(self.metadata.build.statesFile)

        with open(states_file,'wb') as out_f:
            yaml.dump(states, out_f,default_flow_style=False)
    

    #################
    # Prepare
    #################
  
 
  
    def prepare(self):
        import threadpool
        import multiprocessing

        if not self.database.exists():
            self.database.create()

        for state in self.states.keys():
            self.log("Pre-downloading for {}".format(state))
            files = self.summary_files(state.lower())

        return True
 

    #################
    # Build
    #################


    @property
    def urls(self):

        if self._urls_cache is None:
            with open(self.metadata.build.urlsFile, 'r') as f:
                self._urls_cache =  yaml.load(f) 

        return self._urls_cache
  
    @property
    def states(self):

        if self._states is None:
            with open(self.metadata.build.statesFile, 'r') as f:
                self._states =  yaml.load(f) 
                
            if self.run_args.test:
                self._states = dict(self._states.items()[0:4])

        return self._states
  
    @property
    def segments(self):

        if self._segments_cache is None:
            with open(self.metadata.build.packingFile, 'r') as f:
                self._segments_cache  =  yaml.load(f) 
 
        return self._segments_cache  
  
    def seg_spec(self, segment):
        '''Return the list of files and offsets for a segment, across all of the states. '''
        import re 

        states = self.urls.keys()
        #states = ['in','nm','ia','ga']
        
        files = []
        for state in states:
            for file in self.summary_files(state):
                if re.match(r'.*/{:s}{:05d}2010.sf1'.format(state, segment), str(file)):
                    files.append((state, segment, file))
             
        start = 0  
        tables = {}     
        for table, length in self.segments[segment]:

            tables[table] = (start, length)
            start += length
        

        return files, tables


    @staticmethod
    def georecid(releaseid, stateid, logrecno):
        return ((((int(releaseid) * 10**2)
                  + int(stateid)) * 10**7) 
                  + int(logrecno))

    def build(self):
        self.build_segments()
        self.csv_load()
        return True


    def build_segments(self):
        """ Build the bundle. If the -m/--multi argument is specified
        the build will run on the specified number of processors. """
        from multiprocessing import Pool

        n = self.run_args.multi if self.run_args.multi else 1


        if self.run_args.test:
            segment_numbers = [
                1, # P1
                2, # P2
                42, # H1
                43, # H2
                ]
   
        else:
            segment_numbers =[
            1, # P1
            2, # P2
            3, # P3-P9
            4, # P10-14
            5, # P15 - P30
            6, # P31-P49
            12, #P34F -P38E
            17, #PCT12 -
            42, # H1
            43, # H2
            44 #H3 - H11F
            ]

        segments = [ segment for i, segment 
                        in enumerate(self.segments.keys()) 
                        if i+1 in segment_numbers ]

        if n > len(segments):
            self.log("Reduced number of processes to number of segments: {}".format(len(segments)))
            n = len(segments)
            

        if n == 1:
            for i,segment in enumerate(segments):
                self.build_segment(segment) 
        else:

            pool = Pool(n, maxtasksperchild=1)
    
            r = pool.map(run, [ (self.bundle_dir, s) for s in segments])


        return True


    def build_segment(self, segment):
        '''Load tables into CSV segment files'''
        import csv 

        self.log("Run segment: {}".format(segment))
        files, tables = self.seg_spec(segment)
  
        partitions = {}
        
        releaseid = 3601
        
        states = self.states
        
        lr = self.init_log_rate(100000)

        for table, (start, length) in tables.items():
            
            p = self.partitions.find_or_new(table=table, clean=True)

            # NOTE There aer two "segments" here
            # The census data is divided into segments, each of which has one
            # file per state, and one or more tables per file set. 
            # Databundle CSV files are also divided into segments, but with 
            # different structure; each as a sunset of the rows for a table. 
            if self.run_args.test:
                rows = p.optimal_rows_per_segment(50000)
            else:
                rows = p.optimal_rows_per_segment(200*1024*1024)
                
            ins =  p.database.csvinserter(table, segment_rows= rows)

            partitions[table] = (p,ins)
            self.log("Create partition: {}, {} rows".format(p.identity.name, rows))

        for (state, segment,file_) in files:
 
            state = state.upper()
            if state not in states:
                continue
            else:
                self.log("Processing: {} ".format(file_))
            
            with open(file_, 'rbU', buffering=100*1024*1024) as f:
                
                for i,row in enumerate(csv.reader( f )):
                    header = row[0:5]
                    for table, (start, length) in tables.items():
                        try:

                            georecid = self.georecid(releaseid, 
                                             int(states[row[1]]) if row[1] else 0,
                                             int(row[4]))

                            ins_row = [georecid]+[row[1]]+[int(row[4])]+ row[start+5:start+length+5]
                            
                            partitions[table][1].insert(ins_row)
                        except Exception as e:
                            self.log("Error on {}".format(row))
                            raise
                        
                    lr("Segment: {}".format(segment))
                    
                    if self.run_args.test and i >= 100000:
                        self.log('Breaking, for test')
                        break
  
        for table, (p, ins) in partitions.items():
            self.log("Closing partition: {}".format(p.identity.name))
            ins.close()
            
    def _csv_load(self, pid):

        with self.session:
            p = self.partitions.get(pid)
            parts = p.get_csv_parts()
            table_name = p.table.name

        self.log("CSV Loading: {}".format(p.identity.name))
        p.load_csv(table=table_name,parts=parts)
        
    def csv_load(self):
        
        from multiprocessing import Pool

        n = self.run_args.multi if self.run_args.multi else 1

        with self.session:
            partitions = self.partitions.all

        if n > len(partitions):
            self.log("Reduced number of processes to number of partitions: {}".format(len(partitions)))
            n = len(partitions)

        if n == 1:
      
            for p in partitions:   
                # The 'db' format partition is sort of the 'head' of
                # a set of CSV partitions. We'll get the CSV partitions
                # thorugh the db partition. 
                if p.identity.format == 'db':
                    p.query("DELETE FROM {}".format(p.identity.table))
                    self._csv_load(p.identity.id_)

        else:

            pool = Pool(n, maxtasksperchild=1)
    
            r = pool.map(run_cvs_load, 
            [ (self.bundle_dir, p.identity.name, p.identity.id_) 
            for p in partitions if p.identity.format == 'db'])

        
        return True
    

        



    