'''
Example bundle that builds a single partition with a table of random numbers
'''

from ambry.bundle import BuildBundle
from ambry.util import memoize

class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    ##
    ## Meta
    ##

    def meta(self):
        
        self.meta_schema()
        self.meta_states()
        
        return True
       

    def meta_schema(self):
        from csv import DictReader
        from collections import defaultdict
        import yaml
 
        
        # We're using the CensusReporter metadata because it is hard to load the TableShell
        # excell files. I think it requires a later version of xlrd to read the indent level.
        # See https://github.com/censusreporter/census-table-metadata/blob/master/process_merge.py, line 85 or so. 
        col_meta = self.filesystem.read_csv(self.source('column_meta'), 'column_id')
      
        table_meta = self.filesystem.read_csv(self.source('table_meta'), 'table_id')
        
        self.database.create()
        
        config = dict(self.metadata.build.config)
        url = self.metadata.build.sources['table_map'].format(**config)
        
        self.log("Loading table map from {} ".format(url))
        
        tn = self.filesystem.download(url)
        
        current_table = None
        t = None
        
        # These tables spread across more than one segment, 
        # which is a difficult special case, so these tables
        # are re-named to have the segment number as a suffix. 
        large_tables = ['B24121', 'B24122', 'B24123',
                        'B24124', 'B24125', 'B24126']
        
        table_segments = defaultdict(list)
        
        lr = self.init_log_rate(1000)
        indent = 0
        with self.session, open(tn) as f:
            reader = DictReader(f)
        
            for i, row in enumerate(reader):

                if self.run_args.test and i > 500:
                    break
        
                if row['Table ID'] in large_tables:
                    row['Table ID'] = (row['Table ID'] + 
                    '_' + str(int(row['Sequence Number'])))
            
                #### These are gouping lines that have no data
                #### associated with them. 
                if row['Line Number'].endswith('.5'):
                    continue
            
                col_data = {
                    'segment':int(row['Sequence Number'])
                }
            
                if row['Table ID'] != current_table:
                    #
                    # A New Table
                    #
                    new_table = True
                    current_table = row['Table ID']

                    # The row after the table is the universe
                    # Not using this right now -- gettting it from the table_meta
                    universe = reader.next()['Table Title']
                    
                    if not universe.startswith('Universe:'):
                        raise Exception("Universe fail")
                    else:
                        parts = universe.split(':')
                        universe = parts[1].strip()

                    try:
                        keywords = ','.join(yaml.load(table_meta.get(current_table.upper(),{}).get('topics',None)))
                    except Exception as e:
                        keywords = None

                    t = self.schema.add_table(
                        current_table,
                        description=row['Table Title'].title(),
                        universe = table_meta.get(current_table.upper(),{}).get('universe',None),
                        keywords = keywords,
                        data = {
                            'segment':int(row['Sequence Number']),
                            'start': int(row['Start Position']),
                            'length': int(row['Total Cells in Table'].split(' ')[0]),
                            'denominator': table_meta.get(current_table.upper(),{}).get('denominator_column_id',None),
                            'subject': row['Subject Area']
                        },
                        fast = self.run_args.get('fast', False)
                    )

                    if not current_table in table_segments[row['Sequence Number']]:
                        (table_segments[int(row['Sequence Number'])]
                                        .append(current_table))
                    
                    ac = self.schema.add_column
                    
                    is1 = 'i1'
                
                    # Flag to mark which columns should be removed from the table when constructing
                    # a segment header. 
                    link_data = dict(col_data.items())
                    link_data['is_link'] = 1
                
                    ac(t,'id',datatype='integer',is_primary_key = True,
                        description=row['Table Title'].title())
                    #ac(t,'FILEID',datatype='varchar',size=6, data=link_data,
                    #    description = 'Universe: {}'.format(universe))
                    #ac(t,'FILETYPE',datatype='varchar',size=6, data=link_data)
                    ac(t,'STUSAB',datatype='varchar',size=2, data=link_data, indexes = is1, fk_vid='c03q01005')
                    #ac(t,'CHARITER',datatype='varchar',size=3, data=link_data)
                    #ac(t,'SEQUENCE',datatype='varchar',size=4, data=link_data)
                    ac(t,'LOGRECNO',datatype='integer',size=7, data=link_data, indexes = is1, fk_vid='c03q01008')
                    ac(t,'geofile_id',datatype='integer', data=link_data, indexes = 'i2', fk_vid = 't03q01')
                    ac(t,'gvid',datatype='varchar', data=link_data, indexes = 'i3', proto_vid = 'c00104002')
  
                    indent = 0
  
                else:
                    #
                    # A row for an existing table. 
                    #
            
                    try:
                        int(row['Line Number'])
                    except:
                        print "Failed for ", row
                        
                    name = "{}{:03d}".format(current_table,int(row['Line Number']))
            
                    title = row['Table Title'].decode('latin1')
                
            
                    # The estimate value
                    c = self.schema.add_column(t, name, datatype = 'integer',
                        description = title,
                        data = {'is_estimate':1,
                                'parent': col_meta.get(name.upper(),{}).get('parent_column_id',None),
                                'indent': col_meta.get(name.upper(),{}).get('indent',None)
                                },
                        fast = self.run_args.get('fast', False)
                    )
                    
                    # Then the margin
                    self.schema.add_column(t, name+"_m", datatype = 'integer',
                        description = ("Margins for: "+title),
                        data = {'margin_for': c.id_},
                        fast = self.run_args.get('fast', False)
                    )
                    
                        
                lr("Creating schema: {}".format(t.name))
                last_table =  row['Table ID']
                new_table = False


        with open(self.filesystem.path('meta','tables.yaml'), 'w') as f:
            f.write(yaml.dump(dict(table_segments), indent=4, default_flow_style=False))

        with open(self.filesystem.path('meta',self.SCHEMA_FILE), 'w') as f:
            self.schema.as_csv(f)
        
        return True
        

        
    def meta_states(self):
        import yaml
        geo = self.library.dep('geo-p5ye2013').partition
        
        d = {}
        for row in geo.query("""SELECT stusab, name 
                    FROM geofile WHERE sumlevel = 40 AND component = '00'"""):
                            
            d[row['stusab'].encode('utf-8')] = row['name'].encode('utf-8')
        
        d['us'] = 'United States'
        
        with open(self.filesystem.path('meta','states.yaml'), 'w') as f:
            f.write(yaml.dump(d, indent=4, default_flow_style=False))
            
    @property
    @memoize
    def states(self):
        '''Maps state appreviations to state names'''
        import yaml

        with open(self.filesystem.path('meta','states.yaml')) as f:
            return yaml.load(f)
    
    @property
    @memoize
    def table_map(self):
        '''Maps segments to tables'''
        import yaml

        with open(self.filesystem.path('meta','tables.yaml')) as f:
            return yaml.load(f)
        

    @property
    @memoize
    def num_segments(self):
        '''Compute the number of segments. '''
        return max([ int(c.data.get('segment',0) if c.data.get('segment',0) else 0) 
                    for t in self.schema.tables for c in t.columns  ])

    def build(self):
     

        self.id_map() # Make sure it exists before going MP

        if self.run_args.test:
            segments = [2,5,8]
        else:
            segments = range(1,self.num_segments+1)
        

        if int(self.run_args.get('multi')) > 1: 
            self.run_mp(self.build_segment,segments)
    
        else:
            seg_tables = [ (seg, table) for seg in segments for table in self.table_map[seg]] 
            
            for seg, table in seg_tables:
                self.build_segment_tables(seg, table)
    
        return True
       

    def build_get_url(self, geo, stusab, segment):
        '''Return a URL for a segment file'''
        config = dict(self.metadata.build.config)

        state = self.states[stusab.lower()]
        
        t = self.metadata.build.templates.root+self.metadata.build.templates[geo]
        
        url = t.format(
            seg_4 = '{:04d}'.format(segment), 
            lc_utstab = stusab.lower(), 
            state=state.title().replace(' ',''), 
            **config
        )
        
        return url

    def download(self, url):
        '''Download with a single retry. When one process in a multi process
        run throws an error or is canceled, downloads in other processes may 
        be left incomplete. This will clean that up.  '''
        from zipfile import BadZipfile
        import os
        import re
        
        ## The M's and E's might look backwards, but they are not
        ## E == Estimate
        ## M == Margin of Error
        
        zf = None
        try:
            zf = self.filesystem.download(url)
            efn = self.filesystem.unzip(zf,r'^e.*') #  ( "e" for Estimate )
            mfn = self.filesystem.unzip(zf,r'^m.*') # ( "m" for margin or error )
            
        except BadZipfile:
            if zf:
                self.error("Error in {}. Delete and re-download".format(zf))
                os.remove(zf)
                zf = self.filesystem.download(url)
                mfn = self.filesystem.unzip(zf,r'^e.*')
                efn = self.filesystem.unzip(zf,r'^m.*') 
                
        return mfn, efn

    def id_map(self):
        import pickle
        import os.path
        
        idm_path = self.filesystem.build_path('id_map.pkl')
    
        if not os.path.exists(idm_path):

            geo = self.library.dep('geo-p5ye2013').partition
            self.log('Writing id map')
            id_map = { (row['stusab'].lower(), row['logrecno']) : (row['id'], row['gvid']) for  row in geo.rows }
            
            with open(idm_path,'wb') as f:
                pickle.dump(id_map, f)
                
            return id_map

        else:
            with open(idm_path) as f:
                return pickle.load(f)

    def build_segment(self, seg_no):
        
        for table in self.table_map[seg_no]:
            self.build_segment_tables(seg_no, table)
        

    def build_segment_tables(self, seg_no, table_name):
        '''Create all of the tables for a segment. This will load both 
        geographies ( large and small ) and all of the states or one segment'''
        import csv
        import yaml
        from itertools import izip
        from ambry.partitions import Partitions
        
        if int(self.run_args.get('multi')) > 1: 
            lr = self.init_log_rate(20000)
        
        raw_codes = []
        
        segment_header = []
        
        tables_parts = []

        table_name = table_name.lower()

        segment_header =  ['fileid','filetype','stusab','chariter','sequence','logrecno']

        if False and self.run_args.test:
            id_map = {}
        else:
            id_map = self.id_map()

        with self.session:

            table = self.schema.table(table_name)
            start_pos = int(table.data['start'])  - 1
            col_length = int(table.data['length'])

            for c in table.columns:
                if not c.data['is_link'] and not c.is_primary_key and not c.name.endswith('_m'):
                    segment_header.append(c.name) 

            p = self.partitions.find_or_new(table=table_name)
            
            if p.is_finalized:
                self.log("Partition is already finalized: {}".format(p.identity))
                return 
            
            p.clean()

            # Entering the intserter sets the build state of the partition in the dataset, 
            # and the dataset is shared, so this needs to be in the session to be safe. 
            ins = p.inserter()

            table_header = [ c.name for c in table.columns ]

        row_num = 1
        for stusab, state in self.states.items():
                    
            if self.run_args.test and row_num > 1000:
                break
                    
            for geo in ('large','small'):
                
                url = self.build_get_url(geo,stusab, seg_no)
         
                #self.log("=== Building: seg={} {} {} {} {}->{}".format(seg_no, state, geo, table_name, start_pos, start_pos+col_length))
                #self.log("       URL: {}".format(url))
                
                mfn, efn = self.download(url)
            
                #self.log("  measures: {}".format(mfn))
                #self.log("    errors: {}".format(efn))
            
                
                with open(mfn) as mf, open(efn) as ef:
                    m_reader = csv.reader(mf)  
                    e_reader = csv.reader(ef)                    
    
                    for e_line, m_line in izip(e_reader, m_reader):
    
                        stusab, logrecno = e_line[2], e_line[5]
    
                        assert len(e_line) == len(m_line)
                        assert e_line[2] == m_line[2] # stusab
                        assert e_line[5] == m_line[5] # logrecno
    
                        fk_id, gvid = id_map.get( (stusab.lower(), int(logrecno)), (None, None) )
    
                        s = start_pos
                        e = start_pos+col_length
    
                        row = ([None]*5) + [ val for pair in  zip(e_line[s:e], m_line[s:e])  for val in pair]

                        assert len(row) == len(table_header), " {} != {}".format(row, table_header)
                        assert len(row) == col_length*2+5

                        lr("{} {} {} {}".format(table_name, stusab, geo, p.identity))

                        # Just the records for this row. 
                        d = dict(zip(table_header, row))

                        #print d
                        d['id'] = row_num

                        d['geofile_id'] = fk_id
                        d['gvid'] = gvid
                        d['stusab'] = stusab
                        d['logrecno'] = logrecno
    

                        row_num += 1

                        errors =  ins.insert(d)

                        if errors:
                            raw_codes.append(( (stusab.lower(), int(logrecno)), errors, geo, table_name))
             
        with self.session: 
            ins.close()                                               
            p.close()
            p.finalize()
            
        # Write out the coumn names in each table, segment, that the Caster
        # could not translate. These should get processed into meta information
        # that will add 'code' columns into tables to hold the orig values of
        # the Jam Codes
        code_cols = {}
        for link, errors, geo, table in raw_codes:
            ld = dict(zip(segment_header[:6], link))
            del ld['fileid']
            del ld['filetype'] # Should aways be 2012e5
            ld['geo'] = geo
            ld['table'] = table
            
            if not table in code_cols:
                code_cols[table] = []
                
            for k,v in errors.items():
                ld['col'] = k
                ld['value'] = v
                
                if k not in code_cols[str(table)]:
                    code_cols[table].append(str(k))

        if len(code_cols):
            with open(self.filesystem.path('build','code','codes-{}.yaml'.format(seg_no)), 'w') as f:
                f.write(yaml.dump(code_cols, indent=4, default_flow_style=False))

                                    
