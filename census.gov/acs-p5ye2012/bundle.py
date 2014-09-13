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
        
        self.database.create()
        
        config = dict(self.metadata.build.config)
        url = self.metadata.build.sources['table_map'].format(**config)
        
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
                    universe = reader.next()['Table Title']
                    if not universe.startswith('Universe:'):
                        raise Exception("Universe fail")
                    else:
                        parts = universe.split(':')
                        universe = parts[1].strip()
                
            
                    t = self.schema.add_table(
                        current_table,
                        description=row['Table Title'].title(),
                        universe = universe,
                        keywords = row['Subject Area'],
                        data = {
                            'segment':int(row['Sequence Number']),
                            'start': int(row['Start Position'])
                        }
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
                    ac(t,'FILEID',datatype='varchar',size=6, data=link_data,
                        description = 'Universe: {}'.format(universe))
                    ac(t,'FILETYPE',datatype='varchar',size=6, data=link_data)
                    ac(t,'STUSAB',datatype='varchar',size=2, data=link_data, indexes = is1)
                    ac(t,'CHARITER',datatype='varchar',size=3, data=link_data)
                    ac(t,'SEQUENCE',datatype='varchar',size=4, data=link_data)
                    ac(t,'LOGRECNO',datatype='integer',size=7, data=link_data, indexes = is1)

                    

                else:
                    #
                    # A row for an existing table. 
                    #
            
                    try:
                        int(row['Line Number'])
                    except:
                        print "Failed for ", row
                        
                    name = "{}{:03d}".format(current_table,int(row['Line Number']))
            
                    self.schema.add_column(
                        t, 
                        name, 
                        datatype = 'integer',
                        description = (row['Table Title']
                                    .decode('latin1')),
                        data = col_data
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
        geo = self.library.dep('geo-p5ye2012').partition
        
        d = {}
        for row in geo.query("""SELECT stusab, name 
                    FROM geofile WHERE sumlevel = 40 AND component = '00'"""):
                            
            d[row['stusab'].encode('utf-8')] = row['name'].encode('utf-8')
        
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
    def num_segments(self):
        '''Compute the number of segments. It is more efficiently retrieved from 
        self.metadata.build.config.segments'''
        return max([ c.data.get('segment',0) 
                     for t in self.schema.tables for c in t.columns  ])

    def build(self):
        
        if self.run_args.test:
            self.build_segment(3)
            
        elif int(self.run_args.get('multi')) > 1:
        
            segs = [seg for seg in range(1,self.metadata.build.config.segments)]
          
            
            self.run_mp(self.build_segment, segs)
            
        else:
            
            for seg in range(1,self.metadata.build.config.segments):

                self.build_segment(seg)
            
       
        return True
       

    def build_get_url(self, geo, stusab, segment):
        '''Return a URL for a segment file'''
        config = dict(self.metadata.build.config)
        
        state = self.states[stusab.upper()]
        
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


    def build_segment(self, seg_no):
        '''Create all of the tables for a segment. This will load both 
        geographies ( large and small ) and all of the states or one segment'''
        import csv
        import yaml
        from ambry.partitions import Partitions
        
        tables = self.table_map[seg_no]

        lr = self.init_log_rate(20000)
        
        raw_codes = []
        
        segment_header = []
        
        tables_parts = []
        
        ##
        ## Build a combined header for all of the tables in the
        ## segments, and while were at it, get handles to the table headers and partitions. 
        ##
        for table_name in tables:

            table_name = table_name.lower()
        
            table = self.schema.table(table_name)

            with self.session:
                table_header = [c.name for c in table.columns]
                
                if not segment_header: # Copy over link headings, but only once. 
                    for c in table.columns:
                        if  c.data['is_link']:
                            segment_header.append(c.name)
               
                # Copy all of the non link headings for every column.           
                for c in table.columns:
                    if not c.data['is_link'] and not c.is_primary_key:
                        segment_header.append(c.name) 
  
            # We need to convert both the measures and errors files. These have
            # the same structure, but have different prefixes for the file name
            mp = self.partitions.find_or_new(table=table_name, grain='margins')
            ep = self.partitions.find_or_new(table=table_name, grain='estimates')
        
            if (mp.state == Partitions.STATE.BUILT 
                and ep.state == Partitions.STATE.BUILT and not self.run_args.test):
                self.log("Partition {} is already built".format(mp.identity.sname))
                continue 
    
            mp.clean()
            ep.clean()
            
            tables_parts.append((table_name, table_header, mp, ep))

        ##
        ## Now we can iterate over everything and collect the table data from the segments. 
        ##

        row_num = 1
        for table_name, table_header, mp, ep in tables_parts:

            for stusab, state in self.states.items():
                        
                if self.run_args.test and stusab != 'CA':
                    continue

                for geo in ('large','small'):
                    url = self.build_get_url(geo,stusab, seg_no)
             
                    mfn, efn = self.download(url)
                
                    self.log("=== Building: seg={} {} {} {}".format(seg_no, state, geo, table_name))
                    self.log("       URL: {}".format(url))
                    self.log("  measures: {}".format(mfn))
                    self.log("    errors: {}".format(efn))
                
                    for fn,p in [ (efn,ep), (mfn, mp)]:

                        with open(fn) as f:
                            reader = csv.reader(f)                        
            
                            with p.inserter() as ins:
                                for line in reader:
                                    lr("{} {} {} {}".format(table_name, stusab, geo, p.identity.grain))
                                
                                    assert len(segment_header) == len(line)
                                
                                    # Headers applied to the full line. 
                                    full_d = dict(zip(segment_header, line))
                                    
                                    # Just the records for this row. 
                                    d = { k:v for k,v in full_d.items() if k in table_header }

                                    #print d
                                    d['id'] = row_num

                                    errors =  ins.insert(d)

                                    row_num += 1
                            
                                    if errors:
                                        raw_codes.append((line[:6], errors, geo,table_name))
                                        
                                    
                        p.close()


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

                                    
