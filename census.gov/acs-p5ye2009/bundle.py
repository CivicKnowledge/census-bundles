 # -*- coding: utf-8 -*-
import ambry.bundle 
from ambry.util import memoize


class TableRowGenerator(object):
    """Generate table rows by combining multuple state files, slicing out 
    an individual table, and merging the estimates and margins"""
    
    def __init__(self, bundle, source):
        self.source = source
        self.bundle = bundle
        self.library = self.bundle.library
        self.year = int(self.bundle.year)
        self.release = int(self.bundle.release)
        self.header_cols = self.bundle.header_cols
        self.states = self.bundle.states
        self.url_root = self.bundle.source('base_url_5').ref
        self.small_url_template = self.bundle.source('small_area_url_5').ref
        self.large_url_template = self.bundle.source('large_area_url_5').ref
        
        self.test = self.bundle.test
        
    def __iter__(self):
        
        from ambry_sources import SourceSpec, get_source
        from ambry.etl import Slice
        from itertools import izip, chain
        
        cache = self.library.download_cache
        
        table = self.source.dest_table
        
        if isinstance(table, str):
            table = self.table(table)
        
        table_name = table.name
       
        start = int(table.data['start'])
        length = int(table.data['length'])
        sequence = int(table.data['sequence'])
        
        slca_str = ','.join(str(e[4]) for e in self.header_cols)
        slcb_str =  "{}:{}".format(start-1, start+length-1)
        
        # Slice for the stusab, logrecno, etc. 
        slca, slc_code = Slice.make_slicer(slca_str)
        # Slice for the data columns
        slcb, slc_code = Slice.make_slicer(slcb_str)
        
        columns = [ c.name for c in table.columns ]

        # Columns before the first data column, by removing the
        # data columns, which are presumed to all be at the end. 
        preamble_cols = columns[:-2*len(slcb(range(1,300)))]
        data_columns =  columns[len(preamble_cols):]
        
        header_cols = [e[0] for e in self.header_cols]
        
        assert preamble_cols[-1] == 'jam_flags'
        assert data_columns[0][-3:] == '001'
        assert data_columns[1][-3:] == 'm90'

        yield header_cols + data_columns
        
        row_n = 0
        for stusab, state_id, state_name in self.states:
            
            file = "{}{}{}{:04d}000.txt".format(self.year,self.release,
                         stusab.lower(),sequence)
            
            for (size, url_template) in [('s', self.small_url_template), 
                                         ('l',self.large_url_template)]:
                
                url = url_template.format(root=self.url_root, 
                                          state_name=state_name).replace(' ','')
                      
                spec = SourceSpec(
                    url = url,
                    filetype = 'csv', 
                    reftype = 'zip',
                    file = 'e'+file
                )
                
                s1 = get_source(spec, cache)
                  
                spec.file = 'm'+file
                s2 = get_source(spec, cache)
                      

                for i, (row1, row2) in enumerate(izip(s1, s1)):
                    # Interleave the slices of the of the data rows, prepend
                    # the stusab, logrecno, etc. 
                    
                    row_n += 1
                    if self.test and row_n > 10000:
                        return
                    
                    yield slca(row1)+tuple(chain(*zip(slcb(row1),slcb(row2))))
                    
               

class Bundle(ambry.bundle.Bundle):

    _states = None

    # Which of the first columns in the data tavbles to use. 
    header_cols = [
        # Column name, Description, width, datatype, column position
        #('FILEID','File Identification',6,'str' ),
        #('FILETYPE','File Type',6,'str'),
        ('STUSAB','State/U.S.-Abbreviation (USPS)',2,'str',2 ),
        ('CHARITER','Character Iteration',3,'str',3 ),
        ('SEQUENCE','Sequence Number',4,'int',4 ),
        ('LOGRECNO','Logical Record Number',7,'int',5 )
    ]

    def init(self):
        import isodate

        r,y = self.identity.btime.upper().split('E')

        self.release = int(isodate.parse_duration(r).years)
        self.year = int(isodate.parse_date(y).year)
      
        
    jam_map = {
        '.': 'm', # Missing or suppressed value
        ' ': 'g',
        None: 'N',
        '': 'N'
    }
    
    def jam_float(self,v,errors, row):
        """Convert jam values into a code in the jam_values field and write a None"""
        from ambry.valuetype.types import nullify
        v = nullify(v)
        
        try:
            return float(v)
        except:
            if not 'jams' in errors:
                errors['jams'] = ''
                
            try:
                errors['jams'] += self.jam_map[v]
            except KeyError:
                self.error(row)
                raise
                
            return None
        
     
    def jam_values(self, errors, row):
        """Write the collected jam codes to the jam_value field."""
        from itertools import chain, groupby
        
        jams =  errors.get('jams')
        
        def rle(s):
            "Run-length encoded"
            return ''.join(str(e) for e in chain(*[(len(list(g)), k) 
                                            for k,g in groupby(s)]))
        
       
        return rle(jams) if jams else None
        
    def join_geoid(self, row):
        """Add a geoid to the row, from the geofile partition, linked via the
        state abbreviation and logrecno"""
        return self.geofile[(row.stusab.upper(), int(row.logrecno))][0]
        
        
    @property
    @memoize
    def states(self):
        """Return tuples of states, which can be used to make maps and lists"""
        
        if not self._states:
        
            self._states = []
        
            with self.dep('states').datafile.reader as r:
                for row in r.select( lambda r: r['component'] == '00'):
                    self._states.append((row['stusab'], row['state'], row['name'] ))
                    
        return self._states
      
    @property
    @memoize
    def geofile(self):
        with self.dep('geofile').datafile.reader as r:
            return { (row.stusab, row.logrecno): (row.geoid, row.sumlevel)  
                     for row in r }
    
        
    ##
    ## Create Tables
    ##
        
    def schema(self, sources = None, tables=None, stage = 1, clean=False):
        
        self.log("Deleteting old tables and partitions")
        self.dataset.delete_tables_partitions()
        
        self.commit()

        tables = self.tables_list()
        
        self.log("Creating {} tables".format(len(tables)))
        
        lr = self.init_log_rate(100)
        
        for i, table_id in enumerate(sorted(tables.keys())):

            d = tables[table_id]
            
            lr(table_id)
            
            t = self.make_table( **d )
            
            self.make_source(t)
            
        self.commit()
        
        self.sync_out()


    def make_table(self,  name, universe, description, columns, data):
        """Meta-phase routine to create a single table, called from 
        create_tables"""
        t = self.new_table(name, description = description.title(), 
                            universe = universe.title(), data = data)
        
        for name, desc, size, dt, pos in self.header_cols:
            t.add_column(name, 
                         description = desc, 
                         size = size,
                         datatype = dt,
                         data=dict(start=pos))
           
        # NOTE! All of these added columns must also appear in the
        # Add pipe in the bundle.yaml metadata
        t.add_column(name='geoid', datatype='census.AcsGeoid', 
              description='Geoid from geofile', transform='^join_geoid')
           
        t.add_column(name='gvid', datatype='census.GVid', 
              description='GVid from geoid', transform='||row.geoid.gvid')
              
        t.add_column(name='sumlevel', datatype='int', 
              description='Summary Level', transform='||row.geoid.sl')
            
        t.add_column(name='jam_flags', datatype='str', transform='^jam_values',
              description='Flags for converted Jam values')
           
           
        seen = set() # Mostly for catching errors. 
           
           
        for col in columns:
            if col['name'] in seen:
                print col['name'],  "already in name;", seen
                raise Exception()
                
            t.add_column( name=col['name'], 
                          description=col['description'],
                          transform='^jam_float', 
                          datatype='float',
                          data=col['data'])
            
            seen.add(col['name'])
            
            
        return t
            
    def tables_list(self,  add_columns = True):
        from collections import defaultdict
        from ambry.orm.source import DataSource
        from ambry.util import init_log_rate
        
        def prt(v): print v
        
        lr = init_log_rate(prt)

        tables = defaultdict(lambda: dict(table=None, universe = None, columns = []))

        year = self.year
        release = self.release

        table_id = None
        seen = set()
        ignore = set()
        
        #name, universe, description, columns
        
        i = 0

        with self.dep('table_sequence').datafile.reader as r:
            for row in r:
                

                if int(row['year']) != int(year) or int(row['release']) != int(release):
                    #print "Ignore {} {} != {} {} ".format(row['year'], row['release'], year, release)
                    continue
    
                if row['table_id'] in ignore:
                    continue

                if int(row['sequence_number'] ) > 117:
                    # Not sure where the higher sequence numbers are, but they aren't in this distribution. 
                    continue
                    
                i += 1

                table_name = row['table_id']

                if row['start']:
        
                    # Breaking here ensures we've loaded all of the columns for
                    # the previous tables. 
                    if self.test and i > 1000:
                        break
        
                    if table_name in seen:
                        ignore.add(table_name)
                        continue
                    else:
                        seen.add(table_name)
        
                    start = int(float(row['start']))
                    length = int(row['table_cells'])
                    

                    tables[table_name] = dict(
                        name = row['table_id'],
                        universe=None,
                        description=row['title'].title(),
                        columns=[],
                        data = dict(
                            sequence = int(row['sequence_number']),
                            start=start, 
                            length=length, 
                            
                        )
                    )
        
                elif 'Universe' in row['title']:
                    tables[table_name]['universe'] = row['title'].replace('Universe: ','').strip()

                elif add_columns and row['is_column'] == 'Y':
                    
                    col_name = table_name+"{:03d}".format(int(row['line']))
  
                    col_names = [ c['name'] for c in tables[table_name]['columns'] ]
                    if col_name  in col_names:
                        raise Exception("Already have {} in {}".format(col_name, 
                                        col_names))
  
                    tables[table_name]['columns'].append(dict(
                        name=col_name,
                        description=row['title'], 
                        datatype = 'float',
                        data=dict(start=row['segment_column']))
                        )
                        
                    # Add the margin of error column
                    tables[table_name]['columns'].append(dict(
                        name=col_name+'_m90',
                        description="Margin of error for: "+col_name, 
                        datatype = 'float',
                        data=dict(start=row['segment_column']))
                        )
                    
                    
        return tables
 
        
    def make_source(self, table):
        from ambry.orm.exc import NotFoundError

        try:
            ds = self._dataset.source_file(table.name)
        except NotFoundError:
            ds = self._dataset.new_source(table.name,
                dest_table_name = table.name, 
                reftype='generator',
                ref='TableRowGenerator')
                
        except: # Odd error with 'none' in keys for d
            raise
 
    def test_mk_sources(self):
        
        for t in self.tables:
            self.make_source(t)
            print t.name
            
        self.commit()
        
 
    ##
    ##
    ##
 

    def post_build(self, phase='build'):
        """After the build, update the configuration with the time required for
        the build, then save the schema back to the tables, if it was revised
        during the build."""

        try:
            self.build_post_unify_partitions()
        except Exception:
            self.set_error_state()
            self.commit()
            raise

        return True
       
    def post_everything(self):
        self.library.search.index_bundle(self, force=True)

        self.state = phase + '_done'

        self.log("---- Finished Build ---- ")
