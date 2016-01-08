import ambry.bundle 


class Bundle(ambry.bundle.Bundle):
    
    year = 2014
    
    def init(self):
        self._sl_map = None
    
    @staticmethod
    def non_int_is_null(v):
        
        try:
            return int(v)
        except ValueError:
            return None
            

    ##
    ## Meta, Step 1: Bulid the source and dest schemas
    ##
    def meta_mkschema(self):
        """Create the  geofile schema from the configuration in the 
           upstream bundle. """
        from ambry.orm.file import File
   
        t = self.dataset.new_table('geofile')
        st = self.dataset.new_source_table('geofile')

        p = self.dep('geofile_schema')
        i = 1
        for row in p:
           
            if  row['year'] == self.year :
                i += 1
                name = row['name'].lower().strip()
                name = name if name != 'blank' else 'blank{}'.format(i)
                self.logger.info(name)
            
                t.add_column(name, datatype = 'str', description = row['description'])
            
                st.add_column( source_header = name, position = row['seq'],
                        datatype = str,
                        start = row['start'], width = row['width'], 
                        description = row['description'])

        self.commit() 
        
        self.build_source_files.sourceschema.objects_to_record()
        self.build_source_files.schema.objects_to_record()
        
        self.commit()
        
    ##
    ## Meta Step 2: Add source links
    ##
    def meta_add_sources(self):
        self._meta_add_1yr_sources()
        self._meta_add_5yr_sources()
    
    
    def _meta_add_1yr_sources(self):
        """Run once to create to create the sources.csv file. Scrapes the web page with the links to the 
        files.  """
        from ambry.orm import DataSource, File
        from ambry.util import scrape_urls_from_web_page
        from ambry.orm.exc import NotFoundError
        
        span = 1
         
        source = self.source('dnlpage{}{}'.format(self.year,span))
            
        entries = scrape_urls_from_web_page(source.url)['sources']
         
        for k,v in entries.items():
        
            d = {
                'name': k.lower()+"_{}{}".format(self.year,span),
                'source_table_name': 'geofile',
                'dest_table_name': 'geofile',
                'filetype': 'csv',
                'file': 'g{}.*\.csv'.format(self.year),
                'encoding': 'latin1',
                'time': str(self.year)+str(span), 
                'start_line': 0,
                'url': v['url']
            }
        
            try:
                s = self._dataset.source_file(d['name'])
                s.update(**d)
            except NotFoundError:
                s = self.dataset.new_source(**d)
             
            self.session.merge(s)
        
        
        self.commit()

        self.build_source_files.sources.objects_to_record()
  
        self.commit()
  

    def _meta_add_5yr_sources(self):
        """The 5 year release has a different structure because the files are bigger. """
        from ambry.orm import DataSource, File
        from ambry.util import scrape_urls_from_web_page
        from ambry.orm.exc import NotFoundError
        import os
        
        year = self.year
        span = 5

        source = self.source('dnlpage{}{}'.format(year,span))

        self.log("Loading from {}".format(source.url))

        name_map={
            'All_Geographies_Not_Tracts_Block_Groups': 'L',
            'Tracts_Block_Groups_Only': 'S'
        }

        def parse_name(inp):
            for suffix, code in name_map.items():
                if inp.endswith(suffix):
                    return inp.replace('_'+suffix, ''), code
            return (None, None)
                

        for link_name, parts in scrape_urls_from_web_page(source.url)['sources'].items():
            url=parts['url']
            
            state_name, size_code = parse_name(link_name)
                    
            d = {
                'name': "{}{}_{}{}".format(state_name,size_code,self.year, span),
                'source_table_name': 'geofile',
                'dest_table_name': 'geofile',
                'filetype': 'csv',
                'file': 'g{}.*\.csv'.format(self.year),
                'encoding': 'latin1',
                'time': str(self.year)+str(span), 
                'start_line': 0,
                'url':url
            }

            try:
                s = self._dataset.source_file(d['name'])
                s.update(**d)
            except NotFoundError:
                s = self.dataset.new_source(**d)

            self.session.merge(s)
            self.log(s.name)
            
        self.commit()

        self.build_source_files.sources.objects_to_record()
  
        self.commit()
      
    ##
    ## Meta Step 3: Update the datatype based on a single ingestion
    ##
    def meta_update_source_types(self):
        from ambry_sources.intuit import TypeIntuiter
        
        s = self.source('CaliforniaS_20145')
        s.start_line = 0
        s.header_lines = []
        self.commit()
        
        #self.ingest(sources=['CaliforniaS_20145'], force=True)
        
        s = self.source('CaliforniaS_20145')
        st = self.source_table('geofile')
        dt = self.table('geofile')
        
        def col_by_pos(pos):
            for c in st.columns:
                if c.position == pos:
                    return c
        
        with s.datafile.reader as r:
            
            for col in r.columns:
                c = col_by_pos(col.position+1)
                
                c.datatype = col['resolved_type'] if col['resolved_type'] != 'unknown' else 'str'
                
                dc = dt.column(c.name)
                dc.datatype = c.datatype
                
        self.commit()
             
        self.build_source_files.sourceschema.objects_to_record()
        self.build_source_files.schema.objects_to_record()
        
        self.commit()
        
                            
    ##
    ## Meta Step 4, After Build: Create per-summary level tables
    ##              
    def meta_build_reduced_schemas(self):
        """
        After running once, it is clear that not all columns are used in all 
        summary levels. This routine builds new tables for all of the summary 
        levels that have only the columns that are used. 
        
        
        """
        from collections import defaultdict
        from itertools import islice, izip
      
        table_titles = { int(r['sumlevel']): r['description'] if r['description'] else r['sumlevel'] 
                         for r in self.dep('sumlevels')}
        
        p = self.partition(table='geofile', time='20145')
        
        
        # Create a dict of sets, where each set holds the non-empty columns for rows of
        # a summary level
        gf = defaultdict(set)
        for r in p:
            gf[r.sumlevel] |= set(k for k,v in r.items() if v)
            
        for sumlevel, fields in gf.items():

            t = self.dataset.new_table('geofile'+str(sumlevel))
            t.columns = []
            self.commit()
            
            t.description = 'Geofile for: ' + str(table_titles.get(int(sumlevel), sumlevel))

            self.log('New table {}: {}'.format(t.name, t.description))

            for c in self.table('geofile').columns:
                if c.name in fields:
                    t.add_column(name=c.name, datatype=c.datatype, description=c.description, transform=c.transform)

        self.commit()
        
        self.build_source_files.schema.objects_to_record()
        
        self.commit()
                
                
        
        
          
    
