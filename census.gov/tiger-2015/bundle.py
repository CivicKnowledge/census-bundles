# -*- coding: utf-8 -*-
import ambry.bundle


class UrlGenerator(object):
    """Generate from collections of state files. """

    def __init__(self, bundle, source):
     
        self._url_template = source.url
        
        self._states_p = bundle.dep('state_list')

        self._states = set([ (row.stusab, row.state) for row in self._states_p if row.component == '00' ])
        

    def __iter__(self):
        
        for stusab, state_fips in self._states:
            yield stusab, self._url_template.format(state=str(state_fips).zfill(2))
        
        

class Bundle(ambry.bundle.Bundle):
    pass
    
    def parse_state(self, v):
        pass
        
    def meta_add_sources(self):
        from ambry.orm import DataSource
        
        for tmpl_source in self.sources:
            if tmpl_source.name.endswith('_url'):
                
                table_name, _ = tmpl_source.name.split('_')
                
                for stusab, url in UrlGenerator(self, tmpl_source):
                   
                    self.dataset.new_source(table_name+' '+stusab, 
                                            ref=url,
                                            epsg=4239, 
                                            filetype='shape',
                                            source_table_name=table_name, 
                                            dest_table_name=table_name)
                                            
      
        
        self.build_source_files.sources.objects_to_record()
        
        self.commit()
        
                         
    def meta_schema(self):
        
        schema_sources = [ s for s in self.sources if ' RI' in s.name  or s.name in ('states','counties')]
        
        self.ingest(sources=schema_sources)
            
        self.dataset.source_tables[:] = []
 
        self.commit()
 
        self.source_schema(sources=schema_sources)
        
        self.build_source_files.sourceschema.objects_to_record()
        
        self.commit()
                
                
                
        
                                            
                                            
        
   
        

