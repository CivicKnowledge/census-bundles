'''
Load tigerline files
'''
from  ambry.bundle import BuildBundle
class Bundle(BuildBundle):
    '''Load Tigerline data for blocks'''


    def xbuild(self):
        self.load_split_features()
        self.load_combined_features()
        
        return True

    def build(self):
        
        for url_code, table_name in self.metadata.build.url_codes.items():
            if table_name in ('states','counties'):
                self.build_us_partition(2012,url_code,table_name)
            else:
                self.build_partition(2012, url_code,table_name)
        
    def build_us_partition(self,year, url_code,table_name):

        self._load_state_features(None, None, 'us', year, url_code, table_name, 
                                 self.metadata.build.us_url_template)

    def build_partition(self,year, url_code,table_name):

        for year in [year]:
            for s in self._states():
                self._load_state_features(s['state'], s['name'], s['stusab'], year, url_code, table_name,
                                         self.metadata.build.state_url_template)
            
            

    def _states(self):
        '''Get a list of states, names and abbreviations from the 2010 census'''
        import cPickle as pickle
        import os
        
        sf = self.filesystem.build_path('states_cache.pkl')
        
        if os.path.exists(sf):
            
            with open(sf) as f:
                return pickle.load(f)
        
        states_part = self.library.dep('states').partition
        # The geocom names an interation of a subset of the state,
        # '00' is for the whole state,  while there are others for urban,
        # rural metropolitan and many other areas.

        states = [ dict(row) for row in states_part.query("select * from geofile where geocomp = '00'") ]

        with open(sf,'w') as f:
            pickle.dump(states, f, -1)
                
        return  states


    def load_split_features(self):
        from multiprocessing import Pool

        states = [ (s['name'], s['stusab'], s['state']) for s in self._states() ]
             
        num_procs = self.run_args.multi if self.run_args.multi else 1
                 
        
        if self.run_args.test:
            if num_procs == 1:
                states = [ e for e in states if e[1] == 'RI']
            else:
                states = states[:num_procs]

        year = int(self.identity.btime)

        for type_, table_name in self.metadata.build.split_types.items():    
            
            if num_procs == 1:
                for name, stusab, state in states:
                    self._load_state_features(state, name.strip(), stusab, 
                                             year, type_, table_name)
            else:
                self.run_mp(self._load_state_features,
                            [( state, name.strip(), stusab, year, type_, table_name) 
                            for name, stusab, state in states ] )


    def _load_state_features(self, state, name, stusab, year, type_, table_name, template):
        

        self.log("Downloading {} {} {}".format(year, table_name,  stusab))

        url = template.format(
                type=type_.upper(), state= int(state) if state else 'us',
                typelc=type_.lower(), year4=year, year2= year%100 )


        shape_file = self.filesystem.download_shapefile(url)

        p = self.partitions.find_or_new_geo(table=table_name,
                                            space=stusab.lower())

        self._load_partition(p, table_name, shape_file, state)
        
        p.close()
        
        
    def _load_partition(self, p, table_name, shape_file, state):
        import osgeo.ogr as ogr
        import osgeo.gdal as gdal
        from geoid import generate_all
        
        self.log("Loading {} for {} from {}".format(table_name, p.name, shape_file))
        
        shapefile = ogr.Open(shape_file)
        layer = shapefile.GetLayer(0)
        lr = self.init_log_rate()
        columns = [c.name for c in p.table.columns] + ['state','county','blkgrp','tract','place']

        sumlevel =  p.table.data['sumlevel']
        
        with p.database.inserter(layer_name=table_name) as ins:

            i = 0
            while True:
                feature = layer.GetNextFeature() # Copy of the feature.
                if not feature:
                    break
                row = self.make_block_row(columns, state, feature)
                #print i, row['geoid'], feature.geometry().Centroid()
                lr(p.identity.sname)
                
                geoids = generate_all(sumlevel, row)
                row.update(geoids)

                if not geoids['geoidt'] != row['geoid']:
                    print row
                    raise Exception("Geoid mismatch! '{}' != '{}' ".format(geoids['geoidt'], row['geoid']))
                
                ins.insert(row)
                
                
                i += 1
                
        p.close()
                
 
    mbr_types = None

    @staticmethod
    def gf(key,vname,type_, columns, feature):
        '''GetField, from an OGR feature'''
        if key not in columns:
            return None
        elif type_ is int:
            return feature.GetFieldAsInteger(vname)
        elif type_ is str:
            return feature.GetFieldAsString(vname)
        elif type_ is float:
            return feature.GetFieldAsDouble(vname)
        else:
            raise ValueError("Unknown type for type_ : {}", type_)
         
    @classmethod
    def make_block_row(clz,  columns, state, feature):
        '''Create a database row for a census block'''
        import ogr
        gf  = clz.gf

        #feature.GetGeometryRef().TransformTo(aa.srs)
        return {
                'name': gf('name','NAME',str,columns,feature),
                'zacta': gf('zacta','ZCTA5CE',str,columns,feature), 
                'state': gf('state','STATEFP',int,columns,feature),
                'county': gf('county','COUNTYFP',int,columns,feature), 
                'place': gf('place','PLACEFP',int,columns,feature), 
                'cosub': gf('cousub','COUSUB',int,columns,feature), 
                'placens': gf('placens','PLACENS',int,columns,feature), 
                'tract': gf('tract','TRACTCE',int,columns,feature),
                'blkgrp': gf('blkgrp','BLKGRPCE',int,columns,feature),
                'funcstat': gf('funcstat','FUNCSTAT',int,columns,feature),
                'geoid': gf('geoid','GEOID',str,columns,feature),
                'arealand': gf('arealand','ALAND',float,columns,feature),
                'areawater': gf('areawater','AWATER',float,columns,feature),
                'intplat': gf('lat','INTPTLAT',float,columns,feature),
                'intplon': gf('lon','INTPTLON',float,columns,feature),
                # Need to force to multipolygon because some are poly and some
                # are multi pol, which is OK in a shapefile, but not in
                # Spatialite
                'geometry': ogr.ForceToMultiPolygon(feature.geometry()).ExportToWkt()
                }
                
