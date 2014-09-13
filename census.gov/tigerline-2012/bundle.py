'''
Load tigerline files
'''
from  ambry.bundle import BuildBundle
class Bundle(BuildBundle):
    '''Load Tigerline data for blocks'''


    def build(self):
        self.load_split_features()
        self.load_combined_features()
        
        return True

    def _states(self):
        '''Get a list of stats, names and abbreviations from the 2010 census'''

        states_part = self.library.dep('states').partition
        # The geocom names an interation of a subset of the state,
        # '00' is for the whole state,  while there are others for urban,
        # rural metropolitan and many other areas.

        return states_part.query("select * from geofile where geocomp = '00'")


    def load_split_features(self):
        from multiprocessing import Pool

        states = [ (s['name'], s['stusab'], s['state']) 
                 for s in self._states() ]
             
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

    def load_combined_features(self):
    
        for table_name, url in self.metadata.build.combined_types.items():
            
            shape_file = self.filesystem.download_shapefile(url)

            p = self.partitions.find_or_new_geo(table=table_name)

            self._load_partition(p, table_name, shape_file, None)   
            
            p.close() 

    def _load_state_features(self, state, name, stusab, year, type_, table_name):
        

        #gdal.UseExceptions()
        #ogr.UseExceptions()

        url = self.metadata.build.url_template.format(
                type=type_.upper(), state=int(state),
                typelc=type_.lower(), year4=year, year2= year%100 )


        shape_file = self.filesystem.download_shapefile(url)

        p = self.partitions.find_or_new_geo(table=table_name,
                                            space=stusab.lower())

        self._load_partition(p, table_name, shape_file, state)
        
        p.close()
        
        
    def _load_partition(self, p, table_name, shape_file, state):
        import osgeo.ogr as ogr
        import osgeo.gdal as gdal
        
        self.log("Loading {} for {} from {}".format(table_name, p.name, shape_file))
        
        shapefile = ogr.Open(shape_file)
        layer = shapefile.GetLayer(0)
        lr = self.init_log_rate()
        columns = [c.name for c in p.table.columns]
        with p.database.inserter(layer_name=table_name) as ins:

            i = 0
            while True:
                feature = layer.GetNextFeature() # Copy of the feature.
                if not feature:
                    break
                row = self.make_block_row(columns, state, feature)
                #print i, row['geoid'], feature.geometry().Centroid()
                lr(p.identity.sname)
                #print row
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
                'state': None,
                'statefp': gf('statefp','STATEFP',int,columns,feature),
                'statece': state,
                'county': None,
                'countyfp': gf('countyfp','COUNTYFP',int,columns,feature), 
                'placefp': gf('placefp','PLACEFP',int,columns,feature), 
                'placens': gf('placens','PLACENS',int,columns,feature), 
                'tractce': gf('tractce','TRACTCE',int,columns,feature),
                'blkgrpce': gf('blkgrpce','BLKGRPCE',int,columns,feature),
                'funcstat': gf('funcstat','FUNCSTAT',int,columns,feature),
                'geoid': gf('geoid','GEOID',str,columns,feature),
                'arealand': gf('arealand','ALAND',float,columns,feature),
                'areawater': gf('areawater','AWATER',float,columns,feature),
                'lat': gf('lat','INTPTLAT',float,columns,feature),
                'lon': gf('lon','INTPTLON',float,columns,feature),
                # Need to force to multipolygon because some are poly and some
                # are multi pol, which is OK in a shapefile, but not in
                # Spatialite
                'geometry': ogr.ForceToMultiPolygon(feature.geometry()).ExportToWkt()
                }
                
