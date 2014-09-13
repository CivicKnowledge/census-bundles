'''
Load tigerline files
'''
from  ambry.bundle import BuildBundle
class Bundle(BuildBundle):
    '''Load Tigerline data for blocks'''
    def __init__(self, directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

    def build(self):
        self.load_features()
        return True

    def _states(self):
        '''Get a list of stats, names and abbreviations from the 2010 census'''

        states_part = self.library.dep('states').partition
        # The geocom names an interation of a subset of the state,
        # '00' is for the whole state,  while there are others for urban,
        # rural metropolitan and many other areas.

        return states_part.query("select * from geofile where geocomp = '00'")


    def load_features(self):
        from multiprocessing import Pool

        states = [ (s['name'], s['stusab'], s['state']) 
                 for s in self._states() ]
             
        num_procs = self.run_args.multi if self.run_args.multi else 1
                 
        
        if self.run_args.test:
            if num_procs == 1:
                states = [ e for e in states if e[1] == 'RI']
            else:
                states = states[:num_procs]

        year = 2010

        for type_, table_name in self.metadata.build.types.items():    
            
            if num_procs == 1:
                for name, stusab, state in states:
                    self._load_state_features(state, name, stusab, year, type_, table_name)
            else:
                pool = Pool(num_procs, maxtasksperchild=1)
                pool.map(mp_run, [ (self.bundle_dir, state, name, stusab, year, type_, table_name) 
                                    for name, stusab, state in states ] )

    def _load_state_features(self, state, name, stusab, year, type_, table_name):
        import osgeo.ogr as ogr
        import osgeo.gdal as gdal

        #gdal.UseExceptions()
        #ogr.UseExceptions()

        url = self.metadata.build.url_template.format(
                type=type_.upper(), state=int(state),
                typelc=type_.lower(), year4=year, year2= year%100 )


        p = self.partitions.find_or_new_geo(table=table_name,
                                            space=stusab.lower())

        shape_file = self.filesystem.download_shapefile(url)
        
        self.log("Loading {} for {} from {}".format(table_name, p.name, shape_file))
        
        shapefile = ogr.Open(shape_file)
        layer = shapefile.GetLayer(0)
        lr = self.init_log_rate()
        columns = [c.name for c in p.table.columns]
        with p.database.inserter(layer_name=table_name) as ins:

            while True:
                feature = layer.GetNextFeature() # Copy of the feature.
                if not feature:
                    break
                row = self.make_block_row(columns, state, feature)
                #print i, row['geoid'], feature.geometry().Centroid()
                lr("Load {}".format(name))
                #print row
                ins.insert(row)
         
    mbr_types = None


    @staticmethod
    def gf(key,vname,type_, columns, feature):
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
                'name': gf('name','NAME10',str,columns,feature),
                'zacta': gf('zacta','ZCTA5CE10',str,columns,feature), 
                'state': None,
                'statefp': gf('statefp','STATEFP10',int,columns,feature),
                'statece':state,
                'county': None,
                'countyfp': gf('countyfp','COUNTYFP10',int,columns,feature), 
                'placefp': gf('placefp','PLACEFP10',int,columns,feature), 
                'placens': gf('placens','PLACENS10',int,columns,feature), 
                'tractce': gf('tractce','TRACTCE10',int,columns,feature),
                'geoid': gf('geoid','GEOID10',str,columns,feature),
                'arealand': gf('arealand','ALAND10',int,columns,feature),
                'areawater': gf('areawater','AWATER10',int,columns,feature),
                'lat': gf('lat','INTPTLAT10',float,columns,feature),
                'lon': gf('lon','INTPTLON10',float,columns,feature),
                # Need to force to multipolygon because some are poly and some
                # are multi pol, which is OK in a shapefile, but not in
                # Spatialite
                'geometry': ogr.ForceToMultiPolygon(feature.geometry()).ExportToWkt()
                }
                
def mp_run(args):
    (dir_, state, name, stusab, year, type_, table_name) = args
    bundle = Bundle(dir_)
    bundle.log("Multi_processor run: {}".format(args))
    try:
        bundle._load_state_features(state, name, stusab, year, type_, table_name)
    except Exception as exc:
        bundle.error("Multiprocessor run failed: {}, {}".format(args , exc))

import sys
if __name__ == '__main__':
    import ambry.run
    ambry.run.run(sys.argv[1:], Bundle)
    