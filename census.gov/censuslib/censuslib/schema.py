# Mixins for creating the destination schemna


class MakeTablesMixin(object):
    
    ##
    ## Create Tables
    ##
        
    def create_table_schema(self):
        
        self.log("Deleteting old tables and partitions")
        self.dataset.delete_tables_partitions()
        
        self.commit()

        tables = self.tables_list()
        
        self.log("Creating {} tables".format(len(tables)))
        
        lr = self.init_log_rate(100)
        
        for i, table_id in enumerate(sorted(tables.keys())):

            d = tables[table_id]
            
            lr(table_id)
            print i, table_id
            
            t = self.make_table( i+1, **d )
            
            self.make_source(t)
            
        self.commit()
        
        self.sync_out()


    def make_table(self,  sequence_id, name, universe, description, columns, data):
        """Meta-phase routine to create a single table, called from 
        create_tables"""
        t = self.new_table(name, description = description.title(), 
                            universe = universe.title(), data = data,
                           sequence_id = sequence_id)
        
        for name, desc, size, dt, pos in self.header_cols:
            t.add_column(name,
                         sequence_id = pos,
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

    def make_source(self, table):
        from ambry.orm.exc import NotFoundError

        try:
            ds = self._dataset.source_file(table.name)
        except NotFoundError:
            ds = self._dataset.new_source(table.name,
                                          dest_table_name=table.name,
                                          reftype='generator',
                                          ref='TableRowGenerator')

        except:  # Odd error with 'none' in keys for d
            raise

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
                    if self.limited_run and i > 1000:
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
 