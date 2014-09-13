'''
Combine multiple Census geography imports into a single geography dimension dataset

@author: eric
'''
from  ambry.bundle import BuildBundle
import os.path
import yaml

class Bundle(BuildBundle):
    '''
    Bundle code for US 2010 Census geo files. 
    '''

    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        
        bg = self.metadata.build
        self.geoschema_file = self.filesystem.path(bg.geoschemaFile)
        self.states_file =  self.filesystem.path(bg.statesFile)
    
    def build(self):
        
        self.build_large_areas()
        self.build_small_areas(2010)
        
        return True
    
    def build_small_areas(self, year=2010):
        """Create the partitions for Tracts, BlockGroups and Blocks"""

        states = { row['state']:row['stusab'] 
        for row in self.partitions.find(table='states').query("SELECT * from states") }
        
        source = self.library.dep('geo'+str(year))

        

        for sumlev, table_name in self.metadata.queries.small_areas:
            geoid_columns, geoid_template = self.make_geoid_template(table_name)
            
            q = "SELECT * FROM geofile WHERE state = ? "
        
            lr = self.init_log_rate(print_rate=5)
            for state_id, state_abbrev in states.items():

                if sumlev == 101:
                    # These are partitioned by state. 
                    space = state_abbrev.lower()
                else:
                    space = None

                new_part = self.partitions.find_or_new( space=space, table=table_name)
             
                partition = source.partitions.find(table='geofile', grain=sumlev, space=space).get()

                if not partition:
                    self.error("Didn't get partition for sumlev {} space {}".format(sumlev,space ))
                    continue

                
                with new_part.database.inserter(table_name, replace=True) as ins:
                    for row in partition.query(q, state_id):

                        drow = dict(row)

                        drow['geoid'] = geoid_template.format(**row)
                        drow['year'] = year;
                        
                        ins.insert(drow)
                        lr(new_part.identity.sname)

    def build_large_areas(self):
        """Create entries for the large-scale areas, excluding tracts, blockgroups and blocks. 
        
        This code seems more comlicated than it needs be be because it loads 
        from both the 2010 and 2000 files, which have different columns. This 
        allows for linking to either year, and can discover then names for 
        areas change. 
        
        """
        from sqlalchemy.exc import IntegrityError
        
        source1_bundle = self.library.dep('geo2000')
        source2_bundle = self.library.dep('geo2010')

        template = self.metadata.queries.template
        data = self.metadata.queries.large_areas

        table_names = [ table_name for sumlev, table_name, name_remove, where in data ]

        
    
        # name_remove is a string to strip out ffrom the Name field of the 
        # geofile. Some sumlevs have an extra term like "City" that isn't necessary
        for sumlev, table_name, name_remove, where in data:
            
            self.log("Processing large area {}".format(table_name))
            
            table = self.schema.table(table_name)


            p = self.partitions.find_or_new(table=table_name)
            

            sp1_internal = source1_bundle.partitions.find(table='geofile',grain=sumlev)
            
            if not sp1_internal:
                self.error("Didn't get partition (1) for sumlev {} in {}".format(sumlev,source1_bundle.database.path ))
                continue
   
            sp1 = sp1_internal.get() # POssibly fetch from the remote
 
            
            sp2_internal = source2_bundle.partitions.find(table='geofile',grain=sumlev)


            if not sp2_internal:
                self.error("(2a) Didn't get partition for sumlev {} in {}"
                    .format(sumlev,source2_bundle.database.path ))
                continue

            sp2 = sp2_internal.get() # POssibly fetch from the remote


            if not sp2:
                self.error("(2b) Didn't get partition {} for sumlev {} from library or remote"
                    .format(sp2_internal.identity.vname, sumlev))
                continue

            self.log("Attaching {}".format(sp1.database.path))
            s1 = p.database.attach(sp1, 'source1')
            self.log("Attaching {}".format(sp2.database.path))
            s2 = p.database.attach(sp2, 'source2')


            # Create two field sets, for the 2000 and 2010 censuses. We'll use 
            # them to construct the query later. 
            s1_fields = [c.name for c in table.columns if c.data.get('in2000', False) 
                         and c.name not  in ('name', 'geoid') and not c.name.endswith('_id')]
            s2_fields = [c.name for  c in table.columns if c.name not in s1_fields 
                         and c.name not  in ('name', 'geoid') and not c.name.endswith('_id')]

            fields = s1_fields + s2_fields
            s1_fields = ['s1.'+c for c in s1_fields]
            s2_fields = ['s2.'+c for c in s2_fields]
            
            # Using the 'join' data in the schema, create join equalities
            joins = [ "s1.{0} = s2.{0}".format(c.name)  
                       for  c in table.columns if c.data.get('join',False)]

            # Doing a lot here. Create two seperate selects, one for each of the two census files, Set missing
            # fields to NULL, remove unecessary strings from the names 
            select1 = [ c.name if c.data.get('in2000',False) else 'Null as {}'.format(c.name)
                        for c in table.columns if c.name not  in ('name', 'geoid') and not c.name.endswith('_id')]
                        
            select1.append("trim(replace(name,'{name_remove}','')) as name1".format(name_remove=name_remove));
            
            select2 = [ c.name if c.data.get('in2010',False) else 'Null as {}'.format(c.name)
                        for c in table.columns  if c.name not  in ('name', 'geoid') and not c.name.endswith('_id')]
            
            select2.append("trim(replace(name,'{name_remove}','')) as name2".format(name_remove=name_remove));


            q = template.format(fields=','.join(s1_fields+s2_fields+ ['name1', 'name2']), 
                                select1=','.join(select1),
                                select2=','.join(select2),
                                s1=s1, s2=s2,
                                sumlev=sumlev, 
                                joins=' AND '.join(joins),
                                where=' WHERE '+where if where else ''
                                )

            fields = fields + ['name']
            
            # Create a template for constructing geoids
            geoid_columns, geoid_template = self.make_geoid_template(table_name)

            seen = set()
            lr = self.init_log_rate(print_rate=5)
            with p.database.inserter(table_name) as ins:
               
                for row in  p.database.connection.execute(q):
                    
                    row = list(row)

                    name1 = row.pop()
                    name2 = row.pop()
                    
                    if name2:
                        name = name2
                    elif name1:
                        name = name1
                    else:
                        raise Exception("Missing Name")
                    
                    row.append(name)

                    drow = dict(zip(fields, row))
                    drow[table_name+'_id'] = None

                    geoid_parts = {k:int(v) for k,v in drow.items() 
                        if k in geoid_columns}

                    drow['geoid'] = geoid_template.format(**geoid_parts)

                    if drow['geoid'] not in seen:
                        ins.insert(drow)
                        seen.add(drow['geoid'])
                        lr(table_name)
                        
                    

            p.database.detach(s1)
            p.database.detach(s2)

        return True
             

 
    def make_geoid_template(self, table_name):
        
        table = self.schema.table(table_name)
        
        if not table:
            raise Exception("Didn't get table for table name : {}".format(table_name))
        
        columns = []
        
        for column in table.columns:
            if column.data['geoid']:
                columns.append((column,column.data['geoid'], column.data['geoidl']))
             
        columns = sorted(columns, key=lambda x: x[1])
     
        template = ''      
        for column in columns:
            template += "{{{name}:0{length}d}}".format(name=column[0].name, length=column[2])
     
        return [c[0].name for c in columns], template

    def load(self, year, config ):
        """External method to .... yeah I forgot ... """
        from ambry.identity import PartitionIdentity
        
        year_suffix = str(year)[2:]
        
        ds = get_library().get(config.source) 
        qt = config.template
          
        for qd in config.data:
            table_name = str(qd[1])+year_suffix
            
            pid = PartitionIdentity(self.identity, grain=qd[1])
            dp = self.partitions.find(pid) # Find puts id_ into partition.identity
            
            if not dp:
                dp = self.partitions.new_partition(pid)
                dp.database.create(copy_tables = False)
                #dp.create_with_tables() 
            
            sp = ds.bundle.partitions.find(grain=qd[0])
            print sp.identity.name, sp.database.path
            
            with dp.database.inserter(table_name) as ins:
                q = qt.format(*qd)
                print q
                for row in  sp.database.session.execute(q):
                    geo_id = 'foo'
                    ins.insert((None, geo_id)+tuple(row))
    
    def make_generated_geo(self):
        geo_file = self.filesystem.path(self.metadata.build.gengeoFile)
        with open(geo_file, 'w') as f:
            import csv
            writer = csv.writer(f)
            writer.writerow(['table','column','is_pk','type'])
            for year, cfg in  self.metadata.queries.items():
                self._make_generated_geo(writer, cfg.source, cfg.template, cfg.data, year)
             

    def _make_generated_geo(self, writer, dataset_name, template, data, year):
        """Create a schema file for the queries to extract data from the 
        source partition. 
        
        This method executed the queries defined in the 'meta' configuration, 
        then creates a schema for that query, based on the first row. 
        """
        from ambry.identity import PartitionIdentity
        
        ds = get_library().get(dataset_name) 
        year_suffix = str(year)[2:]
        
        for qd in data:
            
            table_name = str(qd[1])+year_suffix
            writer.writerow([table_name,qd[1]+'_id', 1, 'INTEGER'])
            writer.writerow([table_name,'geoid', None, 'TEXT'])
             
            # Source partition          
            partition = ds.bundle.partitions.find(grain=qd[0])
            
            if not partition: 
                raise Exception("Failed to get partition for grain {} from dataset {}"
                                .format(qd[0], dataset_name))
            
            row =  partition.database.session.execute(template.format(*qd)).first()
            
            for k,v in zip(row.keys(), row):
                
                try:
                    int(v)
                    type = "INTEGER"
                except:
                    type = "TEXT"
                    
                if k in ('name'):
                    type = 'INTEGER'
                    
                writer.writerow([table_name, str(k),None,type])

        

