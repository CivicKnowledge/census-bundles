""""""

from ambry.bundle import BuildBundle
from Queue import Queue, Full, Empty
import threading

class Bundle(BuildBundle):

    span = 2013
    year = 5

    
    def write_partitions(self):
        """Because accessing the parittions database is a bit slow. """
        l = self.library
        b = l.get('census.gov-acs-p5ye2013')
        geo = l.get('census.gov-acs-geo-p5ye2013-geofile').partition
        
        partitions = []

        for i,p in enumerate(b.partitions.all):
            
            if self.run_args.test:
                if i > 10: 
                    break
            
            partitions.append({
                    "partition": str(p.name),
                    "table": p.table.name
                })
                
        self.filesystem.write_yaml(partitions, 'meta','partitions.yaml')
        

                
    def test_build(self):

        for e in self.filesystem.read_yaml('meta','partitions.yaml'):
            table = e['table']
            self.write_partition(self.year, self.span, table)
  
    def write_partition(self, year, span, table):
         from ambry import library
         import unicodecsv as csv
         import os
         from collections import defaultdict

         print "Building table:",  year, span, table

     
         l = self.library

         geo = l.get('census.gov-acs-geo-p5ye2013-geofile').partition
         p = l.get('census.gov-acs-p5ye2013-{}'.format(table)).partition

         p.attach(geo, "geo")

         path_t =    "p{span}Ye{year}/{sumlev}/{table}.csv"

         sumlevs = defaultdict(list)

         q = """
             SELECT * FROM geo.geofile AS geo LEFT JOIN {table} ON geo.id = {table}.geofile_id
         """.format(table=table)

         for i, row in enumerate(p.query(q)):

             sumlevs[row.sumlevel].append(row)

         p.detach('geo')


         for sumlevel, rows in sumlevs.items():
         
             path = path_t.format(sumlev=row.sumlevel, span=span, year = year, table = table)

             root = '/Volumes/DataLibrary/census'

             fn = os.path.join(root, path)
         
             dir_name = os.path.dirname(fn)
             if not os.path.exists(dir_name):
                 os.makedirs(dir_name)
         
             with open(fn, 'w') as f:
                 writer = csv.writer(f)
             
                 writer.writerow( c.name for c in p.table.columns )
             
                 for row in rows:
             
                     if row.component != '00':
                         continue
                     
                     writer.writerow(list(row))      
        
        
        