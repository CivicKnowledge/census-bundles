""""""

from ambry.bundle import BuildBundle
from Queue import Queue, Full, Empty
import threading
import os

from ckcache.s3 import S3Cache

class Bundle(BuildBundle):

    span = 2013
    year = 5

    def __init__(self, bundle_dir=None):

        super(Bundle, self).__init__(bundle_dir)

        account = dict(
            access = os.getenv('AWS_ACCESS_KEY_ID'),
            secret = os.getenv('AWS_SECRET_ACCESS_KEY')
        )

        self.fs = S3Cache('census.public.civicknowledge.com', prefix='test', account = account )

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

         header = None
         for i, row in enumerate(p.query(q)):

             if not header:
                 header = row.keys()

             sumlevs[row.sumlevel].append(row)

         p.detach('geo')
         p.close()

         for sumlevel, rows in sumlevs.items():
         
             path = path_t.format(sumlev=row.sumlevel, span=span, year = year, table = table)
             
             self.log('{} {} {}'.format(sumlevel, table, path))

             if self.fs.has(path):
                self.log("Exists: {}".format(path))
                continue


             with self.fs.put_stream(path) as f: # #open(fn, 'w') as f:
                 writer = csv.writer(f)
             
                 writer.writerow( header )
             
                 for row in rows:
             
                     if row.component != '00':
                         continue
                     
                     writer.writerow(list(row))      
           