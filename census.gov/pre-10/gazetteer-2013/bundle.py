'''

'''

from  ambry.bundle.loader import CsvBundle
 


class Bundle(CsvBundle):
    ''' '''

    def meta(self):
        from sqlalchemy.orm.exc import NoResultFound
        
        super(Bundle, self).meta()
        
        proto = self.library.dep('proto')
        
        # Add in the gvid
        with self.session:
            for t in self.schema.tables:
                t.add_column('gvid',datatype='varchar', description='Civic Knowledge geographic id',
                fk_vid = t.data.get('foreign_key',None), indexes='i1')


                try:
                    pt = proto.schema.table(t.name)
                  
                    t.data['summary_level'] = pt.data['summary_level']
                except NoResultFound:
                    pass
                
                

        self.schema.write_schema()

        return True
        
    def build(self):
        from geoid  import generate_all, Geoid
        from geoid.tiger import TigerGeoid
        from geoid.civick import GVid

        for source in self.metadata.sources:

            table = self.schema.table(source)

            sl = table.data.get('summary_level', None)

            p = self.partitions.find_or_new(table=source)

            p.clean()

            self.log("Loading source '{}' into partition '{}' sl={}".format(source, p.identity.name, sl))

            lr = self.init_log_rate(print_rate = 5)

            header = [c.name for c in p.table.columns]

            with p.inserter() as ins:
                for _, row in self.gen_rows(source):
                    lr(str(p.identity.name))

                    d = dict(zip(header, row))

                    if sl:
                        d['gvid']  = str(TigerGeoid.get_class(sl).parse(d['geoid']).convert(GVid))
                    
                    ins.insert(d)


        return True
        
    
