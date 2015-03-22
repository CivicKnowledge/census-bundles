'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def build(self):
        import sas7bdat
        
        
        for k, v in self.metadata.sources.items():
            zfn = self.filesystem.download(k)
            fn = self.filesystem.unzip(zfn)
            
            print fn
            
            sas = sas7bdat.SAS7BDAT(fn)
            
            for row in sas.readData():
                print row
        
        
        
