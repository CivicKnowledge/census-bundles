 # -*- coding: utf-8 -*-
import ambry.bundle 

from censuslib import ACS2009Bundle
from censuslib import ACS09TableRowGenerator as TableRowGenerator

class Bundle(ACS2009Bundle):
    pass 
    
    def print_specs(self):

        s = self.source('b00001')
        
        trg = TableRowGenerator(self, s)

        for s1, s2 in trg.generate_source_specs():
            print s1.url
    
