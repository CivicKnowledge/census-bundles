# Support for building census bundles in Ambry

__version__ = '0.0.1'
__author__ = 'eric@civicknowledge.com'


from .generator import * 
from .schema import *
from .sources import * 
from .transforms import *

import ambry.bundle

class AcsBundle(ambry.bundle.Bundle, MakeTablesMixin, MakeSourcesMixin, 
             JamValueMixin, JoinGeofileMixin):

    # Which of the first columns in the data tavbles to use. 
    header_cols = [
        # Column name, Description, width, datatype, column position
        #('FILEID','File Identification',6,'str' ),
        #('FILETYPE','File Type',6,'str'),
        ('STUSAB','State/U.S.-Abbreviation (USPS)',2,'str',2 ),
        ('CHARITER','Character Iteration',3,'str',3 ),
        ('SEQUENCE','Sequence Number',4,'int',4 ),
        ('LOGRECNO','Logical Record Number',7,'int',5 )
    ]

    def init(self):
        from .util import year_release

        self.year, self.release = year_release(self)



class ACS2009Bundle(AcsBundle):
    pass

class ACS2010Bundle(AcsBundle):
    pass