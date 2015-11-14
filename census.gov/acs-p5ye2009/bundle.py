 # -*- coding: utf-8 -*-
import ambry.bundle 
from ambry.util import memoize

from censuslib import ACS09TableRowGenerator as TableRowGenerator
from censuslib import MakeTableMixin, MakeSourcesMixin, JamValueMixin
from censuslib import JoinGeofileMixin, ACS2009Bundle

class Bundle(ACS2009Bundle):

   
    ##
    ##
    ##
 

    def post_build(self, phase='build'):
        """After the build, update the configuration with the time required for
        the build, then save the schema back to the tables, if it was revised
        during the build."""

        try:
            self.build_post_unify_partitions()
        except Exception:
            self.set_error_state()
            self.commit()
            raise

        return True
       
    def post_everything(self):
        self.library.search.index_bundle(self, force=True)

        self.state = phase + '_done'

        self.log("---- Finished Build ---- ")
