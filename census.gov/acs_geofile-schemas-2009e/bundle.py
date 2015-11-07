import ambry.bundle 
from ambry.etl import Pipe
from ambry.util import memoize

class AugmentTableMeta(Pipe):

    def __init__(self, bundle, source):
        super(AugmentTableMeta, self).__init__(bundle, source)

    def __iter__(self):

        pass

class Bundle(ambry.bundle.Bundle):
    
    
    @staticmethod
    def save_start(v, row, accumulator):
        """Store the start position and sequence number for a table"""
        from ambry.valuetype.types import int_n, nullify
        try:
            v = nullify(v)
            if v:
                v = int(v)
                accumulator[(row.table_id.lower(), 
                             int_n(row.sequence_number))] = v
        
                return v
        except:
            print row
            raise
    
        return None

    def segment_column(self, v, row, source, accumulator):
        """Calculate the 1-indexed column position in the segment file for this 
        column, base on the sequence_id and start position for the table, 
        stored by save_start"""
    
        try:
            if row.is_column == 'Y' and row.line:
                start = accumulator[(row.table_id.lower(), 
                                     int(row.sequence_number))]
                pos = start + row.line  - 1
                return pos
        except Exception as e:
            self.error("segment_column exc for source {}: {} "
                       .format(source.name, e))
            print accumulator
            print row
            raise
            
        return None
    
    @staticmethod
    def skip_empty_table_id(row, source):
        """Predicate for the Skip pipeline to skip lines without a table_id """
        
        try:
            return not bool(row['table_id'].strip())
        except AttributeError:
            # row['table_id'] is None, so no .strip()
            return True
        except KeyError:
            self.error("No column for 'table_id' in {}".format(source.name))
            return False

    def tc_caster(self, pipe, row, v):
        # Value has  an int, or "<int> CELL" or "<int> CELLS"

        orig_value = v

        try:
            return int(v)
        except TypeError: # None
            return None
        except ValueError:
            try:
                v = v.split()[0]
            except IndexError:
                return None
                
            try:
                return int(v)
            except ValueError:
                return int(float(v))
   

    @staticmethod
    def fix_group_headings(pipe, row, v):
        from six import text_type

        if text_type(row.title).endswith('--'):
            return float(v)/10.0
        else:
            return v

    @staticmethod
    def set_grain(source):
        return str(source.grain) if source.grain else 'all' 

    def set_is_column(self, pipe, row, accumulator):
        """In the table_sequence table, the column lines have integer values, 
        while headers have decimal values and other entries have no line number"""
        from ambry.valuetype.types import nullify
       
        line = nullify(row.line)
        
        if not line:
            accumulator['ts_line'] = 0
            return 'N'
            
        try:
            
            # Catch lines that are out of order. These are header lines. Usually
            # they are offset by .5, but in the 2009 file, they are
            # all multiplied by 10, so what should be 1.5 appears as 15. 
            if  line - accumulator['ts_line'] > 1:
                line = float(line) / 10.0
            
            accumulator['ts_line'] = int(line)
            
            if int(line) == line: # Check that it is an integer. 
                return 'Y'
            else:
                return 'N'
                
        except ValueError:
            return 'N'


    

    @property
    @memoize
    def table_spans(self):

        p = self.partition('census.gov-acs_geofile-schemas-2009e-table_sequence')  
        
        table_spans = {}
        
        for row in p.stream(as_dict = True):
            if bool(row['start_position']) and bool(row['start_position'].strip()):
                sp =  int(float(row['start_position']))

                # For the entries that have the word ' cell ' in them
                length = int(row['total_cells_in_table'].split()[0])
                    
                table_spans[(row['table_id'], int(row['year']), int(row['release']) )] = (sp, length)
                
        return  table_spans
        
    def test_augment(self):
        
        self.log("Getting Spans")
        spans = self.table_spans
        
        p = self.partition('census.gov-acs_geofile-schemas-2009e-table_meta')  
        
        errors = set()
        
        for row in p.stream(as_dict = True):
            try:
                table_span = spans[(row['table_id'], int(row['year']), int(row['release']))]   
                
            except KeyError:
                print (row['table_id'], int(row['year']), int(row['release']))
                errors.add((int(row['year']), int(row['release'])))
                
        print errors
        
