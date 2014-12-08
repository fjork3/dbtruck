import csv
import os

from base import *

class CSVMethods(BaseMethods):
    '''
    Exporter class for writing imported data to a CSV file.
    '''


    def __init__(self, *args, **kwargs):
        BaseMethods.__init__(self, *args, **kwargs)
        self.outdir = kwargs.get('outdir', './')
        self.outfile = None
        self.writer = None
        self.header = None

    def setup_table(self, types, header, new=False):
        '''
        Prepare for CSV output. Creates a new file with the same name as the table,
        or appends to it if the file already exists in the output directory.
        '''
        self.outfile = file(os.path.join(self.outdir, self.tablename),
                            'w' if new else 'a')
        self.writer = csv.DictWriter(self.outfile, header)
        self.header = header

    def prepare_row_for_copy(self, row):
        '''
        Converts all fields to a standard format for CSV writeout.

        @return the dictionary representation of the row
        '''
        newrow = []
        for col in row:
            if col is None:
                newrow.append(None)
            elif isinstance(col, basestring):
                newrow.append(col.encode('utf-8', errors='ignore'))
            else:
                newrow.append(str(col))
        return self.row_to_dict(newrow)

    def row_to_dict(self, row):
        '''
        Creates a dictionary, with keys from the header row and values from the input row.
        '''
        return dict(zip(self.header, row))


    def import_block(self, buf, iterf):


        
        dicts = (self.prepare_row_for_copy(row) for row in buf)
        self.writer.writerows(dicts)

    def import_row(self, row):
        try:
            self.writer.writerow(self.prepare_row_for_copy(row))
        except:
            print row

    def close(self):
        self.outfile.close()
