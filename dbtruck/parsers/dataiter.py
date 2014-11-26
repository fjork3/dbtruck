import re

from collections import Counter

from dbtruck.infertypes import *
from dbtruck.util import get_logger, to_utf

re_space = re.compile('\s+')
re_nonascii = re.compile('[^\w\s]')
re_nonasciistart = re.compile('^[^\w]')
_log = get_logger()


class DataIterator(object):
    def __init__(self, iter_func, **kwargs):
        self.iter_func = iter_func
        self.fname = None
        self.file_index = 0  # keeps track of which table in th file this object refers to
        self.header = None
        self.header_inferred = False
        self.add_id_col = False
        self.types = None
        # Add any kwargs as class attribues
        self.__dict__.update(kwargs)

    def infer_metadata(self):
        if not self.types:
            # infer_col_types from infertypes.py
            self.types = infer_col_types(self)

        self.infer_header()
        _log.info('types:\t%s', ' '.join(map(str, self.types)))
        _log.info('headers:\t%s', ' '.join(self.header))

    def infer_header(self):
        """
        validate, infer/generate a header for this iterator
        """

        # if all things fail, we can always make up headers!
        try:
            self.validate_header()

            if not self.header:
                self.infer_header_row()

            self.clean_header()
        except:
            pass

        # if we didn't find a header row, then lets default to
        # generating one
        if not self.header:
            self.header_inferred = False
            self.header = ['attr%d' % i for i in xrange(len(self.types))]

        # ensure proper length by manufacturing extra header columns
        if len(self.header) < len(self.types):
            for i in xrange(len(self.types) - len(self.header)):
                self.header.append('attr%d' % i)

        # trim extra columns
        self.header = self.header[:len(self.types)]

        # we _always_ need an ID column
        if 'id' not in self.header:
            self.header.append('id')
            self.add_id_col = True
            self.types.append(int)

    def infer_header_row(self):
        "analyze first row in iterator and check if it looks like a header"

        types = self.types

        # TODO: does this need to be in a try/except? If so can it be a less general except?
        try:
            header = self().next()
            header = [s.strip() for s in header]
            _log.info("checking header: %s", header)
            htypes = map(get_type, header)
            # Compare the types of header columns and self.types, record the number of matches
            matches = sum([(ht == t) and (ht is not None) and (t != str)
                           for ht, t in zip(htypes, types)])
            _log.info("matches: %s", matches)

            if matches > 0:
                return

            if max(map(len, header)) > 100:
                _.log.warn("header colname longer than 100: %s", max(map(len, header)))
                return

            # TODO more analysis?
            # lots of more complex analysis goes HERE
            self.header = header
            self.header_inferred = True
        except:
            return

    def clean_header(self):
        """Remove non-ascii values from the header, fill in any gaps with generic
        attribute names, and rename duplicate attributes. Fails if the same attribute
        is present more than three times."""

        header = self.header
        if not header:
            return

        newheader = []
        timesseen = Counter()
        attridx = 0
        for value in header:
            try:
                ret = re_nonasciistart.sub('', re_space.sub('_', re_nonascii.sub('', value.strip()).strip()))
                ret = to_utf(ret)
                if not ret:
                    ret = 'attr%d' % attridx
                if re.match('\d+', ret):
                    ret = 'n_%s' % ret
            except:
                _log.info('clean_header\t%s', value)
                ret = 'attr%d' % attridx
            attridx += 1
            ret = ret.lower()
            if timesseen[ret] > 0:
                newheader.append('%s_%d' % (ret, timesseen[ret]))
            elif timesseen[ret] > 3:
                break
            else:
                newheader.append(ret)
            timesseen[ret] += 1

        # ensure that header doesn't have overlapping values
        if len(set(newheader)) < len(newheader):
            _log.info("duplicate elements in header\t%s", str(newheader))
            self.header = None
        else:
            self.header = newheader

    def validate_header(self):
        """Check that the length of the header matches the most common row length.
        If the header is invalid, sets self.header to None."""
        if self.header:
            c = Counter()
            # __call__ is defined below, so self() returns self.iter_func()
            # count the length of the first thousand rows
            for idx, row in enumerate(self()):
                c[len(row)] += 1
                if idx > 1000:
                    break
            if c:
                ncols = c.most_common(1)[0][0]
                if len(self.header) != ncols:
                    self.header = None
                    _log.info("""invalidating self.header because length %d doesn't
                                 match most popular row length %d""",
                              len(self.header),
                              ncols)

    def __call__(self):
        return self.iter_func()

    def __iter__(self):
        return self.iter_func()
