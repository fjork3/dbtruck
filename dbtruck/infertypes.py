import sys
import csv
import re
import math
import datetime

from collections import *
from dateutil.parser import parse as dateparse


# notes: alter table readings alter column time type time using (time::time);

re_null_chars = re.compile('[\*\.\?-_]+')
re_num_bad_chars = re.compile('[\,\$\@\#\*]')

bool_true_values = ['TRUE', 't', 'true', 'y', 'yes', 'on']
bool_false_values = ['FALSE', 'f', 'false', 'n', 'no', 'off']

def get_type(val):
    '''
    Determine the data type of a single value.
    Check in order int, float, date, then default to string if none match.
    '''
    if not isinstance(val, basestring):
        return type(val)
    val = val.strip()
    if not val: return None

    numval = re_num_bad_chars.sub('', val)

    
    try:
        i = int(numval)
        return int
    except:
        pass

    try:
        f = float(numval)
        return float
    except:
        try:
            if numval.endswith('%'):
                float(numval[:-1])
                return float
        except:
                pass

    try:
        d = dateparse(val)
        if d.hour == d.minute and d.minute == d.microsecond and d.hour == 0:
            return datetime.date
        elif datetime.datetime.now().date() == d.date():
            if d.hour != 0 or d.minute != 0 or d.second != 0 or d.microsecond != 0:
                return datetime.time
        return datetime.datetime
    except:
        pass


    if (val in bool_true_values) or (val in bool_false_values):
        return bool

    return str

def validate_type((t, v)):
    """
    check that the value matches the type
    """
    if v == 'NULL':
        return True
    
    if t in [datetime.datetime, datetime.date, datetime.time]:
        if '0000-00-00' == v:
            return False
        
    try:
        if t in [int, float]:
            t(v)
    except:
        return False
    
    return True

def str2sqlval((t, val)):
    if not isinstance(val, basestring):
        return val

    # if type was designated as a string, we're already good
    if issubclass(t, basestring):
        return val
    
    val = val.strip()
    nullval = re_null_chars.sub('', val)
    # try:
    #     if t == datetime.datetime:
    #         d = dateparse(val).strftime('%Y-%m-%d %H:%M:%S')
    #         return d
    #     if t == datetime.date:
    #         d = dateparse(val).strftime('%Y-%m-%d')
    #         return d
    #     if t == datetime.time:
    #         d = dateparse(val).strftime('%H:%M:%S')
    #         return d
    # except:
    #     pass

    if t == bool:
        if val in bool_true_values:
            return True
        if val in bool_false_values:
            return False
        # something has gone wrong
        return None
    
    if t == int or t == float:
        numval = re_num_bad_chars.sub('', val)
        try:    
            if t == int:
                return int(numval)
            if t == float:
                return float(numval)
        except:
            if t == float and numval.endswith('%'):
                try:
                    return float(numval[:-1])
                except:
                    pass
            if not nullval:
                return None
            return 0

    if not nullval:
        return None

    return val



def infer_col_types(iterf):
    """
    Attempts to guess the msot likely type for each column.
    Scans the first 1000 rows to get the most consistent row length,
    then up to 5000 rows to check types.

    @return a list of Python types
    """
    # infer best row length
    if iterf.header:
        # if we have headers, just get the number of cols from there
        types = [Counter() for j in xrange(len(iterf.header))]
    else:
        # otherwise, look at the first 1000 rows and pick the most common length
        import time
        b = time.time()
        rowiter = iterf()
        rowiter.next()
        bestrowlen = 0
        secondbest = 0
        stddev = 0
        numread = 0
        avg = 0
        var = 0
        while abs(secondbest - bestrowlen) <= 2*math.sqrt(var) and numread < 10000:
            counter = Counter(len(rowiter.next()) for i in xrange(1000))
            rowlens = counter.most_common()
            newsum = sum([x[1] for x in rowlens])
            newvar = sum([(x[1] - newsum/len(rowlens))**2 for x in rowlens]) / len(rowlens)
            var = (numread*var + len(rowlens)*newvar) / (numread + len(rowlens)) - (numread * len(rowlens)) / ((numread + len(rowlens))**2) * ((avg - newsum/len(rowlens))**2)
            avg = (numread*avg + newsum) / (numread + len(rowlens))
            numread += len(rowlens)
            if len(rowlens) > 0:
                bestrowlen = rowlens[0][0]
            if len(rowlens) > 1:
                secondbest = rowlens[1][0]
        types = [Counter() for j in xrange(bestrowlen)]
        e = time.time()
        print 'inferring row len', b, e, e-b

    # iterate through rows (up to the first 5000 with the same length)
    b = time.time()
    rowiter = iterf()
    allcertain = False
    numread = 0
    while not allcertain and numread < 10000:
        linenum = 0
        for row in rowiter:
            if len(row) != len(types):
                continue

            for key, val in enumerate(row):
                t = get_type(val)
                if t is not None:
                    types[key][t] += 1
            linenum += 1
            if linenum >= 1000:
                break
        commons = [c.most_common() for c in types]
        # there are 4 types: int, float, date, string
        avgs = [sum([x[1] for x in col])/4 for col in commons]
        variances = [(sum([(x[1] - avgs[i])**2 for x in commons[i]]) + (4 - len(commons[i])) * avgs[i]**2) / 4 for i in xrange(len(commons))]
        numread += linenum
        certains = [abs(commons[i][0][1] - avgs[i]) > 1.5*math.sqrt(variances[i]) for i in xrange(len(commons))]
        allcertain = all(certains)
        
    # read off most common type from each column counter (type, count)
    #commons =  [c.most_common(1) for c in types]
    commons = [len(c) and (c[0][0] or str) for c in commons]
    e = time.time()
    print 'inferring data types', b, e, e-b
    return commons



def infer_primary_key(iterf, header, types):
    '''
    Infer whether a table has a primary key field.
    Returns the first column with all unique values.

    @return name of inferred primary key field, or None if no apparent pkey
    '''
    # keep counter objects on values in columns
    # if max count greater than 1, it's not a pkey
    # TODO: restrict to types that make sense for pkey field?

    counts = [Counter() for j in xrange(len(header))]
    rowiter = iterf()
    valid_cols = [True] * len(counts)


    for row in rowiter:
        # ignore non-confirming rows
        if len(row) != len(counts):
            continue

        # if we've seen a value before, that column is invalid
        for key, val in enumerate(row):
            if valid_cols[key]:
                # TODO: also needs to check for nulls
                if val in counts[key]:
                    # disqualify this column
                    valid_cols[key] = False
                else:
                    counts[key][val] += 1

        if not any(valid_cols):
            return None

    # if we still have valid pkey fields, return the first one
    # prefer integer fields
    for i, h in enumerate(header):
        if (valid_cols[i]) and (types[i] == int):
            return h

    for i, h in enumerate(header):
        if valid_cols[i]:
            return h

    return None
