import random
import time
import string

c = string.printable[1:62]

def gen_rand_cols(ncols, primary_key):
    types = ['str', 'int', 'float', 'time']
    if primary_key:
        return ['id'] + [random.choice(types) for i in xrange(ncols-1)]
    else:
        return [random.choice(types) for i in xrange(ncols)]

def gen_rand(i, t, rand_errors):
    if t=='id':
        return str(i)
    if random.random() < rand_errors:
        return ''
    if t=='str':
        return ''.join([random.choice(c) for i in xrange(random.randint(1, 5))])
    elif t=='int':
        return str(random.randint(1, 1000000))
    elif t=='float':
        return str(random.random()*1000000)
    elif t=='time':
        currtime = time.time()
        return time.strftime('%m/%d/%Y %I:%M %p', time.localtime(random.random() * currtime))
    else:
        raise Exception("Type " + str(t) + " is not in types.")

def gen(filename, nrows, ncols, primary_key=True, write_header=True, rand_errors=0):
    with open(filename, 'w') as f:
        cols = gen_rand_cols(ncols, primary_key)
        header = [cols[i] + str(i) for i in xrange(len(cols))]
        if primary_key:
            header[0] = 'is_an_id'
        print 'Header: ' + str(header) + ' (Press Enter to confirm)'
        raw_input()
        if write_header:
            f.write(','.join(header) + '\n')
        for i in xrange(nrows):
            if i % 1000 == 0:
                print 'Generating row ' + str(i)
            row = [gen_rand(i, t, rand_errors) for t in cols]
            f.write(','.join(row) + '\n')
        f.close()

if __name__ == '__main__':
    gen("dummy1.csv", 10000, 5, primary_key=True, write_header=True, rand_errors=0)
