import random
import time
import string

c = string.printable[1:62]

def gen_rand_cols(ncols, primary_key=True):
    types = ['str', 'int', 'float', 'time']
    if primary_key:
        return ['id'] + [random.choice(types) for i in xrange(ncols-1)]
    else:
        return [random.choice(types) for i in xrange(ncols)]

def gen_rand(i, t):
    if t=='id':
        return str(i)
    elif t=='str':
        return ''.join([random.choice(c) for i in xrange(random.randint(1, 1000))])
    elif t=='int':
        return str(random.randint(1, 1000000))
    elif t=='float':
        return str(random.random()*1000000)
    elif t=='time':
        currtime = time.time()
        return time.strftime('%m/%d/%Y %I:%M %p', time.localtime(random.random() * currtime))
    else:
        raise Exception("Type " + str(t) + " is not in types.")

def gen(filename, nrows, ncols, primary_key=True, write_header=True):
    with open(filename, 'w') as f:
        cols = gen_rand_cols(ncols)
        if write_header:
            header = [cols[i] + str(i) for i in xrange(len(cols))]
            print 'Header: ' + str(header)
            raw_input()
            f.write(','.join(header) + '\n')
        for i in xrange(nrows):
            if i % 1000 == 0:
                print 'Generating row ' + str(i)
            row = [gen_rand(i, t) for t in cols]
            f.write(','.join(row) + '\n')
        f.close()

if __name__ == '__main__':
    gen("dummy1.csv", 10000, 200)
