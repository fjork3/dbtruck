import random
import string


def gen(filename, nrows):
    with open(filename, 'w') as f:
        for i in xrange(nrows):
            randstr = ''.join([random.choice(string.ascii_lowercase) for i in range(8)])
            row = [str(random.randint(1, 10000)),
                   randstr,
                   str(random.random() * 10000),
                   str(random.randint(1, 10))]
            f.write(','.join(row) + '\n')

if __name__ == '__main__':
    gen("dummy1.csv", 10000)
