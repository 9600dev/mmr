import os
import sys
import time
import random

def main(arg):
    rand_int = random.randint(61, 120)
    print('test_pycron_sync {} waiting {} secs'.format(arg, str(rand_int)))
    time.sleep(rand_int)


if __name__ == '__main__':
    main(sys.argv[1])
