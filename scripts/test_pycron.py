import os
import time
import random

rand_int = random.randint(61, 120)
print('test_pycron! waiting {} secs'.format(str(rand_int)))

time.sleep(rand_int)
