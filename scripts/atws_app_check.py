import sys
import subprocess
from subprocess import Popen, PIPE

def atws_app_check():
    process = Popen(['adb', 'shell', 'ps', '|', 'grep', 'atws.app'], stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    result = stdout.decode().strip() + stderr.decode().strip()
    process.wait()
    errcode = process.returncode
    return not ('error' in result or len(result) == 0 or errcode > 0)

if __name__ == '__main__':
    print(atws_app_check())
