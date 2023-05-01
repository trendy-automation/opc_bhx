"""
nohup python3 /home/kipia/opc_BHX/opc_run.py>> /home/kipia/opc_BHX/log_opcBHX.txt 2>&1 &
source /home/kipia/opc_BHX/env/bin/activate
deactivate
ps aux | grep BHX
"""

from subprocess import Popen, PIPE
from time import sleep
import sys
from multiprocessing import Process

def start():
    print("START LOADER")
    sudo_password = '8gC7Ud3WCo'
    command1 = 'python3 /home/kipia/opc_BHX/loader.py'.split()
    p = Popen(['sudo', '-S'] + command1, stdout=PIPE, stdin=PIPE, stderr=PIPE, universal_newlines=True)
    # p.communicate(sudo_password + '\n')
    print("LOADER STARTED")
    with p.stdout as pipe:
        for line in iter(pipe.readline, b''):  # b'\n'-separated lines
            print(line)

if __name__ == "__main__":
    print("BHX START")
    start()