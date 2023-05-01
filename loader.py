#8gC7Ud3WCo
#screen -S opc_BHX
#source /home/kipia/opc_BHX/env/bin/activate
#cd opc_BHX
#sudo python3 loader.py
import os
import sys
import importlib
import time
import threading
import logging
from multiprocessing.connection import Client
from logger import logger

print("LOADER STEP 1")

class Monitor(threading.Thread):
    def __init__(self, loader, frequency=1):
        super().__init__()
        self.frequency = frequency
        self.loader = loader
        self.daemon = True
        self.path = os.path.dirname(os.path.realpath(__file__)) + '/'
        self.last_mtime = 0
        files = [f for f in filter(self.file_filter, os.listdir(self.path))]
        logger.debug(f"Monitor files {files} in {self.path}")

    def file_filter(self, name):
        return (not name.startswith(".")) and (name.endswith(".py"))

    def file_times(self, path):
        for file in filter(self.file_filter, os.listdir(path)):
            yield os.stat(path + file).st_mtime

    def run(self):
        while True:
            ft = self.file_times(self.path)
            max_mtime = max(ft)
            if max_mtime > self.last_mtime:
                self.last_mtime = max_mtime
                self.loader.notify(self.last_mtime)
            time.sleep(self.frequency)
 

class Loader:
    def __init__(self, source, port=None, key=None):
        self.port = port
        self.key = key
        self.source = source
        self.__name = os.path.splitext(self.source)[0]
        self.module = importlib.import_module(self.__name)
        self.last_mtime = None
        self.changed = False
        monitor = Monitor(self)
        monitor.start()

    def notify(self, last_mtime):
        self.last_mtime = last_mtime
        try:
            logger.debug(f"Last_mtime changed to {last_mtime} script {self.source}, reloading.")
            if self.port and self.key:
                try:
                    conn = Client(('localhost', self.port), authkey=self.key)
                    conn.send("EXIT")
                    conn.close()
                    print('EXIT send')
                except Exception as e:
                    logger.error(f"Loop stop failed. {e}")
            self.changed = True
            self.module = importlib.reload(self.module)
        except Exception as e:
            logger.error(f"Reload failed. {e}")

    def has_changed(self):
        if self.changed:
            logger.debug(f"Loader.has_changed called, self.changed is {self.changed}")
            self.changed = False
            return True
        else:
            return False

    def __getattr__(self, attr):
        return getattr(self.module, attr)


logger = logging.getLogger("opc_py")
if __name__ == "__main__":
    print("LOADER STEP 2")

    # FORMAT = "[%(asctime)s: %(filename)13s:%(lineno)3s - %(funcName)20s() %(levelname)5s] %(message)s"
    # logging.basicConfig(format=FORMAT, level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

    server = Loader("server.py", 6002, b'expopsw')
    print("LOADER STEP 3")
    while True:
        print("running...")
        # Check if server-script has been modified since last poll.
        if server.has_changed():
            # Execute a function from server-script if it has been modified.
            logger.info(f"Script {server.source} has changed and reloaded")
            server.main(server.port, server.key)
        time.sleep(1)
