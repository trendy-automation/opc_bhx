import logging
import os
import sys

# Определить формат сообщений
FORMAT = logging.Formatter("[%(asctime)s: %(filename)13s:%(lineno)3s - %(funcName)20s() %(levelname)5s] %(message)s")
LOGS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '/home/kipia/opc_BHX/log_opcBHX.txt'))

# Создать обработчик, который выводит сообщения в файл
server_log_hand = logging.FileHandler(LOGS_PATH)
server_log_hand.setFormatter(FORMAT)

# Создать обработчик, который выводит сообщения в консоль
server_terminal_log = logging.StreamHandler(sys.stderr)
server_terminal_log.setFormatter(FORMAT)

# Создать регистратор
logger = logging.getLogger('opc_py')
logger.setLevel(logging.DEBUG)
logger.addHandler(server_log_hand)
logger.addHandler(server_terminal_log)
