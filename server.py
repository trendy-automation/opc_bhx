# nohup python3 /home/kipia/opc_BHX/loader.py>> /home/kipia/opc_BHX/log_opc_BHX.txt 2>&1 &
# ps -aux | grep loader.py 

# global modules
import time
import json
import datetime
import logging
import threading
import importlib
#from importlib import reload
from multiprocessing.connection import Listener, Client
from queue import Queue
import ast
import traceback
# from web_server import web_app
import web_server
# local modules
#from service import Service
#from tags import Tags
#from robot import Robot
import sql
import socket
import plc
import telegramSQL
import tags
import robot
import service
import movie_maker
import draw
import csv_prepare
from plc import PLC
from telegramSQL import Telegram_SQL_Error


# logger = logging.getLogger("opc_py")
from logger import logger


def plc_robot_create(plcs, plc_ip, plc_name, robots_count, robot_ids):
    """
    Создание PLC и роботов
    """
    try:
        if not plc_ip in plcs:
            logger.debug(f"Создание нового PLC с IP: {plc_ip} имя {plc_name} robots_count {robots_count}")
            plc = PLC(plc_ip=plc_ip, plc_name=plc_name, robots_count=robots_count, robot_ids=robot_ids)
            plc.start()
            assert plc, f"PLC не добавлен {plc_ip}"
            plcs[plc_ip] = plc
            logger.info(f"Загружен PLC {plc.plc_ip}")
            return plc
        else:
            logger.info(f"PLC {plc_ip} уже загружен")
            return plcs[plc_ip]
    except Exception as error:
        logger.error(f"PLC {plc_ip} не создан. Ошибка {str(error)} {traceback.format_exc()}")
    return None


def task_append(plc, robot_task):
    """
    Добавление задания робота в очередь
    """
    try:
        if all(k in robot_task for k in
               ["robot_task_id", "operation_side", "task_status", "part_status", "robot_id", "robot_number"]):
            if not plc.robots[robot_task['robot_number']] is None:
                if plc.robots[robot_task['robot_number']].queue_task.empty():
                    plc.robots[robot_task['robot_number']].queue_task.put(robot_task)
                    logger.info(f"Новое задание {robot_task['robot_task_id']} отрпавлено в PLC: {plc.name}")
                    return True
                else:
                    logger.error(f"Ошибка записи нового задания {robot_task}")
                    logger.error(f"Робот контроллера {plc.plc_ip} ещё занят заданием {plc.robots[robot_task['robot_number']].queue_task.queue[0]['robot_task_id']} ({plc.robots[robot_task['robot_number']].queue_task.queue[0]})")
            else:
                logger.info(f"Недействительный robot_number {robot_task['robot_number']} в задании {robot_task['robot_task_id']} для {plc.name} len(plc.robots) {len(plc.robots)} plc.robots {plc.robots}")
        else:
            logger.error(f"Получено некорректное задание {robot_task}")
    except Exception as error:
        logger.error(f"Задание не отправлено {robot_task}. Ошибка {str(error)} {traceback.format_exc()}")
    return False


def load_plcs(plcs, sql_conn):
    """
    Загрузка PLC
    """
    try:
        logger.debug(f"{sql_conn.database} Загрузка PLC, подписка на задания")
        msg = sql_conn.get_plcs_status()
        logger.debug(f"msg {msg}")
        if msg:
            for rt in msg:
                robot_task = rt[0]
                if all(k in robot_task for k in ["plc_ip", "plc_name", "robots_count", "robot_ids"]):
                    plc = plc_robot_create(plcs, robot_task["plc_ip"], robot_task["plc_name"], robot_task["robots_count"], robot_task["robot_ids"])
                    logger.debug(f"{sql_conn.database} Отправка задания")
                    task_append(plc, robot_task)
                else:
                    logger.error(f"{sql_conn.database} Ошибка загрузки PLC: некорректное сообщение {robot_task}")
        return True
    except Exception as error:
        logger.error(f"{sql_conn.database} Ошибка загрузки PLC {str(error)} {traceback.format_exc()}. Сервер остановлен")
        return False


def telegram_errors_handler(listener, telegram):
    """
    Отправка сообщений в телеграм
    """
    conn = None
    time.sleep(1)
    while getattr(threading.currentThread(), "do_run", True):
        try:
            if conn:
                if conn.poll():                    
                    msg = str(conn.recv())
                    logger.info(f"got error message: {msg}")
                    if msg!='EXIT':
                        telegram.queue_error.put(msg)
                        conn.send(dict(result="OK"))
                    conn.close()
                    conn = None
            else:
                conn = listener.accept()
                ip, port = listener.last_accepted
        except Exception as error:
            logger.error(f'Ошибка при получении сообщения для телеграма {str(error)} {traceback.format_exc()}')
            conn = None
        time.sleep(0.2)


def status_handler(plcs, sql_conn):
    """
    Обновление статуса заданий
    """
    while getattr(threading.currentThread(), "do_run", True):
        try:
            # Опрос всех очередей PLC в поиске новых статусов
            for plc_ip in plcs:
                plc = plcs[plc_ip]
                if not plc.queue_status.empty():
                    logger.debug(f"{plc.name} query {plc.queue_status.queue[0]['query']}")
                    status_feedback = plc.queue_status.queue[0]
                    res = sql_conn.set_status(status_feedback)
                    status = plc.queue_status.get()
                    assert res, f'status_handler: Пустой ответ'
                    if 'error' in res:
                        logger.error(f"{sql_conn.database} Статус не записан в БД. {res['error']}")
                    else:
                        assert 'result' in res, f"{sql_conn.database} Статус не записан в БД. Неизветная ошибка. res={res} status={status_feedback}"
                        assert res['result'] == 'OK', f"{sql_conn.database} Статус не записан в БД res={res} status={status_feedback}"
                        if status_feedback['query']=='expected_robot_trajectory':
                            if res['robot_trajectories'] and not plc.robots[status_feedback['robot_number']].queue_task.empty():
                                plc.robots[status_feedback['robot_number']].queue_task.queue[0]['robot_trajectories']=json.dumps(res['robot_trajectories'])
                                logger.info(f"{sql_conn.database} robot_trajectories {res['robot_trajectories']}")
        except AssertionError as error:
            logger.error(f'{sql_conn.database} Ошибка при обновлении статуса задания {str(error)}')
        except Exception as error:
            logger.error(f'{sql_conn.database} Ошибка при обновлении статуса задания {str(error)} {traceback.format_exc()}')
        time.sleep(0.2)

def notify_handler(plcs, sql_conn, web_app):
    """
    Обработка событий БД
    """
    while getattr(threading.currentThread(), "do_run", True):
        try:
            # logger.debug(f"sql_conn.connect.poll()")
            sql_conn.connect.poll()
        except Exception as error:
            logger.error(f'Ошибка при обновлении соединения: {str(error)} {traceback.format_exc()}')
            try:
                sql_conn.connect.close()
                sql_conn.connect2db()
            except Exception as error:
                logger.error(f'Не удалось переподключиться к БД: {str(error)} {traceback.format_exc()}')
                time.sleep(60)
        try:
            # logger.debug(f"sql_conn.connect.notifies")
            if sql_conn.connect.notifies:
                notify = sql_conn.connect.notifies.pop()
                if notify.channel == "part_layers":
                    part_layers = json.loads(notify.payload)
                    logger.debug(f"notify_handler {sql_conn.database} get part_layers: {part_layers}")
                    web_app.queue_part_layers.queue[0]=part_layers
                if notify.channel == "robot_task":
                    robot_task = json.loads(notify.payload)
                    logger.debug(f"notify_handler {sql_conn.database} get robot_task: {robot_task}")
                    assert 'plc_ip' in robot_task, "plc_ip не найден в задании"
                    plc_ip = robot_task['plc_ip']
                    if not plc_ip in plcs:
                        logger.debug(f"{sql_conn.database} Загрузка новейшего PLC: {plc_ip} {plcs.keys()}")
                        if all(k in robot_task for k in ["robot_task_id", "operation_side", "task_status", "part_status", "robots_count","robot_ids"]):
                            plc = plc_robot_create(plcs, plc_ip, robot_task["plc_name"], robot_task["robots_count"], robot_task["robot_ids"])
                        else:
                            logger.debug(f"Некорректно отправлено задание на : {plc_ip}")
                    else:
                        logger.debug(f"Используем созданный поток с именем: {plc_ip}")
                        plc = plcs[plc_ip]
                    if plc:
                        result = task_append(plc, robot_task)
            sql_conn.connect.notifies.clear()
        except Exception as error:
            logger.error(f'Ошибка при получении {notify.channel}: {str(error)} {traceback.format_exc()}')
        time.sleep(0.2)


def web_handler(plcs, sql_conn, web_app):
    """
    Обработка веб интерфейса
    """
    while getattr(threading.currentThread(), "do_run", True):
        try:
            if not web_app.queue_tags.empty():
                for plc_ip in plcs:
                    web_app.queue_tags.queue[0][plc_ip]=plcs[plc_ip].plc_tags.tags
                    #for robot_number in range(1,plcs[plc_ip].robots_count+1):
                    #    if not plcs[plc_ip].robots[robot_number] is None:
                    #        if not plcs[plc_ip].robots[robot_number].queue_task.empty():
                    #            #for tag_name in plcs[plc_ip].robots[robot_number].queue_task.queue[0].keys():
                    #            #    web_app.queue_tags.queue[0][plc_ip]['tsk_'+tag_name] = dict(type='LInt' if tag_name.endswith('_id') else 'String[30]',use='Out',read_only=True,db_number=0,offsetbyte=0,offsetbit=0,tag_value='',new_value=None)
                    #            #    web_app.queue_tags.queue[0][plc_ip]['tsk_'+tag_name]['tag_value'] = plcs[plc_ip].robots[robot_number].queue_task.queue[0][tag_name]
                    #            if 'robot_trajectories' in plcs[plc_ip].robots[robot_number].queue_task.queue[0]:
                    #                web_app.queue_tags.queue[0][plc_ip][robot_number]['tsk_robot_trajectories'] = dict(type='String[255]',use='Out',read_only=True,db_number=0,offsetbyte=0,offsetbit=0,tag_value=plcs[plc_ip].robots[robot_number].queue_task.queue[0]['robot_trajectories'],new_value=None,robot_number=robot_number)
                    #                logger.info(f"robot_trajectories {plcs[plc_ip].robots[robot_number].queue_task.queue[0]['robot_trajectories']}") 
                    #            #else:
                    #            #    web_app.queue_tags.queue[0][plc_ip][robot_number]['tsk_robot_trajectories'] = None
                    #        else:
                    #            for tag_name in web_app.queue_tags.queue[0][plc_ip][robot_number]:
                    #                if tag_name.startswith('tsk_'):
                    #                    web_app.queue_tags.queue[0][plc_ip][robot_number][tag_name] = None
            else:
                logger.error(f"web_app.queue_tags.empty() {web_app.queue_tags.empty()}")
            if not web_app.queue_hmi.empty():
                web_status = web_app.queue_hmi.get()
                logger.debug(f"HMI: {web_status}")
                if 'plc_ip' in web_status:
                    #plc = plcs[web_status['plc_ip']]
                    plc_ip = web_status['plc_ip']
                    del web_status['plc_ip']
                    for robot_number in range(1,plcs[plc_ip].robots_count+1):
                        for tag_name in web_status.keys():
                            if tag_name in plcs[plc_ip].robots[robot_number].tags:
                                plcs[plc_ip].robots[robot_number].tags[tag_name]['new_value'] = web_status[tag_name]
                                logger.debug(f"Тег {tag_name}={web_status[tag_name]} поставлен в очередь на запись. ")
                            else:
                                logger.error(f"robot не содержит тега '{tag_name}' среди {len(plcs[plc_ip].robots[robot_number].tags)} тегов!")
        except Exception as error:
            logger.error(f'Ошибка при обновлении web интерфейса {str(error)} {traceback.format_exc()}')
        time.sleep(0.2)


def client_handler(conn, plcs, q):
    """
    получение заданий для робота через сокет (устарело)
    """
    while getattr(threading.currentThread(), "do_run", True):
        # logger.debug(f'Loop start on port: {port} with key: {key}')
        if conn:
            logger.debug("client_handler loop")
            if conn.poll():
                try:
                    msg = conn.recv()
                    msg = json.dumps(msg, ensure_ascii=False, default=int)
                    msg = ast.literal_eval(msg)
                    logger.debug("Получено корректное значение через сокет: " + str(msg))
                except Exception as error:
                    logger.error(
                        f"Получено некорректное сообщение: {msg} {str(e.__class__)} {str(error)} {traceback.format_exc()}")
                else:
                    try:
                        logger.debug(f"Получено сообщение {msg}")
                        if len(msg) > 0:
                            if 'EXIT' in msg:
                                conn.close()
                                q.put(False)
                                logger.info(f"Перезагрузка server.py: {not q.queue[0]}")
                                break
                            elif 'plc_ip' in msg:
                                plc_ip = msg['plc_ip']
                                logger.debug(f"client_handler {sql_conn.database} get robot_task: {robot_task}")
                                assert 'plc_id' in msg, "plc_id не найден в сообщении"
                                if not plc_ip in plcs:
                                    logger.debug(f"plc_ip: {plc_ip} len(plcs) {len(plcs)}")
                                    plc = plc_robot_create(plcs, plc_ip, msg['plc_id'], plc_ip)
                                else:
                                    logger.debug(f"Используем созданный поток с именем: {plc_ip}")
                                    plc = plcs[plc_ip]
                                result = task_append(plc, msg)
                                conn.send(dict(result=result))
                                conn.close()
                                q.put(True)
                                break
                    except Exception as error:
                        logger.info('Error TASK ' + str(e.__class__) + str(e) + str(traceback.format_exc()))
        time.sleep(0.2)

def csv_handler(csv, sql_conn):
    """
    Импорт csv файдов в базу данных
    Загрузка информации о деталях в заказах, размеры, как обрабатывать и т.п.
    """
    logger.info("START CSV PREPARE")
    while getattr(threading.currentThread(), "do_run", True):
        try:
            while not csv.queue_status.empty():
                status_feedback = csv.queue_status.queue[0]
                res = sql_conn.set_status(status_feedback)
                status = csv.queue_status.get()
                assert res, f'status_handler: Пустой ответ'
            csv.check_csv()
        except Exception as error:
            logger.error(f'Ошибка при обработке csv {str(error)} {traceback.format_exc()}')
        time.sleep(5)

def main(port, key):
    """
    Загрузка OPC    
    """
    try:
        #logger = logging.getLogger("opc_py")
        logger.setLevel(logging.DEBUG)
        logger.debug(f"Инициализация port {port}")

        # Перезагрузка модулей
        importlib.reload(plc)
        importlib.reload(robot)
        importlib.reload(service)
        importlib.reload(tags)
        importlib.reload(sql)
        importlib.reload(telegramSQL)
        importlib.reload(web_server)
        importlib.reload(csv_prepare)
        importlib.reload(movie_maker)
        importlib.reload(draw)
        from plc import PLC
        from robot import Robot
        from service import Service
        from tags import Tags
        from sql import SQL
        from telegramSQL import Telegram_SQL_Error

        address = ('localhost', port)
        listener = Listener(address, authkey=key)
        # listener._listener._socket.settimeout(1)
        plcs = dict()
        sql_conn = sql.SQL('mtk_production_db', 'db_admin', 'qazxsw23$', '192.168.31.152')

        sql_errors_address = ('localhost', port + 1)
        sql_errors_listener = Listener(sql_errors_address, authkey=b'expopsw')
        # logger.debug(f'sql_errors connection accepted from {sql_errors_listener.last_accepted}')
        telegram = Telegram_SQL_Error()
        telegram.start()
        sql_errors_thread = threading.Thread(target=telegram_errors_handler, args=(sql_errors_listener, telegram,))
        sql_errors_thread.start()
        
        run = True
        load_plcs(plcs, sql_conn)
    except Exception as error:
        run = False
        logger.error(f"Ошибка инициализации {str(error)} {traceback.format_exc()}. OPC остановлен")

    try:
        status_thread = threading.Thread(target=status_handler, args=(plcs, sql_conn,))
        kwargs = {'host': '0.0.0.0', 'port': port + 2, 'threaded': True, 'use_reloader': False, 'debug': False}
        web_thread = threading.Thread(target=web_server.web_app.run, daemon=True, kwargs=kwargs)
        web_handler_thread = threading.Thread(target=web_handler, args=(plcs, sql_conn, web_server.web_app,))
        web_server.web_app.queue_port.put(kwargs['port'])
        notify_thread = threading.Thread(target=notify_handler, args=(plcs, sql_conn, web_server.web_app,))
        notify_thread.start()
        status_thread.start()
        web_thread.start()
        web_handler_thread.start()
        #csv_thread = threading.Thread(target=csv_handler, args=(csv_prepare, sql_conn,))
        #csv_thread.start()
        logger.debug("Вход в main loop")
        q = Queue()
    except Exception as error:
        run = False
        logger.error(f"Ошибка при запуске status_handler {str(error)} {traceback.format_exc()}. OPC остановлен")

    while run:
        try:
            logger.debug(f"Возвращается к прослушке")
            conn = listener.accept()
            recv_thread = threading.Thread(target=client_handler, args=(conn, plcs, q,))
            recv_thread.start()
            time.sleep(0.2)
            if not q.empty():
                run = q.get()
            else:
                logger.error(f"Нет сообщения в очереди: {q.empty()}")
        except Exception as error:
            # if not e.__class__==socket.timeout:
            logger.error(f'Ошибка подключения {str(e.__class__)} {str(error)} {traceback.format_exc()}')
    try:
        #Выгрузка сервера (автоматическая перезагрузка при обновлении кода)
        #csv_thread.do_run = False
        #logger.debug("Завершается csv_thread")
        #csv_thread.join()
        recv_thread.do_run = False
        logger.debug("Завершается recv_thread")
        recv_thread.join()
        for plc_ip in plcs:
            logger.debug(f"Завершается PLC {plc_ip}")
            plcs[plc_ip].do_run = False
            logger.debug(f"plcs[plc_ip].do_run = False {plc_ip}")
            plcs[plc_ip].join()
            logger.debug(f"plcs[plc_ip].join() {plc_ip}")
        logger.debug("Завершается status_thread")
        status_thread.do_run = False
        status_thread.join()
        logger.debug("Завершается notify_thread")
        notify_thread.do_run = False
        notify_thread.join()
        logger.debug("Завершается web_server")
        web_server.web_app.shutdown()
        web_thread.join()
        logger.debug("Завершается web_handler_thread")
        web_handler_thread.do_run = False
        web_handler_thread.join()
        logger.debug("Завершается telegram")
        telegram.do_run = False
        telegram.join()
        logger.debug("Завершается sql_errors_thread")
        sql_errors_thread.do_run = False
        conn = Client(sql_errors_address, authkey=b'expopsw')
        conn.send("EXIT")
        conn.close()
        sql_errors_thread.join()
        logger.debug(f"server.py полностью завершён {not run}")
    except Exception as error:
        logger.error(f'Ошибка завершения скрипта {str(error)} {traceback.format_exc()}')
