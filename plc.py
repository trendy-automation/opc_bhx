"""

class PLC
27.01.2022
"""
 
import threading
import logging
import traceback
from robot import Robot
from tags import Tags
from queue import Queue
import time
import snap7

#from logger import logger

class PLC(threading.Thread):
    def __init__(self, plc_ip, plc_name, robots_count, robot_ids, camera_number=2):
        # init
        threading.Thread.__init__(self, args=(), name=plc_ip, kwargs=None)
        self.logger = logging.getLogger("opc_py")
        #self.logger.setLevel(logging.DEBUG)
        #self.logger = logger
        snap7.client.logger.setLevel(logging.INFO)
        self.plc_ip = plc_ip
        self.plc_name = plc_name
        self.robot_ids = robot_ids
        self.snap7client = snap7.client.Client()
        self.robots_count = robots_count
        self.connection_ok = False
        self.queue_status = Queue()
        #self.tags = self.plc_tags.tags #array of robots
        self.camera_number = camera_number
        self.plc_tags = Tags(self)
        #self.robots=[None]*(self.robots_count+1)
        self.robots=[None]
        for robot_number in range(1,self.robots_count+1):
            #self.robots[robot_number] = Robot(self,self.plc_tags.tags[robot_number],robot_number)
            self.robots.append(Robot(self,self.plc_tags.tags[robot_number],robot_number))
        self.unreachable_time = 0

    def db_read(self,db_number, offsetbyte, len_arr):
        return self.snap7client.db_read(db_number, offsetbyte, len_arr)

    def db_write(self,db_number, offsetbyte, tag_data):
        return self.snap7client.db_write(db_number, offsetbyte, tag_data)

    def run(self):
        self.logger.info(f"{self.plc_name} started")
        cur_thread = threading.currentThread()
        # Основной цикл
        while getattr(cur_thread, "do_run", True):
            if self.unreachable_time == 0 or (time.time() - self.unreachable_time)>600:
                pause=True
                for robot_number in range(1,self.robots_count+1):
                    if not self.robots[robot_number].queue_task.empty():
                        if self.robots[robot_number].queue_task.queue[0].get('loading','')!='done':
                            pause=False
                if pause:
                    time.sleep(0.2)
                if not self.snap7client.get_connected():
                # Подключение к контроллеру
                    try:
                        if self.snap7client.connect(self.plc_ip, 0, 1):
                            self.connection_ok = True
                            self.unreachable_time = 0
                            self.logger.info(f"Соединение Открыто {self.plc_ip} {self.plc_name}")
                            snap7.client.logger.disabled = False
                            self.queue_status.put(dict(query='process_plc_status',plc_status='connected',plc_ip=self.plc_ip))
                            #self.logger.info(f"{self.plc_name} connected, loading_done {self.plc_tags.loading_done}")
                    except Exception as error:
                        if self.connection_ok:
                            self.snap7client.disconnect()
                            self.connection_ok=False
                            self.logger.error(f"Не получилось подключиться к контроллеру: {self.plc_ip} {self.plc_name}. "
                                              f"Ошибка {str(error)} {traceback.format_exc()}")
                        snap7.client.logger.disabled = True
                        self.queue_status.put(dict(query='process_plc_status',plc_status='unreachable',plc_ip=self.plc_ip))
                        ## Очистка заданий
                        #for robot_number in range(1,self.robots_count+1):
                        #    if not self.robots[robot_number] is None:
                        #        self.robots[robot_number].release_task()
                        #        self.robots[robot_number]=None
                        self.unreachable_time = time.time()
                        #time.sleep(600)
                else:
                # Подключение активно
                    if not self.plc_tags.loading_done:
                    # Загрузка тегов 
                        self.logger.debug(f"Загрузка тегов из PLC {self.plc_name}...")
                        self.plc_tags.load_PLC_tags()
                        if 'plc_name' in self.plc_tags.tags[0]:
                            self.plc_tags.tags[0]['plc_name']['new_value']=self.plc_name
                        if 'plc_ip' in self.plc_tags.tags[0]:
                            self.plc_tags.tags[0]['plc_ip']['new_value']=self.plc_ip
                        for robot_number in range(1,self.robots_count+1):
                            if 'robot_id' in self.plc_tags.tags[robot_number]:
                                self.plc_tags.tags[robot_number]['robot_id']['new_value']=self.robot_ids[robot_number-1]
                        self.plc_tags.update_plc_tags()
                        if self.plc_tags.loading_done:
                            try:
                                #self.logger.info(f"{self.plc_name} robots_count {list(range(1,self.robots_count+1))} full {len(self.robots)}")
                                for robot_number in range(1,self.robots_count+1):
                                    self.logger.info(f"{self.plc_name} Robot{robot_number} creating")
                                    self.robots[robot_number].load_robot_task()
                                    #self.robots[robot_number] = Robot(self,self.plc_tags.tags[robot_number],robot_number)
                                    #if self.robots[robot_number].queue_task.empty():
                                    #    #self.logger.info(f"{self.plc_name} Robot{robot_number} robot_task_id {self.robots[robot_number].robot_task_id}")
                                    #    self.robots[robot_number].load_robot_task()
                                    #else:
                                    #    self.logger.info(f"Задание есть {self.plc_name} Robot{robot_number} {self.robots[robot_number].queue_task.queue[0]}")
                            except Exception as error:
                                self.logger.error(f"Не получилось создать объект робот {self.plc_name} Robot{robot_number}. len(tags) {len(self.plc_tags.tags)}  range {list(range(1,self.robots_count+1))} len(robots) {len(self.robots)} robots_count {self.robots_count}"
                                                  f"Ошибка {str(error)} {traceback.format_exc()}")
                        else:
                            self.logger.error(f"Не получилось загрузить теги {self.plc_name}.")
                    else:
                    # Загрузка тегов завершена, загрузка заданий роботов
                        try:
                            #self.logger.info(f"{self.plc_name} connected, loading_done {self.plc_tags.loading_done}")
                            # обновление тегов
                            if not self.plc_tags.update_plc_tags():
                                self.logger.info(f"{self.plc_name} not self.plc_tags.update_plc_tags()")
                                self.connection_ok=False
                                self.plc_tags.loading_done=False
                                self.queue_status.put(dict(query='process_plc_status',plc_status='disconnected',plc_ip=self.plc_ip))
                                self.snap7client.disconnect()
                                for robot_number in range(1,self.robots_count+1):
                                    self.robots[robot_number].release_task()
                                    #self.robots[robot_number] = None
                                continue
                            # Выполнение задания
                            #self.logger.info(f"self.robots[1:]  {self.robots[1:]} robots {self.robots}")
                            for robot_number in range(1,self.robots_count+1):
                                self.robots[robot_number].process_robot()
                        except Exception as error:
                                self.logger.error(f"Не обработан цикл {self.plc_name} Robot{robot_number}. "
                                                  f"Ошибка {str(error)} {traceback.format_exc()}")
