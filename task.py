"""
class task
01.06.2021
"""
 
import threading
#from time import sleep
import time
import traceback
from queue import Queue
import snap7
import numpy
import re
from snap7.util import *
from movie_maker import *

import re

from logger import logger

class ThreadPLC(threading.Thread):
    def __init__(self, plc_ip, plc_id, plc_name="", robot_id=0, camera_number=2):
        # init
        threading.Thread.__init__(self, args=(), name=plc_ip, kwargs=None)
        # self.logger = logging.getLogger("opc_py")
        # self.logger.setLevel(logging.DEBUG)
        self.logger = logger
        snap7.client.logger.setLevel(logging.INFO)
        self.plc_ip = plc_ip
        self.plc_id = plc_id
        self.robot_id = robot_id
        self.connection_ok = False

        # Пробрасываю в Task ПЛК, очередь и имя
        self.plc = snap7.client.Client()
        self.queue_status = Queue()
        self.plc_name = plc_name
        self.camera_number = camera_number

        self.robot_task=None

    def run(self):
        self.logger.info(f"{self.plc_name} started")
        cur_thread = threading.currentThread()
        # Основной цикл
        while getattr(cur_thread, "do_run", True):
            time.sleep(0.2)

            if not self.plc.get_connected():
            # Подключение к контроллеру
                try:
                    self.plc.connect(self.plc_ip, 0, 1)
                    self.connection_ok = True
                    self.logger.info(f"Соединение Открыто {self.plc_ip} {self.plc_name}")
                    self.robot_task=Task(self)
                    snap7.client.logger.disabled = False
                    #snap7.common.logger.disabled = False
                    self.queue_status.put(dict(query='process_plc_status',plc_status='connected',plc_id=self.plc_id))
                except Exception as error:
                    #self.logger.info(f"Соединение закрыто {self.plc_ip} {self.plc_name} connection_ok {self.connection_ok}")
                    if self.connection_ok:
                        self.plc.disconnect()
                        self.connection_ok=False
                        self.logger.error(f"Не получилось подключиться к контроллеру: {self.plc_ip} {self.plc_name}. "
                                          f"Ошибка {str(error)} {traceback.format_exc()}")
                    snap7.client.logger.disabled = True
                    #snap7.common.logger.disabled = True
                    #if self.robot_task.task:
                    self.queue_status.put(dict(query='process_plc_status',plc_status='unreachable',plc_id=self.plc_id))
                    # Очистка задания
                    self.robot_task.release_task()
                    time.sleep(60)


            else: #if self.plc.get_connected():
            # Подключение активно
                if not self.robot_task.loading_done:
                # Загрузка тегов 
                    self.logger.debug(f"Загрузка тегов из PLC...")
                    self.robot_task.load_PLC_tags()
                    assert self.robot_task.write_tag('plc_id',self.plc_id) , "Не записан тег plc_id"
                    assert self.robot_task.write_tag('plc_ip',self.plc_ip) , "Не записан тег plc_ip"
                    assert self.robot_task.write_tag('plc_name',self.plc_name) , "Не записан тег plc_name"
                    assert self.robot_task.write_tag('robot_id',self.robot_id) , "Не записан тег robot_id"
                    if not self.robot_task.task:
                        self.robot_task.load_robot_task()
                else: #if self.robot_task.tags:
                # Загрузка завершена 
                    # Чтение состояния робота и QR кода
                    #self.robot_task.update_plc_tags()
                    if not self.robot_task.update_plc_tags():
                        self.connection_ok=False
                        self.queue_status.put(dict(query='process_plc_status',plc_status='disconnected',plc_id=self.plc_id))
                        self.plc.disconnect()
                        self.robot_task=None
                        continue
                    if not self.robot_task.task is None:
                    # Выполнение задания
                        self.robot_task.working()
                    else: #if not self.robot_task.task:
                        #if self.robot_task.machine_status=='ready' and self.queue_status.empty():
                        if self.queue_status.empty(): #and not self.robot_task.queue_task.empty():
                        # Очередь исходящих статусов пуста и есть задания в очереди, можно запрашивать задания
                            try:
                            # Запускаем задания стоящие в очереди, если очередь статосов пустая, сервер всё забрал
                                new_task = self.robot_task.get_task()
                            except Exception as error:
                                self.logger.error(f"Не получилось запустить следующее задание {self.plc_name} {self.robot_task.robot_name}. "
                                                  f"Ошибка {str(error)} {traceback.format_exc()}")
                            if new_task:
                                self.logger.info(f"Задание {self.robot_task.robot_task_id} "
                                                 f"принято в работу {self.plc_name} {self.robot_task.robot_name}")
                            #else:
                            #    if self.robot_task.new_qr_code:
                            #    # Проверка отсканирована ли новая деталь
                            #        try:
                            #            self.robot_task.new_qr_code = False
                            #            self.logger.info(f"Отсканированы QR коды {self.robot_task.XYA_codes}")
                            #            #self.queue_status.put(dict(QR_code=self.robot_task.QR_code,plc_id=self.plc_id))
                            #            self.queue_status.put(dict(gen_robot_task=(self.robot_task.robot_task_id==(-1)),XYA_codes=self.robot_task.XYA_codes, scan_lay_number=self.robot_task.scan_lay_number,scan_robot_pos_x=self.robot_task.scan_robot_pos_x,scan_robot_pos_y=self.robot_task.scan_robot_pos_y,scan_robot_pos_z=self.robot_task.scan_robot_pos_z,scan_robot_angle_a=self.robot_task.scan_robot_angle_a,plc_id=self.plc_id))
                            #        except Exception as error:
                            #            self.logger.error(f"Не получилось проверить, отсканирована ли новая деталь {self.plc_name} {self.robot_task.robot_name}. "
                            #                              f"Ошибка {str(error)} {traceback.format_exc()}")

class Task:
    def __init__(self, plc_thread):
        # Init data
        self.logger = logging.getLogger("opc_py")
        self.plc = plc_thread.plc
        self.plc_name = plc_thread.plc_name
        self.plc_ip = plc_thread.plc_ip
        self.plc_id = plc_thread.plc_id
        self.robot_name = ''
        self.camera_number = plc_thread.camera_number
        self.queue_status = plc_thread.queue_status
        self.queue_task = Queue()

        # Инициализация переменных
        self.task = None
        self.task_status = None
        self.machine_status = 'busy'
        self.part_id = None
        self.robot_task_id = None
        self.order_operation_id = None
        self.status_feedback = None
        self.loading_done = False
        self.tags = dict()
        self.db_tags = 1000
        self.tag_size = 78
        self.name_size = 30+2
        self.type_size = 30+2
        self.use_size = 5+2
        self.read_only_size = 1
        self.db_number_size = 2
        self.offsetbyte_size = 2
        self.tag_start_bytes = [0]
        cur_offset=0
        for size in [self.name_size,self.type_size,self.use_size,self.read_only_size,self.db_number_size,self.offsetbyte_size]:
            cur_offset+=size
            self.tag_start_bytes.append(cur_offset)
        self.new_code = False
        self.qr_codes_dad = ''
        self.XYA_codes = []
        self.new_qr_code = False

        self.massa = {
            "USInt": 1,
            "UInt": 2,
            "Int": 2,
            "DInt": 4,
            "LInt": 8
        }

        self.set_int_function = {
            "USInt": set_usint,
            "UInt": set_int,
            "Int": set_int,
            "DInt": set_int,
            "LInt": self.set_lint
        }

        self.get_int_function = {
            "USInt": get_usint,
            "UInt": get_int,
            "Int": get_int,
            "DInt": get_int,
            "LInt": self.get_lint
        }
        
        self.query_time = 0

        self.logger.info(f"{self.plc_name} Task init done")

    def load_PLC_tags(self):
        try:
            tags_count = get_int(self.plc.db_read(self.db_tags, 0, 2),0)
            for n in range(tags_count):
                tag_data = self.plc.db_read(self.db_tags, n*self.tag_size+2, self.tag_size)
                tag_name = get_string(tag_data,self.tag_start_bytes[0],self.name_size)
                tag_type = get_string(tag_data,self.tag_start_bytes[1],self.type_size)
                use = get_string(tag_data,self.tag_start_bytes[2],self.use_size)
                read_only = bool(get_int(tag_data,self.tag_start_bytes[3]))
                db_number = get_int(tag_data,self.tag_start_bytes[4])
                offsetbyte = get_int(tag_data,self.tag_start_bytes[5])
                offsetbit = get_usint(tag_data,self.tag_start_bytes[6])
                self.tags[tag_name]=dict(type=tag_type,use=use,read_only=read_only,db_number=db_number,offsetbyte=offsetbyte,offsetbit=offsetbit,tag_value='',write_value=False)
                #self.logger.debug(f"Получен тег {tag_name} {self.tags[tag_name]}")
            for tag_name in self.tags.keys():
                assert self.read_tag(tag_name), f"Не считано значение тега {tag_name} {self.tags[tag_name]}"
                #    self.logger.debug(f"Считано значение тега {tag_name} = {getattr(self, tag_name)}")
            self.logger.info(f"{self.plc_name}: Загружено {tags_count} тега(-ов)")
            self.loading_done = True
            # self.tags_count_ = tags_count
            return True
        except Exception as error:
            self.logger.error(f"Ошибка получения тегов {self.plc_name}. "
                              f"Ошибка {str(error)} {traceback.format_exc()}")
            return False

    def read_tag(self,tag_name):
        try:
            if tag_name in self.tags:
                tag_type=self.tags[tag_name]['type']
                db_number=self.tags[tag_name]['db_number']
                offsetbyte=self.tags[tag_name]['offsetbyte']
                offsetbit=self.tags[tag_name]['offsetbit']
                if tag_type=='Bool':
                    tag_value = get_bool(self.plc.db_read(db_number, offsetbyte, 1),0,offsetbit)
                elif tag_type=='Real':
                    tag_value = get_real(self.plc.db_read(db_number, offsetbyte, 4),0)
                elif "Int" in tag_type and tag_type in self.massa:
                    tag_value = self.get_int_function[tag_type](self.plc.db_read(db_number, offsetbyte, self.massa[tag_type]), 0)
                elif tag_type=='Char':
                    tag_value = self.plc.db_read(db_number, offsetbyte, 1).decode()
                elif 'String' in tag_type:
                    #data = re.findall(r"(\w+)", tag_type)
                    #len_arr = int(data[1]) if len(data) > 1 else 254
                    len_arr = int(tag_type[7:-1])
                    byte_array_read = self.plc.db_read(db_number, offsetbyte, len_arr)
                    tag_value = get_string(byte_array_read, 0, len_arr)
                    if byte_array_read[0 + 1]>len_arr:
                        self.logger.info(f"Считывание тега '{tag_name}'={tag_value} type {tag_type} из {self.plc_name} size = {byte_array_read[0 + 1]} max_size {len_arr}")
                elif 'Array[0..512] of Byte' in tag_type:
                    #tag_value = get_string(self.plc.db_read(db_number, offsetbyte, 512), 0, 512)
                    tag_data = self.plc.db_read(db_number, offsetbyte, 512)
                    tag_value = tag_data.rstrip(b'\x00').decode()
                    #self.logger.info(f"Считано значение тега {tag_name} = {tag_value}")
                else:
                    self.logger.info(f"Неизвестный тип тега для чтения {tag_name} - '{tag_type}'")
                    return False
                #self.logger.info(f"Считано значение тега {tag_name} = {tag_value}")
                setattr(self, tag_name, tag_value)
                self.tags[tag_name]['tag_value']=tag_value
                return True
            else:
                self.logger.info(f"Не найден тег {tag_name}")
                return False
        except Exception as error:
            self.logger.error(f"Ошибка считывания тега '{tag_name}' из {self.plc_name} "
                              f"Ошибка {str(error)} {traceback.format_exc()}")
            return False

    def write_tag(self,tag_name,tag_value):
        try:
            if tag_name in self.tags:
                if tag_value is None:
                    self.logger.info(f"None значение тега {tag_name} для записи")
                    return False                
                tag_type=self.tags[tag_name]['type']
                db_number=self.tags[tag_name]['db_number']
                offsetbyte=self.tags[tag_name]['offsetbyte']
                offsetbit=self.tags[tag_name]['offsetbit']
                if tag_type == 'Bool':
                    tag_data = self.plc.db_read(db_number, offsetbyte, 1)
                    set_bool(tag_data, 0, offsetbit, bool(tag_value))
                elif tag_type == 'Real':
                    tag_data = bytearray(4)
                    set_real(tag_data, 0, tag_value)
                elif "Int" in tag_type and tag_type in self.massa:
                    tag_data = bytearray(self.massa[tag_type])
                    self.set_int_function[tag_type](tag_data, 0, tag_value)
                elif tag_type == 'Char':
                    tag_data = bytearray(1)
                    tag_data = bytes(tag_value[0],"ascii")
                elif 'String' in tag_type:
                    #data = re.findall(r"(\w+)", tag_type)
                    #len_arr = int(data[1]) if len(data) > 1 else 254
                    #data = re.findall(r"^String\[(\d+)\]$", tag_type)
                    #len_arr = int(data[0])
                    len_arr = int(tag_type[7:-1])
                    tag_value = f"%.{len_arr}s" % tag_value
                    tag_data = bytearray(len_arr + 2)
                    set_string(tag_data, 0, tag_value, len_arr)
                    tag_data[0] = numpy.uint8(len(tag_data))
                    tag_data[1] = numpy.uint8(len(tag_value))
                else:
                    self.logger.info(f"Неизвестный тип тега для записи {tag_name} - '{tag_type}'")
                    return False

                self.plc.db_write(db_number, offsetbyte, tag_data)
                return True
            else:
                self.logger.info(f"{self.plc_name}: Не найден тег {tag_name}")
                return False
        except Exception as error:
            self.logger.error(f"Ошибка записи тега  {tag_name} - '{tag_type}'=<{tag_value}> DB {db_number} {offsetbyte}.{offsetbit} ")
                              #f"Ошибка {str(error)} {traceback.format_exc()}")
            return False

    def working(self):
        try:
            if self.task:
                # 1. Отправляем задание в PLC
                if self.task_status == 'not_sended':
                    if self.post_task():
                        assert self.write_tag('task_status','sended'), "Статус задания 'sended' не записан в PLC"
                # 2. Отправка старта задания в PLC
                elif self.task_status == 'sended':
                    send_ok = True
                    for tag_name in self.tags.keys():
                        if tag_name in self.task.keys():
                            #send_ok = send_ok and getattr(self, tag_name,"")==self.task[tag_name]
                            if (self.tags[tag_name]['tag_value']!=self.task[tag_name]) and not tag_name in ['not_sended2bhx','task_status']:
                                if self.tags[tag_name]['type']=='Real':
                                    if int(self.tags[tag_name]['tag_value'])==int(self.task[tag_name]):
                                        continue
                                self.logger.debug(f"{tag_name} tags {self.tags[tag_name]['tag_value']} - task {self.task[tag_name]}")
                                send_ok = False
                    if send_ok:
                        assert self.write_tag('task_status','received'), "Статус задания 'received' не записан в PLC"
                # 3. Отправляем статус и стадию, если изменились
                if not self.status_feedback or (('task_status' in self.status_feedback and self.status_feedback['task_status'] != self.task_status) or ('part_status' in self.status_feedback and self.status_feedback['part_status'] != self.part_status)):
                    self.status_feedback = dict(query='set_robot_task_status', 
                                                robot_task_id=self.robot_task_id,
                                                task_status=self.task_status,
                                                plc_id=self.task['plc_id'],
                                                robot_id=self.task.get('robot_id',0),
                                                part_id=self.task['part_id'],
                                                part_status=self.part_status,
                                                part_side=self.part_side,
                                                part_slot=self.part_slot,
                                                slot_pos_x=self.slot_pos_x,
                                                slot_pos_y=self.slot_pos_y,
                                                slot_pos_z=self.slot_pos_z,
                                                slot_angle_a=self.slot_angle_a)
                    if self.robot_task_id!=(-1) and self.task['part_id']!=(-1):                            
                        self.queue_status.put(self.status_feedback)
                    self.logger.info(f"{self.plc_name}: status_feedback. self.task_status = {self.task_status}, self.part_status = {self.part_status}")
                # 4. Завершение задания. Отправка финального статуса задания в PLC
                if (self.task_status == "done" or (self.task_status=="not_sended" and self.part_status=='ordered')): 
                #and self.queue_status.empty():
                    #self.logger.info(f"{self.plc_name} {self.robot_name}: Заданние {self.robot_task_id} завершенно")
                    #self.status_feedback = dict(query='set_robot_task_status', 
                    #                            robot_task_id=self.robot_task_id,
                    #                            task_status="finish",
                    #                            plc_id=self.task['plc_id'],
                    #                            robot_id=self.task.get('robot_id',1),
                    #                            part_id=self.task['part_id'],
                    #                            part_status=self.part_status)
                    part_id=self.task['part_id']
                    self.release_task()
                    #if self.robot_task_id!=(-1) and self.part_id!=(-1):
                    #    self.queue_status.put(self.status_feedback)
                    if self.operation_type != "go_home":
                        self.queue_status.put(dict(query='gen_robot_task', part_id=part_id, robot_id=self.robot_id))
                    self.logger.info(f" status_feedback. {self.status_feedback}")
            else:
                self.logger.error(f"{self.plc_name} {self.robot_name}: Запуск Task без задания")
            return True
        except Exception as error:
            self.logger.error(f"{self.plc_name} {self.robot_name}: Не удалось обработать задание: working не сработал для self.task {self.task}. "
                              f"{str(error)} {traceback.format_exc()} status_feedback {self.status_feedback}")
            #if self.task and self.robot_task_id:
            self.release_task()

    def post_task(self):
        try:
            for tag_name in self.task.keys():
                if tag_name in self.tags:
                    assert self.write_tag(tag_name,self.task[tag_name]), f"Не записано в PLC значение тега {tag_name} = {self.task[tag_name]}"
            self.logger.info(f"Заданние в PLC отправленно корректно")
            return True
        except Exception as error:
            self.logger.error(f"{self.plc_name} {self.robot_name}: Не удалось отправить данные в PLC: post_task не сработал"
                              f"{str(error)} {traceback.format_exc()}")
            self.release_task()
            
        self.logger.info(f"Заданние {self.task} отправлено в PLC не корректно  ")
        return False

    def release_task(self):
        try:
            if self.robot_task_id==0 or (not self.queue_task.empty() and 'robot_task_id' in self.queue_task.queue[0] and self.robot_task_id == self.queue_task.queue[0]['robot_task_id']):
                self.task = self.queue_task.get()
                self.logger.debug(f"Сброс задания {self.robot_task_id}")
            else:
                self.logger.debug(f"{self.plc_name}: Обнуление неактуального задания {self.robot_task_id}")
            self.task = None
            self.task_status = None
            self.robot_task_id = None
            self.part_id = None
            #self.status_feedback = None
            assert self.write_tag('task_status','finish'), "Статус 'finish' не записан в PLC"
            ##Обнуление флага сканирования для исключения движения робота
            #self.new_qr_code = False
            assert self.write_tag('new_code',False), "Тег new_code=False не записан в PLC"
        except Exception as error:
            self.logger.error(f"{self.plc_name}: Не удалось сбросить задание: release_task не сработал")
                              #f"{str(error)} {traceback.format_exc()}")

    def load_robot_task(self):
        try:
            #self.logger.debug(f"Загрузка задания из PLC")
            if self.task is None:
                assert self.read_tag('robot_task_id'), "Не считан robot_task_id"
                if self.robot_task_id!=0:
                    assert self.read_tag('operation_type'), "Не считан тип задания"
                    assert self.read_tag('task_status'), "Не считан статус задания"
                    assert self.read_tag('plc_id'), "Не считан plc_id"
                    assert self.read_tag('part_id'), "Не считан part_id"
                    assert self.read_tag('robot_name'), "Не считан robot_name"
                    if ((not (self.task_status in ('finish', 'unknown'))) and self.robot_task_id!=0 and self.plc_id!=0 and self.part_id!=0):
                        #if not self.task_status!='done':
                        #self.status_feedback = dict(robot_task_id=self.robot_task_id, plc_id=self.plc_id, task_status=self.task_status)
                        #self.queue_status.put(self.status_feedback)
                        self.task = dict(task_status=self.task_status,robot_task_id=self.robot_task_id,plc_id=self.plc_id,part_id=self.part_id,robot_name=self.robot_name)
                        self.logger.info(f"Задание {self.task} загружено из PLC " )
                        return True
        except Exception as e:
            self.logger.error(f"Ошибка загрузки задания {self.plc_name} {self.robot_name}. "
                              f"Ошибка {str(e)} {traceback.format_exc()}")
        return False
 
    def get_task(self):
        try:
            if (self.task_status =='done' or self.task_status =='finish' ) and not self.queue_task.empty() and self.task is None:
                task = self.queue_task.queue[0]
                if not all(k in task for k in ("robot_task_id", "operation_side","operation_type","task_status","part_status")):
                    self.logger.error(f"Получено некорректное задание {task}")
                    return False
                self.task = task
                self.robot_name = self.task.get('robot_name', '')
                self.robot_task_id = task['robot_task_id']                
                self.robot_id = task['robot_id']                
                for tag_name in self.tags.keys():
                    if self.tags[tag_name]['use'] in ['In','InOut'] and tag_name in self.task.keys():
                        assert self.write_tag(tag_name,self.task[tag_name]), f"Не записано значение {task[tag_name]} тега {tag_name} {self.tags[tag_name]}"
                        #self.logger.info(f"Задание {self.robot_task_id}, тег  {tag_name} = {self.task[tag_name]} (PLC:{getattr(self,tag_name)}) загружен в PLC " )
                        #assert self.read_tag(tag_name), f"Не считано значение тега {tag_name} {self.tags[tag_name]}"
                        #self.logger.info(f"Задание {self.robot_task_id}, тег  {tag_name} = {getattr(self,tag_name)}  загружен из PLC " )
                return True
        except Exception as e:
            self.logger.error(f"Ошибка получения задания {self.plc_name} {self.robot_name}. "
                              f"Ошибка {str(e)} {traceback.format_exc()}")
        return False

    def update_plc_tags(self):
        try:
            #self.logger.debug(f"Обновление тегов PLC. self.robot_task_id = {self.robot_task_id}")
            for tag_name in self.tags.keys():
                if self.tags[tag_name]['write_value']:
                    assert self.write_tag(tag_name,self.tags[tag_name]['value']), f"Не записано значение {self.tags[tag_name]['value']} тега {tag_name} {self.tags[tag_name]}"
                    self.tags[tag_name]['write_value']=False
                    self.logger.debug(f"{self.plc_name}: Тег {tag_name}={self.tags[tag_name]['value']} записан. ")
                #if self.tags[tag_name]['use'] in ['In','Out','InOut']:
                assert self.read_tag(tag_name), f"Не считано значение тега {tag_name} {self.tags[tag_name]}"
            if self.start_job:
                self.queue_status.put(dict(query='gen_robot_task', part_id=-1, robot_id=self.robot_id))
                assert self.write_tag('start_job',False) , "Не записан тег start_job"
            if self.reset_job:
                robot_task_id=-1
                part_id=-1
                part_status=''
                if self.task:
                    robot_task_id=self.task.get('robot_task_id',-1)
                    part_id=self.task.get('part_id',-1)
                    part_status=self.task.get('part_status','')
                while not self.queue_status.empty():
                    self.queue_status.get()
                self.queue_status.put(dict(query='reset_robot_task', robot_id=self.robot_id, robot_task_id=robot_task_id, part_id=part_id, part_status=part_status))
                assert self.write_tag('reset_job',False) , "Не записан тег reset_job"
            if self.new_pallet:
                self.queue_status.put(dict(query='reset_robot_pallet',new_pallet=True, plc_id=self.plc_id, robot_id=self.robot_id))
                assert self.write_tag('new_pallet',False) , "Не записан тег new_pallet"
            if self.new_code:
                #if self.new_qr_code:
                assert self.write_tag('new_code',False) , "Не записан тег new_code"
                #self.XYA_codes = []
                for QR_code in self.qr_codes_dad.split(';'):
                    if QR_code:
                        data_list = re.split('X|Y|A',QR_code)
                        if not data_list[0]=='Failure':
                            if '-' in data_list[0]:
                                id_num = data_list[0].split('-')
                                part_id = int(id_num[0]) + int(id_num[1]) - 1
                            else:
                                part_id = int(data_list[0])
                        if len(data_list)==4:
                            XYA_code = dict(part_id=part_id,scan_label_pos_x=int(data_list[1]),scan_label_pos_y=int(data_list[2]),scan_label_angle_a=int(data_list[3]),scan_robot_pos_x=self.scan_robot_pos_x,scan_robot_pos_y=self.scan_robot_pos_y,scan_robot_pos_z=self.scan_robot_pos_z,scan_robot_angle_a=self.scan_robot_angle_a)
                        else:
                            self.logger.info(f"Сообщение сканера {QR_code}")
                            XYA_code = QR_code
                        self.XYA_codes.append(XYA_code)
                        #self.new_qr_code = True
                if self.pallet_scanned and self.XYA_codes:
                    if self.scan_lay_number==0:
                        self.scan_lay_number=1
                    self.logger.info(f"Отсканированы новые QR коды {self.XYA_codes} pallet_scanned {self.pallet_scanned}")
                    self.queue_status.put(dict(query='process_xyacodes',gen_robot_task=(self.scan_lay_number==1),XYA_codes=self.XYA_codes, scan_lay_number=self.scan_lay_number,plc_id=self.plc_id,robot_id=self.robot_id))
                    assert self.write_tag('pallet_scanned',False) , "Не записан тег pallet_scanned"
                    self.XYA_codes = []
                if len(self.XYA_codes)==2 and self.plc_name=="Conveyor":
                    self.logger.info(f"{self.plc_name} Отсканированы новые QR коды {self.XYA_codes}")
                    self.queue_status.put(dict(query='process_label',XYA_codes=self.XYA_codes, plc_id=self.plc_id, robot_id=self.robot_id))
                #    self.XYA_codes = []
                if self.plc_name=="Conveyor":
                    self.XYA_codes = []
                self.logger.debug(f"{self.plc_name}: QR коды считаны: {self.qr_codes_dad}->{self.XYA_codes}, pallet_scanned {self.pallet_scanned} ")
            #if (self.robot_task_id==0) and self.task:
            #    self.logger.debug(f"self.robot_task_id==0 -> release_task")
            #    self.release_task()
            
            if (self.task_status=='received' or self.task_status=='on_trajectory'):
                #self.logger.info(f"time.time() {time.time()} - self.query_time {self.query_time}")
                if (time.time() - self.query_time)>1:
                    self.query_time = time.time()
                    self.queue_status.put(dict(query='expected_robot_trajectory',previous_program=self.robot_program, machine_status=self.machine_status, robot_status=self.robot_status, part_status=self.part_status, part_destination=self.part_destination, operation_type=self.operation_type, lay_number=self.lay_number, operation_side=self.operation_side, part_side=self.part_side,plc_ip=self.plc_ip))
                    #self.logger.info(f"select expected_robot_trajectory")
            else:
                if self.task and self.query_time!=0:
                    if 'robot_trajectories' in self.task:
                        self.task['robot_trajectories'] = None
                        self.query_time = 0
                        #self.logger.info(f"release robot_trajectories")
            return True
        except Exception as error:
            self.logger.error(f"{self.plc_name} {self.robot_name}: Не удалось прочитать данные из PLC: "
                              f"{str(error)} {traceback.format_exc()}")
        return False

    def get_lint(self, bytearray_: bytearray, byte_index: int) -> int:
        data = bytearray_[byte_index:byte_index + 8]
        lint = struct.unpack('>q', struct.pack('8B', *data))[0]
        return lint

    def set_lint(self, bytearray_: bytearray, byte_index: int, lint: int):
        lint = int(lint)
        _bytes = struct.unpack('8B', struct.pack('>q', lint))
        for i, b in enumerate(_bytes):
            bytearray_[byte_index + i] = b