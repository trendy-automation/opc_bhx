"""
class Service
27.01.2022
"""
 
import time
import logging
import traceback
from queue import Queue
from movie_maker import *

import re

from logger import logger

class Service:
    def __init__(self, robot):
        # Init data
        self.logger = logging.getLogger("opc_py")
        self.robot = robot
        self.plc_tags = robot.plc_tags
        self._plc_name = robot._plc_name
        self._robot_number = robot._robot_number
        self.queue_task = robot.queue_task        
        self.queue_status = robot.queue_status        
        self.tags = robot.tags
        self.XYA_codes = []
        #self.plc_tags = plc_thread.plc_tags
        #self.tags = plc_thread.plc_tags.queue_tags.queue[0]
        
        # Инициализация переменных
        self.query_time = 0

        self.logger.info(f"{self._plc_name} Robot{self._robot_number} Service init done")

    def load_tags(self):
        """
        Загрузка (копирование) тегов в атрибуты класса сервис
        """
        try:
            for tag_name, tag in self.tags.items():
                if tag['new_value'] is None:
                    setattr(self, tag_name, tag['tag_value'])
                else:
                    setattr(self, tag_name, tag['new_value'])
        except Exception as error:
            self.logger.error(f"{self._plc_name} Robot{self._robot_number}: Не удалось загрузить теги. "
                              f"{str(error)} {traceback.format_exc()}")

    def save_tags(self):
        """
        Сохранение (запись) атрибутов в теги
        """
        try:
            for tag_name, tag in self.tags.items():
                if tag['tag_value']!=getattr(self, tag_name):
                    #self.plc_tags.write_tag(self._robot_number,tag_name,getattr(self, tag_name))
                    self.tags[tag_name]['new_value']=getattr(self, tag_name)
                
        except Exception as error:
            self.logger.error(f"{self._plc_name} Robot{self._robot_number}: Не удалось сохранить теги. "
                              f"{str(error)} {traceback.format_exc()}")

    def process_service(self):
        """
        Обработка тегов
        """
        try:
            #Нажата кнопка старт, начало работы
            if self.start_job and not self.new_pallet and not self.reset_job:
                if self.robot.queue_task.empty() or (self.task_status in ['received', 'not_sended', 'sended']) and self.robot_status!='manual_mode':
                    self.queue_status.put(dict(query='gen_robot_task', part_id=-1, robot_id=self.robot_id))
                    self.tags['start_job']['new_value']=False
                else:
                    self.logger.info(f"Старт не выполнен. queue_task.empty() {self.robot.queue_task.empty()} task_status {self.task_status} robot_status {self.robot_status}")
            #Обнуление поддона (пока не используется)
            if self.new_pallet:
                self.queue_status.put(dict(query='reset_robot_pallet',new_pallet=True, robot_id=self.robot_id))
                self.tags['new_pallet']['new_value']=False
            #Сброс задания робота. Очистка очереди queue_task
            if self.reset_job:
                self.logger.debug(f"reset_job {self.reset_job}: Сброс очереди. ")
                self.robot.release_task()
                self.tags['reset_job']['new_value']=False
                if not self.robot_in_home:
                    robot_task_id=-1
                    part_id=-1
                    part_status=''
                    #текущее задание
                    if not self.robot.queue_task.empty():
                        robot_task_id=self.robot_task_id
                        part_id=self.part_id
                        part_status=self.part_status
                    self.queue_status.put(dict(query='reset_robot_task', robot_id=self.robot_id, robot_task_id=robot_task_id, part_id=part_id, part_status=part_status))
                self.tags['reset_job']['new_value']=False
            #Отсканирован новый(е) QR код(ы)
            if self.new_code:
                self.tags['new_code']['new_value']=False
                XYA_codes=[]
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
                #Сканирование паллеты завершено
                if self.pallet_scanned and self.XYA_codes:
                    self.logger.info(f"Отсканированы новые QR коды {self.XYA_codes} pallet_scanned {self.pallet_scanned}")
                    self.queue_status.put(dict(query='process_xyacodes',XYA_codes=self.XYA_codes, scan_lay_number=self.scan_lay_number,robot_id=self.robot_id))
                    self.tags['pallet_scanned']['new_value']=False
                    self.XYA_codes = []
                #Для конвейора используется другая процедура
                if self._plc_name=="Conveyor":
                    if len(XYA_codes)==2:
                        self.logger.info(f"{self._plc_name} Отсканированы новые QR коды {self.XYA_codes}")
                        self.queue_status.put(dict(query='process_label',XYA_codes=self.XYA_codes, robot_id=self.robot_id))
                    self.XYA_codes = []
                #if self._plc_name=="PushPuzzle":
                #    self.logger.info(f"{self._plc_name} pack_parts")
                #    if len(XYA_codes) == 1 and XYA_codes[0] == "Failure":
                #        self.logger.info(f"{self._plc_name} pack_parts part_slot")
                #        self.queue_status.put(dict(query='pack_parts',  part_source='part_slot', part_destination='pallet_out', robot_id=self.robot_id))
                #    else:
                #        self.logger.info(f"{self._plc_name} pack_parts pallet_in")
                #        self.queue_status.put(dict(query='pack_parts', part_source='pallet_in', part_destination='part_slot', robot_id=self.robot_id))                    
                self.logger.debug(f"{self._plc_name}: QR коды считаны: {self.qr_codes_dad}->{self.XYA_codes}, pallet_scanned {self.pallet_scanned}")
            #Сброс задания
            if (self.robot_task_id==0) and not self.robot.queue_task.empty() and 'loaded' in self.robot.queue_task.queue[0]:
                self.logger.debug(f"{self._plc_name} Robot{self._robot_number}: self.robot_task_id==0 -> release_task")
                self.robot.release_task()
            #Проверка выбора следующей траектории в PLC
            if not self.robot.queue_task.empty() and (self.task_status in ['received', 'on_trajectory', 'new_trajectory']) and self.robot_status!='manual_mode':
                if (time.time() - self.query_time)>60 or self.query_time==0:
                    self.robot.queue_task.queue[0]['robot_trajectory']=''
                    self.queue_status.put(dict(query='expected_robot_trajectory',previous_program=self.robot_program, machine_status=self.machine_status, robot_status=self.robot_status, part_status=self.part_status, part_destination=self.part_destination, operation_type=self.operation_type, lay_number=self.lay_number, operation_side=self.operation_side, part_side=self.part_side, robot_number=self._robot_number))
                    self.query_time = time.time()
            else:
                if not self.robot.queue_task.empty() and self.query_time!=0:
                    if 'robot_trajectories' in self.robot.queue_task.queue[0]:
                        self.robot.queue_task.queue[0]['robot_trajectories'] = None
                        self.query_time = 0
            return True
        except Exception as error:
            self.logger.error(f"{self._plc_name} {self.robot_name}: Не удалось обработать теги: "
                              f"{str(error)} {traceback.format_exc()}")
        return False