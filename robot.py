"""
class task
29.01.2022
"""

import logging
import traceback
from tags import Tags
from service import Service
from queue import Queue
import time
import os.path

class Robot:
    def __init__(self, plc_thread, tags, robot_number):
        # Init data
        self.logger = logging.getLogger("opc_py")
        self._plc_name = plc_thread.plc_name
        self.plc_tags = plc_thread.plc_tags
        self._robot_number = robot_number
        self.queue_status = plc_thread.queue_status
        self.tags = tags

        # Инициализация переменных
        self.queue_task = Queue()
        self.task_time = 0
        self.status_feedback = None

        self.service = Service(self)
        
        self.logger.info(f"{self._plc_name} Robot{self._robot_number} Task init done")

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

    def process_robot(self):
        """
        
        """
        try:
            self.load_tags()
            self.working()
            #self.save_tags()

            self.service.load_tags()
            self.service.process_service()
            self.service.save_tags()
            
        except Exception as error:
            self.logger.error(f"{self._plc_name} Robot{self._robot_number}: Не удалось обработать цикл: process_robot не сработал для task {task}. "
                              f"{str(error)} {traceback.format_exc()} status_feedback {self.status_feedback}")

    def working(self):
        """
        Обработка задания робота 
        """
        try:
            #Если нет задания, то выход
            if self.queue_task.empty():
                return False
            self.load_tags()

            task=self.queue_task.queue[0]
            # 0. Проверка robot_task_id задания в PLC TODO проверка всех ключей id задания
            if self.robot_task_id != task['robot_task_id'] and task.get('loading','')=='done':
                self.logger.error(f"{self._plc_name} Robot{self._robot_number}: разный robot_task_id в задании {task['robot_task_id']} и в PLC {self.robot_task_id}")
                self.release_task()
                return False
            # 0. Проверка robot_task_id
            #if self.robot_task_id == task['robot_task_id'] and self.task_status=='finish':
            #    self.logger.error(f"{self._plc_name} Robot{self._robot_number}: задание  {self.robot_task_id } выполнено ")
            #    self.release_task()
            #    return False
            
            # 1. Отправляем задание в PLC
            # Проверка времени загрузки. сброс, если привышено 
            # TODO task_time
            if 'task_load_time' in task and task.get('loading','')=='begin':
                if (time.time() - task['task_load_time'])>7:
                    self.tags['reset_job']['new_value']=True
                    task['error']=f"{self._plc_name} Robot {self._robot_number}: Задание {self.robot_task_id} (тип {self.operation_type}) для детали {self.part_id} (статус {self.part_status}) не загружено вовремя!"
                    self.queue_status.put(dict(query='write_error_log',error=task['error']))
                    self.logger.error(f"{task['error']}")
                    return False
            # Загрузка задания. Если не отправлено - отправить, иначе загрузка завершена
            if task.get('loading','')=='middle':
                if self.task_status  in ['not_sended','sended']:
                    self.tags['task_status']['new_value']='sended'
                else:
                    task['loading']='done'
            # Начало загрузки задания
            if (self.task_status == task['task_status'] or task['task_status']=='not_sended') and task.get('loading','')=='begin':
                #self.queue_task.queue[0]['loading']='middle'
                task['loading']='middle'
                self.tags['task_status']['new_value']='sended'
                #self.plc_tags.write_tag(self._robot_number,'task_status','sended')
                #self.task_status='sended'
            # Запись тегов в PLC
            if (not (task.get('loading','') in ['middle','done'])):
                for tag_name in self.tags.keys():
                    if self.tags[tag_name]['use'] in ['In','InOut'] and tag_name in task.keys():
                        self.tags[tag_name]['new_value']=task[tag_name]
                        #setattr(self, tag_name, task[tag_name])
                        #self.plc_tags.write_tag(self._robot_number,tag_name,task[tag_name])
                #self.plc_tags.write_tag(self._robot_number,'robot_task_id',0)
                #self.queue_task.queue[0]['loading']='begin'
                task['loading'] = 'begin'
                task['task_load_time'] = time.time()
                #self.logger.debug(f"{self._plc_name} Robot{self._robot_number}: task_status: PLC {self.task_status} task {task['task_status']}")
                self.logger.debug(f"{self._plc_name} Robot{self._robot_number}: Новое задание {task['robot_task_id']} загружается ({self.queue_task.queue[0].get('loading','')}) в PLC")
            # 2. Отправка старта задания в PLC
            # Проверка правильности загрузки тегов в PLC 
            elif self.task_status == 'sended':
                send_ok = True
                for tag_name in self.tags.keys():
                    if tag_name in task.keys():
                        if not tag_name in ['not_sended2bhx','task_status']:
                            if (self.tags[tag_name]['tag_value']!=task[tag_name]):
                                if self.tags[tag_name]['type'] in ('Real', 'UInt'):
                                    if int(self.tags[tag_name]['tag_value'])==int(task[tag_name]):
                                        continue
                                self.logger.debug(f"{tag_name} <{self.tags[tag_name]['type']}> tags {self.tags[tag_name]['tag_value']} != task {task[tag_name]}")
                                self.tags[tag_name]['new_value']=task[tag_name]
                                send_ok = False
                                #if 'loading' in task:
                                #    del task['loading']
                #Проверка правильности загрузки программы в станок 
                if send_ok and task.get('program_fullpath','').lstrip()!='' and not task['w/o_machine_mode']:
                    try:
                        mpr_found = mirror_found = mpr_loaded = False
                        machine_ready = self.machine_status=='ready2load'
                        if not 'sended2bhx' in task and not machine_ready:
                            task['sended2bhx']=task['not_sended2bhx']
                            self.tags['not_sended2bhx']['new_value']=task['not_sended2bhx']
                        if not 'machine_ready' in task and not machine_ready:
                            task['machine_ready']=machine_ready
                            task['error']=f"{self._plc_name} Robot{self._robot_number}: Станок не в автоматическом режиме!"
                            self.queue_status.put(dict(query='write_error_log',error=task['error']))
                            self.logger.error(f"{task['error']}")
                        if 'machine_ready' in task and machine_ready:
                            self.tags['not_sended2bhx']['new_value']=task['not_sended2bhx']
                            self.logger.debug(f"{self._plc_name} Robot{self._robot_number}: Файл {task['program_fullpath'].lstrip()} отправлен на загрузку в станок заново.")
                            del task['machine_ready']
                        program_fullpath="/mnt/bhx1mpr/"+task['program_fullpath'].lstrip()+".mpr"
                        program_alias="Zakazi/RobotMPR/"+task['program_fullpath'].lstrip()+".mpr"
                        mirror_alias="D:/control/M60-C1/data/cnc/prg/"+task['program_fullpath'].lstrip()
                        mpr_found = os.path.exists(program_fullpath)
                        if not 'mpr_found' in task and not mpr_found:
                            task['mpr_found']=mpr_found
                            task['error']=f"{self._plc_name} Robot{self._robot_number}: Файл {program_alias} не найден на сетевом диске!"
                            self.queue_status.put(dict(query='write_error_log',error=task['error']))
                            self.logger.error(f"{task['error']}")
                        if 'mpr_found' in task and mpr_found:
                            self.tags['not_sended2bhx']['new_value']=task['not_sended2bhx']
                            self.logger.debug(f"{self._plc_name} Robot{self._robot_number}: Файл {task['program_fullpath'].lstrip()} отправлен на загрузку в станок заново.")
                            del task['mpr_found']
                        if machine_ready and mpr_found:
                            mirror_fullpath="/mnt/bhx1/"+task['program_fullpath'].lstrip()
                            mirror_found = os.path.exists(mirror_fullpath)
                            if 'mpr_load_time' in task and not mirror_found:
                                if (time.time() - task['mpr_load_time'])>30:
                                    self.tags['not_sended2bhx']['new_value']=task['not_sended2bhx']
                                    task['error']=f"{self._plc_name} Robot{self._robot_number}: Файл {mirror_alias} не найден на станке!"
                                    self.queue_status.put(dict(query='write_error_log',error=task['error']))
                                    self.logger.error(f"{task['error']}")
                                    #self.logger.debug(f"{self._plc_name} Robot{self._robot_number}: Файл {task['program_fullpath'].lstrip()} отправлен на загрузку в станок заново.")
                                    del task['mpr_load_time']
                            if not 'mirror_found' in task and not mirror_found:
                                task['mirror_found']=mirror_found
                                task['mpr_load_time']=time.time()
                                #task['error']=f"{self._plc_name} Robot{self._robot_number}: Файл {mirror_alias} не найден на станке!"
                                #self.queue_status.put(dict(query='write_error_log',error=task['error']))
                                #self.logger.error(f"{task['error']}")
                            if mirror_found:
                                modify_elapsed = time.time() - os.stat(mirror_fullpath).st_mtime
                                #self.logger.info(f"{self._plc_name} Robot{self._robot_number}: modify_elapsed {modify_elapsed}")
                                mpr_loaded  = modify_elapsed<30
                                if not 'mpr_loaded' in task and not mpr_loaded:
                                    task['mpr_loaded']=mpr_loaded
                                    self.tags['not_sended2bhx']['new_value']=task['not_sended2bhx']
                                    task['error']=f"{self._plc_name} Robot{self._robot_number}: Файл {program_alias} не загружен в MCC!"
                                    self.queue_status.put(dict(query='write_error_log',error=task['error']))
                                    self.logger.error(f"{task['error']}")
                        send_ok = machine_ready and mpr_found and mirror_found and mpr_loaded
                    except Exception as error:
                        pass
                        self.logger.error(f"{self._plc_name} Robot{self._robot_number}: Не удалось обработать загрузку управляющей программы в станок: {str(error)}")
                if send_ok:
                    self.tags['task_status']['new_value']='received'
                    #self.plc_tags.write_tag(self._robot_number,'task_status','received')
                    #self.task_status='received'
                    self.logger.debug(f"{self._plc_name} Robot{self._robot_number}: Задание {task['robot_task_id']} успешно загружено в PLC ({task.get('program_fullpath','')})")
            # 3. Отправляем статус и стадию, если изменились
            if task.get('loading','')=='done' and self.task_status != 'finish' and (not self.status_feedback or ('task_status' in self.status_feedback and self.status_feedback['task_status'] != self.task_status and self.task_status != 'finish') or ('part_status' in self.status_feedback and self.status_feedback['part_status'] != self.part_status)):
                task['last_change_time']=time.time()
                self.status_feedback = dict(query='set_robot_task_status', 
                                            robot_task_id=self.robot_task_id,
                                            task_status=self.task_status,
                                            operation_type=self.operation_type,
                                            part_destination=self.part_destination,
                                            robot_id=self.robot_id,
                                            part_id=self.part_id,
                                            part_status=self.part_status,
                                            part_side=self.part_side,
                                            part_slot=self.part_slot,
                                            slot_pos_x=self.slot_pos_x,
                                            slot_pos_y=self.slot_pos_y,
                                            slot_pos_z=self.slot_pos_z,
                                            slot_angle_a=self.slot_angle_a,
                                            part_pos_x=self.part_pos_x,
                                            part_pos_y=self.part_pos_y,
                                            part_pos_z=self.part_pos_z,
                                            part_angle_a=self.part_angle_a)
                if self.robot_task_id!=(-1) and self.part_id!=(-1):                            
                    self.queue_status.put(self.status_feedback)
                self.logger.info(f"{self._plc_name} Robot{self._robot_number}: status_feedback. self.task_status = {self.task_status}, self.part_status = {self.part_status}")
            else:
                if 'last_change_time' in task:
                    if (time.time() - task['last_change_time'])>180:
                        task['error']=f"{self._plc_name} Robot {self._robot_number}: Задание {self.robot_task_id} (статус {self.task_status},  тип {self.operation_type}) для детали {self.part_id} (статус {self.part_status}) зависло!"
                        self.queue_status.put(dict(query='write_error_log',error=task['error']))
                        self.logger.error(f"{task['error']}")
                        del task['last_change_time']
            # 4. Завершение задания. Отправка финального статуса задания в PLC
            if (self.task_status == "done"):
                if (time.time() - self.task_time)>5:
                    self.logger.info(f" release done task {self.robot_task_id}")
                    part_id=self.part_id
                    self.logger.info(f" status_feedback = {self.status_feedback}")
                    self.release_task()
                    if self.operation_type != "go_home":
                        self.queue_status.put(dict(query='gen_robot_task', part_id=part_id, robot_id=self.robot_id))
                else:
                    self.logger.error(f"ЗАДАНИЕ <{self.robot_task_id} {self.operation_type}> ЗАВЕРШИЛОСЬ СЛИШКОМ РАНО! ЗА {(time.time() - self.task_time)} секунд")
                    self.logger.info(f" При этом после read_tag self.task_status = {self.task_status}, self.operation_type = {self.operation_type}")
                    self.tags['task_status']['new_value']='sended'
                    #self.plc_tags.write_tag(self._robot_number,'task_status','sended')
                    #self.task_status='sended'                    
            return True
        except Exception as error:
            self.logger.error(f"{self._plc_name} Robot{self._robot_number}: Не удалось обработать задание: working не сработал для task {task}. "
                              f"{str(error)} {traceback.format_exc()} status_feedback {self.status_feedback}")
            self.release_task()

    def release_task(self):
        """
        Сброс очереди заданий
        """
        try:
            while not self.queue_task.empty():
                self.queue_task.get()
            self.logger.debug(f"{self._plc_name} Robot{self._robot_number}: Очередь очищена")
            self.tags['task_status']['new_value']='finish'
            #сброс флага нового QR-кода
            self.tags['new_code']['new_value']=False
            self.status_feedback = None
        except Exception as error:
            self.logger.error(f"{self._plc_name} Robot{self._robot_number}: Не удалось сбросить задание: release_task не сработал")
                              #f"{str(error)} {traceback.format_exc()}")

    def load_robot_task(self):
        """
        Загрузка текущего не завершённого задания из PLC
        """
        try:
            self.load_tags()
            if self.robot_task_id!=0:
                #self.logger.info(f"{self._plc_name} Robot{self._robot_number}: Загрузка задания из PLC")
                #self.logger.info(f"{self._plc_name} Robot{self._robot_number}: task_status={self.task_status},robot_task_id={self.robot_task_id},part_id={self.part_id},robot_id={self.robot_id}")
                if (self.task_status in ['in_process', 'new_trajectory', 'on_trajectory', 'done']) and self.part_status!='' and self.robot_task_id!=0 and self.robot_id!=0 and self.part_id!=-1:
                    task = dict(task_status=self.task_status,part_status=self.part_status,robot_task_id=self.robot_task_id,part_id=self.part_id,robot_id=self.robot_id,loading='done')
                    while not self.queue_task.empty():
                        self.queue_task.get()
                    self.queue_task.put(task)
                    self.task_time = time.time()-5
                    self.logger.info(f"{self._plc_name} Robot{self._robot_number}: Задание {task} загружено из PLC" )
                    return True
        except Exception as error:
            self.logger.error(f"Ошибка загрузки задания {self._plc_name} Robot{self._robot_number}. "
                              f"Ошибка {str(error)} {traceback.format_exc()}")
        return False