"""
class Tags
27.01.2022
"""

import traceback
import logging
import numpy
from queue import Queue
from collections import defaultdict
from snap7.util import *

class Tags:
    def __init__(self, plc_thread):
        # Init data
        self.logger = logging.getLogger("opc_py")
        self.plc = plc_thread
        self.plc_name = plc_thread.plc_name
        self.robots_count = plc_thread.robots_count
        self.tags=[dict()]*(self.robots_count+1)
        
        # Инициализация переменных
        self.loading_done = False
        self.db_tags = 1000
        #self.tag_size = 78
        self.name_size = 30+2
        self.type_size = 30+2
        self.use_size = 5+2
        self.read_only_size = 1
        self.db_number_size = 2
        self.offsetbyte_size = 2
        self.offsetbit_size = 1
        self.robot_number_size = 1
        self.tag_start_bytes = [0]
        cur_offset=0
        for size in [self.name_size,self.type_size,self.use_size,self.read_only_size,self.db_number_size,self.offsetbyte_size,self.offsetbit_size]:
            cur_offset+=size
            self.tag_start_bytes.append(cur_offset)
        self.tag_size = cur_offset+self.robot_number_size
        
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
        
        self.logger.info(f"{self.plc_name} Tags init done")

    def load_PLC_tags(self):
        try:
            count = self.plc.db_read(self.db_tags, 0, 2)
            tags_count = get_int(count,0)
            for n in range(tags_count):
                tag_data = self.plc.db_read(self.db_tags, n*self.tag_size+2, self.tag_size)
                tag_name = get_string(tag_data,self.tag_start_bytes[0],self.name_size)
                tag_type = get_string(tag_data,self.tag_start_bytes[1],self.type_size)
                use = get_string(tag_data,self.tag_start_bytes[2],self.use_size)
                read_only = bool(get_int(tag_data,self.tag_start_bytes[3]))
                db_number = get_int(tag_data,self.tag_start_bytes[4])
                offsetbyte = get_int(tag_data,self.tag_start_bytes[5])
                offsetbit = get_usint(tag_data,self.tag_start_bytes[6])
                robot_number = get_usint(tag_data,self.tag_start_bytes[7])
                self.tags[robot_number][tag_name]=dict(type=tag_type,use=use,read_only=read_only,db_number=db_number,offsetbyte=offsetbyte,offsetbit=offsetbit,robot_number=robot_number,tag_value='',new_value=None)
                #self.logger.debug(f"Получен тег {tag_name} {self.tags[robot_number][tag_name]}")
            #self.plc_tags.update_plc_tags()
            for robot_number in range(1,self.robots_count+1):
                for tag_name in self.tags[robot_number].keys():
                    assert self.read_tag(robot_number,tag_name), f"Не считано значение тега {tag_name} {self.tags[robot_number][tag_name]}"
                    #self.logger.debug(f"Считано значение тега {tag_name} = {getattr(self, tag_name)}")
            self.logger.info(f"{self.plc_name}: Загружено {tags_count} тега(-ов)")
            self.loading_done = True
            # self.tags_count_ = tags_count
            return True
        except Exception as error:
            self.logger.error(f"Ошибка получения тегов {self.plc_name}. "
                  f"Ошибка {str(error)} {traceback.format_exc()}")
            return False

    def read_tag(self,robot_number,tag_name):
        try:
            if robot_number>self.robots_count:
                self.logger.info(f"Неверный номер робота {robot_number} - robots_count={self.robots_count}")
                return False
            if not tag_name in self.tags[robot_number]:
                self.logger.info(f"Не найден тег {tag_name}")
                return False
            tag_type=self.tags[robot_number][tag_name]['type']
            db_number=self.tags[robot_number][tag_name]['db_number']
            offsetbyte=self.tags[robot_number][tag_name]['offsetbyte']
            offsetbit=self.tags[robot_number][tag_name]['offsetbit']
            if tag_type=='Bool':
                tag_value = get_bool(self.plc.db_read(db_number, offsetbyte, 1),0,offsetbit)
            elif tag_type=='Real':
                tag_value = get_real(self.plc.db_read(db_number, offsetbyte, 4),0)
            elif "Int" in tag_type and tag_type in self.massa:
                tag_value = self.get_int_function[tag_type](self.plc.db_read(db_number, offsetbyte, self.massa[tag_type]), 0)
            elif tag_type=='Char':
                tag_value = self.plc.db_read(db_number, offsetbyte, 1).decode()
            elif 'String' in tag_type:
                len_arr = int(tag_type[7:-1])
                byte_array_read = self.plc.db_read(db_number, offsetbyte, len_arr)
                tag_value = get_string(byte_array_read, 0, len_arr)
                if byte_array_read[0 + 1]>len_arr:
                    self.logger.info(f"Считывание тега '{tag_name}'={tag_value} type {tag_type} из {self.plc_name} size = {byte_array_read[0 + 1]} max_size {len_arr}")
            elif 'Array[0..512] of Byte' in tag_type:
                tag_data = self.plc.db_read(db_number, offsetbyte, 512)
                tag_value = tag_data.rstrip(b'\x00').decode()
            else:
                self.logger.info(f"Неизвестный тип тега для чтения {tag_name} - '{tag_type}'")
                return False
            #self.logger.info(f"Считано значение тега {tag_name} = {tag_value}")
            #setattr(self, tag_name, tag_value)
            self.tags[robot_number][tag_name]['tag_value']=tag_value
            return True
        except Exception as error:
            self.logger.error(f"Ошибка считывания тега '{tag_name}' из {self.plc_name} "
                  f"Ошибка {str(error)} {traceback.format_exc()}")
            return False

    def write_tag(self,robot_number,tag_name,tag_value):
        try:
            if robot_number>self.robots_count:
                self.logger.info(f"Неверный номер робота {robot_number} - robots_count={self.robots_count}")
                return False
            if not tag_name in self.tags[robot_number]:
                self.logger.info(f"{self.plc_name}: Не найден тег {tag_name}")
                return False
            if tag_value is None:
                self.logger.info(f"None значение тега {tag_name} для записи")
                return False            
            tag_type=self.tags[robot_number][tag_name]['type']
            db_number=self.tags[robot_number][tag_name]['db_number']
            offsetbyte=self.tags[robot_number][tag_name]['offsetbyte']
            offsetbit=self.tags[robot_number][tag_name]['offsetbit']
            if tag_type == 'Bool':
                tag_data = self.plc.db_read(db_number, offsetbyte, 1)
                set_bool(tag_data, 0, offsetbit, bool(tag_value))
            elif tag_type == 'Real':
                tag_data = bytearray(4)
                set_real(tag_data, 0, tag_value)
            elif "Int" in tag_type and tag_type in self.massa:
                tag_data = bytearray(self.massa[tag_type])
                assert tag_type[1]!='U' or tag_value>=0, f"Запись отрицательного значения в тип {tag_type}"
                self.set_int_function[tag_type](tag_data, 0, tag_value)
            elif tag_type == 'Char':
                tag_data = bytearray(1)
                tag_data = bytes(tag_value[0],"ascii")
            elif 'String' in tag_type:
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
            self.tags[robot_number][tag_name]['tag_value']=tag_value
            return True
        except Exception as error:
            self.logger.error(f"Ошибка записи тега  {tag_name} - '{tag_type}'=<{tag_value}> DB {db_number} {offsetbyte}.{offsetbit} "
                              f"Ошибка {str(error)} {traceback.format_exc()}")
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
            
    def update_plc_tags(self):
        try:
            for robot_number in range(1,self.robots_count+1):
                for tag_name in self.tags[robot_number].keys():
                    if not self.tags[robot_number][tag_name]['new_value'] is None:
                        assert self.write_tag(robot_number,tag_name,self.tags[robot_number][tag_name]['new_value']), f"Не записано значение {self.tags[robot_number][tag_name]['new_value']} тега {tag_name} {self.tags[robot_number][tag_name]}"
                        #self.tags[robot_number][tag_name]['tag_value']=self.tags[robot_number][tag_name]['new_value']
                        #self.logger.debug(f"{self.plc_name} Robot{robot_number}: Тег {tag_name}={self.tags[robot_number][tag_name]['new_value']} записан. ")
                        self.tags[robot_number][tag_name]['new_value']=None
                    assert self.read_tag(robot_number,tag_name), f"Не считано значение тега {tag_name} {self.tags[robot_number][tag_name]}"
            return True
        except Exception as error:
            self.logger.error(f"{self.plc_name} Robot{robot_number}: Не удалось обновить данные PLC: "
                  f"{str(error)} {traceback.format_exc()}")
        return False
 