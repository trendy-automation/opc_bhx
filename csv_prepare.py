"""
apt-get install -y cifs-utils
cd ..
cd mnt
sudo mkdir csv
sudo mkdir mpr
sudo mkdir bhx1
sudo mkdir bhx1mpr
#sudo mount -t cifs //192.168.201.130/share2/share2 /mnt/csv -o user=kipia,password=6450300
sudo mount -t cifs //192.168.203.21/share/Share2/share2 /mnt/csv -o user=krd,password=6450300
sudo mount -t cifs //192.168.31.46/online /mnt/mpr -o vers=1.0,user=admin,password=adminpsw
sudo mount -t cifs //192.168.31.31/prg /mnt/bhx1 -o vers=1.0,user=machineAdmin,password=0123456789
sudo mount -t cifs //192.168.31.31/RobotMPR /mnt/bhx1mpr -o vers=1.0,user=machineAdmin,password=0123456789
 
Заказ order_id,
внешний ключ external_id,
поворот этикетки "label_pos_A": 0,
положение этикетки X "label_pos_X": -1,
положение этикетки Y "label_pos_Y
номер детали part_number
длина по X - part_length_X
длина по Y - part_length_Y
толщина part_thickness_Z
задания robot_tasks [{"robot_id": 1, "task_status": "done", "operation_side": "A", "operation_type": "milling",
"operation_number": 1, "operation_content": {"program_A": " KUKA_545_9"}}]}
номер операции operation_number 1
программа-program_A
"""

import csv
import pandas as pd
import string
from decimal import Decimal
import os
import time
from datetime import datetime
import threading
import traceback
from queue import Queue
from shutil import copyfile



from logger import logger


ENCODING = 'cp1251'



select_str_template = "{\"order_number\": $order_number, \"label_angle_a\": $label_pos_A, " \
                      "\"id\": $id, " \
                      "\"part_number\": $part_number, " \
                      "\"part_counter\": $part_counter, " \
                      "\"order_position\": $order_position, " \
                      "\"part_length_x\": $part_length_x, \"part_length_y\": $part_length_y, " \
                      "\"part_thickness_z\": $part_thickness_z, \"robot_tasks\":[{\"robot_id\": 1, " \
                      "\"operation_type\": \"machining\", \"operation_number\": 1, \"operation_content\": " \
                      "{\"program_A\": \"$program_A\", \"program_B\": \"$program_B\"}}]}"
template = string.Template(select_str_template)  # шаблон sql запроса


queue_status = Queue()
error_messages = []
#sleep_sec = 5

def check_file_exist(order_path: str, program_name: str) -> bool:
    try:
        if not program_name.lstrip():
            return False
        #program_fullpath="/mnt/mpr/RobotMPR/"+program_name.lstrip()+".mpr"
        #program_alias="Z:/RobotMPR/"+program_name+".mpr"
        program_fullpath=order_path+"/"+program_name.lstrip()+".mpr"
        program_alias=program_name.lstrip()+".mpr"
        if not os.path.exists(program_fullpath):
            error_text = f"Импорт csv: Файл {program_fullpath} не найден на сетевом диске!"
            #logger.error(f"{error_text}")
            if not error_text in error_messages:
                error_messages.append(error_text)
                queue_status.put(dict(query='write_error_log',error=error_text))
            return False
        else:
            return True
    except Exception as err:
        logger.error(f"{type(err)}:\n{err} {traceback.format_exc()}")
        return False



def prepare_csv(file_name: str, encoding_='utf-8') -> bool:
    """
    Обработка ксв файлов и создание sql запросов
    Создание нового csv файла с полученным part_id
    :param encoding_: кодировка открываемого файла
    :param file_name: имя обрабатываемого файла
    :return: bool
    """
    try:
        with open(f"/mnt/csv/Input/{file_name}", 'r', encoding=encoding_) as f_r:
            with open(f"/mnt/csv/Output/{file_name}", 'w', encoding=encoding_, newline='') as f_w:
                #file_reader = csv.DictReader(f_r, delimiter=';')
                #headers = file_reader.fieldnames
                df = pd.read_csv(f_r, delimiter=';')
                df.fillna('', inplace=True)
                headers = df.columns.tolist()
                mandatory_keys = ['poz', 'order', 'thinkness', 'height', 'width', 'qty', 'part_ID', 'program_lic', 'program_obr', 'label_pos_A']
                missing_keys = list(set(mandatory_keys)-set(headers))
                #logger.info(f"Headers: {headers} missing_keys: {missing_keys}")                
                if len(missing_keys)>0:
                    q = "'"
                    error_text = f"Импорт csv: Не найдены ключи: {str(missing_keys).replace(q,'')} в {file_name}!"
                    #logger.error(f"{error_text}")
                    if not error_text in error_messages:
                        error_messages.append(error_text)
                        queue_status.put(dict(query='write_error_log',error=error_text))
                    return False
                file_reader=df.to_dict(orient='records')
                #data.append(file_reader)
                #data.to_csv(f"/mnt/csv/Output/{file_name}", index = False)
                #file_writer = pd.write_csv(f_w, columns=(headers + ['part_id']), delimiter=';')
                file_writer = csv.DictWriter(f_w, fieldnames=(headers + ['part_id']), delimiter=';')
                file_writer.writeheader()
                #logger.info(f"lines count: {len(list(file_reader))}")
                #Проверка наличия программ на сетевом диске
                programs_ok = True
                poz_ok = True
                for line in file_reader:
                    order_range = f"{int(line['order'])//100}00-{int(line['order'])//100}99"
                    order_path = f"/mnt/mpr/{datetime.now().year}r/{order_range}/{line['order']}"
                    #logger.info(f"line: {line}")
                    if line['program_lic']:
                        programs_ok =  check_file_exist(order_path,line['program_lic'])
                    if line['program_obr']:
                        programs_ok =  check_file_exist(order_path,line['program_obr'])
                    #if not line["poz"].isdigit():
                    #    poz_ok = False
                    if not programs_ok or not poz_ok:
                        return False
                i = 1
                #logger.info(f"programs_ok: {programs_ok}")
                for line in file_reader:
                    order_range = f"{int(line['order'])//100}00-{int(line['order'])//100}99"
                    order_path = f"/mnt/mpr/{datetime.now().year}r/{order_range}/{line['order']}"
                    #logger.info(f"line: {line}")
                    if line["label_pos_A"]==90 or line["label_pos_A"]==270:
                        line["height"],line["width"]=line["width"],line["height"]
                    if i == 1:
                        #remove_res = query_execute(f'SELECT order_remove({line["order"]})','order_id')
                        queue_status.put(dict(query='order_remove',order_id=line["order"]))
                    if line["label_pos_A"] == '':
                        line["label_pos_A"] = 0
                    data = {"order_number": int(line["order"]),
                            "order_position": int(line["poz"]),
                            "label_pos_A": int(line["label_pos_A"]),
                            "part_length_x": str(line["height"]).replace(',','.'),
                            "part_length_y": str(line["width"]).replace(',','.'),
                            "part_thickness_z": str(line["thinkness"]).replace(',','.'),
                            "program_A": line["program_obr"],
                            "program_B": line["program_lic"]}
                    count = int(line["qty"])
                    first_id = False
                    for _ in range(count):
                        data["part_number"] = (int(line["order"]) * 10000) + i
                        if count==1:
                            field_6 = 9
                        else:
                            field_6 = 0
                        data["part_counter"] = _+1
                        id_fields = [str(datetime.now().year)[3],str(line["order"]).zfill(5),'0',str(line["poz"]).zfill(3),'0',str(field_6),str(_+1).zfill(3)]
                        data["id"] =  int(''.join(id_fields))
                        line['part_id'] = data["id"]
                        file_writer.writerow(line)
                        string_ = template.substitute(**data)
                        #logger.info(string_)
                        queue_status.put(dict(query='part_add',json_data=string_))
                        i += 1
                    program_name=line['program_lic']
                    if program_name:
                        #program_fullpath = f"/mnt/mpr/RobotMPR/{program_name.lstrip()}.mpr"
                        #if os.path.exists(program_fullpath):
                        #    os.copy(program_fullpath, f"/mnt/bhx1mpr/{program_name.lstrip()}.mpr")
                        program_fullpath = f"{order_path}/{program_name.lstrip()}.mpr"
                        #if os.path.exists(program_fullpath):
                        copyfile(program_fullpath, f"/mnt/bhx1mpr/{program_name.lstrip()}.mpr")
                    program_name=line['program_obr']
                    if program_name:
                        #program_fullpath = f"/mnt/mpr/RobotMPR/{program_name.lstrip()}.mpr"
                        #if os.path.exists(program_fullpath):
                        #    os.copy(program_fullpath, f"/mnt/bhx1mpr/{program_name.lstrip()}.mpr")
                        program_fullpath = f"{order_path}/{program_name.lstrip()}.mpr"
                        #if os.path.exists(program_fullpath):
                        copyfile(program_fullpath, f"/mnt/bhx1mpr/{program_name.lstrip()}.mpr")
        return True
    except Exception as err:
        logger.error(f"{type(err)}:\n{err} {traceback.format_exc()}")
        return False
     
def start():
    while getattr(threading.currentThread(), "do_run", True):
        check_csv()
        time.sleep(5)

def check_csv():
    try:
        # logger.info(f"{datetime.now()}: Проверяю наличие новых csv файлов.")
        #if len(error_messages)>0:
        #    time.sleep(60)
        files = os.listdir("/mnt/csv/Input")
        for file in files:
            if file.endswith(".csv"):
                logger.info(f"Найден     {file}. Начинаю обработку.")
                impotr_done = prepare_csv(file, ENCODING)
                if impotr_done:
                    error_messages = []
                    #sleep_sec = 5
                    #file_fullpath=f"/mnt/csv/Input/{file}"
                    #logger.info(f"Файл {file} os.path.exists {os.path.exists(file_fullpath)}.") 
                    os.rename(f"/mnt/csv/Input/{file}", f"/mnt/csv/Old/{file}")
                    logger.info(f"Файл {file} обработан и перемещен.")
                else:
                    time.sleep(60)
                    #sleep_sec = 60
    except Exception as err:
        logger.error(f"{type(err)}:\n{err} {traceback.format_exc()}")
