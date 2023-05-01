import os
import re
import subprocess
from pip._vendor import msgpack, requests
from pip._vendor.requests.auth import HTTPDigestAuth
import datetime
import logging
from telegramSQL import Telegram_SQL_Error

# Перед первым запуском необходимо установить ffmpeg: sudo apt install ffmpeg

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)

from logger import logger
        
login = 'kolesnik'                                  # логин для системы видеонаблюдения
password = 'DCNwg42#rwn'                            # пароль для системы видеонаблюдения
header = {'Content-type': 'application/x-msgpack'}  # не трогать - способ кодирования апроса/ответа сервера видеонаблюдения
maxDuration=120                                     # максимальная длина видео

cameras = {}    #словарь для сопоставления номера каждой камеры с URL для доступа к ней и её уникальным id на сервере видеонаблюдения. Чтобы добавить новую камеру в этот список, узнайте ее id, отправив GET запрос на адрес "URL камеры/cameras"
#cameraTimeShift - поправка в секундах для конкретной камеры. Если на скачанном видео время на 30 секунд больше, чем нужно, вносим поправку: cameraTimeShift = -30 
cameras[0] = {"cameraId":6,"cameraURL" : "http://192.168.201.17:9786/rpc","cameraTimeShift":0}      #камера 0 = "96 (Спарка Котельники 1-2)"
cameras[1] = {"cameraId":12,"cameraURL" : "http://192.168.201.17:9786/rpc","cameraTimeShift":0}      #камера 1 = "87 (Спарка Котельники 3-4)"
cameras[2] = {"cameraId":29, "cameraURL" : "http://192.168.30.10:9786/rpc","cameraTimeShift":-20}      #-211 камера 2 = "79_А2-15 (Спарка Мотяково 1-2)"
cameras[3] = {"cameraId":0,  "cameraURL" : "http://192.168.30.11:9786/rpc","cameraTimeShift":-8}      #-316 камера 3 = "82_А1-13 (Спарка Мотяково 3-4)"
cameras[4] = {"cameraId":37, "cameraURL" : "http://192.168.20.10:9786/rpc","cameraTimeShift":-21}      #-21 камера 4 = "188 (Спарка Краснодар 1-2)"
cameras[5] = {"cameraId":38,  "cameraURL" : "http://192.168.20.10:9786/rpc","cameraTimeShift":-19}     #-19 камера 5 = "187 (Спарка Краснодар 3-4)"
cameras[6] = {"cameraId":7,  "cameraURL" : "http://192.168.80.50:9786/rpc","cameraTimeShift":0}      #камера 6 = "Спарка Пятигорск"
cameras[7] = {"cameraId":6, "cameraURL" : "http://192.168.10.10:9786/rpc","cameraTimeShift":-18}       #-18 камера 7 = "Спарка Ростов"

def getDateWithCorrection(date, timeShift):        #возвращает дату и время с поправкой timeShift в секундах
    oldDate = datetime.datetime(date[0], date[1], date[2], date[3], date[4], date[5])
    newDate=oldDate+datetime.timedelta(seconds=timeShift)
    newDateArray = []
    newDateArray.append(newDate.year)
    newDateArray.append(newDate.month)
    newDateArray.append(newDate.day)
    newDateArray.append(newDate.hour)
    newDateArray.append(newDate.minute)
    newDateArray.append(newDate.second)
    return newDateArray

def getVideFromServer(channel, startTime, endTime,resultFileName):
    logger.debug(f" getVideFromServer ({channel},{startTime},{endTime},{resultFileName})")
    # собираем запрос для получения списка id всех кадров с нужной камеры за указанный период времени:
    requestData = msgpack.packb(
    {
    "method" : "archive.get_frames_list",
     "params" :
        {
        "channel" : cameras[channel].get("cameraId"),   #id этой камеры на сервере видеонаблюдения
        "stream" : "video",                             #   у  всех камер из списка есть толлько один поток с видео - "video"
        "start_time" : startTime,                       #дата и время начала записи в формате
        "end_time" : endTime                            #дата и время начала записи
        }
    }
    )

    # получем список id всех кадров с нужной камеры за указанный период времени:
    getFramesListRequest = ""
    try:
        getFramesListRequest = requests.post(cameras[channel].get("cameraURL"), auth=HTTPDigestAuth(login, password), data=requestData, headers ={'Content-Type': 'application/x-msgpack'})
        frames = msgpack.unpackb(getFramesListRequest.content)  # распаковываем ответ сервера с помощью msgpack
        logger.debug(f"getFramesListRequest.content:")
        # logger.info(getFramesListRequest.content)
        # logger.info()
    except Exception as e:
        logger.error(f"Ошибка запроса {getFramesListRequest.status_code}: ", e)
        raise


    if frames.__contains__("result"):
        # создаем массив gop(групп кадров) для скачивания, отбрасываем в начале списка все кадры до первого опорного - они не нужны:
        frames_id = []  # массив gop(групп кадров)
        key_frame = 0
        # logger.info(len(frames['result']['frames_list']))
        logger.debug(f"len(frames['result']) = {len(frames['result'])}")
        # переменная, чтобы выбрасить
        for frame in frames['result']['frames_list']:
            # logger.info(frame)
            if (key_frame == 0) and (frame['gop_index'] != 0):  # не ключевой кадр в начале видео, отбрасываем
                pass
            else:
                if (frame['gop_index'] == 0):
                    key_frame += 1
                    # logger.info(frame)
                    frames_id.append([])
                frames_id[key_frame - 1].append(frame['id'])

        logger.debug(f"Скачаем групп изображений: {len(frames_id)}", )
        logger.debug(resultFileName)
        if len(frames_id)>0:
            resultFile = open(resultFileName, 'wb')  # создаем файл, который станет видео

        for frame_keys in frames_id:
            #logger.info(f"Скачиваем кейфреймы... {frame_keys}")
            # logger.info("#", end='')
            for frame in frame_keys:
                # logger.info(frame)
                data = msgpack.packb({"method": "archive.get_frame", "params": {"channel": cameras[channel].get("cameraId"), "stream": "video", "id": frame}})  # собираем запрос для получения кадра из архива по id
                getFrameRequest = requests.post(cameras[channel].get("cameraURL"), auth=HTTPDigestAuth(login, password), data=data, headers={'Content-Type': 'application/x-msgpack'})
                payload = msgpack.unpackb(getFrameRequest.content, raw=True)
                resultFile.write(
                    payload[b'result'][b'frame'][b'raw_bytes'])  # пишем в файл данные из поля raw_bytes ответа сервера
        if len(frames_id)>0:
            resultFile.close()

        logger.debug("Если кейфреймы найдены, то они были скачаны")
        return len(frames_id)
    else:
        return 0


def convertH264ToMP4 (H264File, MP4File):   #конвертирует H264File в файл MP4File, затем удаляет H264File файл
    try:
        #stringForExecute = "ffmpeg -i "+"\""+H264File +"\""+" "+"\""+ MP4File+"\""
        stringForExecute = f'ffmpeg -i "{H264File}" "{MP4File}"'
        logger.info(stringForExecute)
        try:
            os.remove(MP4File)              #удаляем видеофайл, если он уже существует
        except OSError:
            pass
        subprocess.call([stringForExecute], shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, timeout=300)
        logger.info('Конвертация прошла успешно')
        os.remove(H264File)
        logger.info('Удалили h264')
    except subprocess.CalledProcessError as e:
        logger.error('Ошибка:\ncmd:{}\noutput:{}'.format(e.cmd, e.output))
        raise


def getVideo(request, pathToresultFile, fileName):
    # Если видео удалось скачать и конвертировать воврщает 1. Вовращает 0, если апрос выполнен с ошибками или в архиве нет видео с такими параметрами.
    # request - (тип строка) вида№ ДД.ММ.ГГГГ ЧЧ:ММ:СС t
    # № - номер камеры
    # t - длительность видеозаписи в секундах.
    # 10-ти секундный ролик с 7й камеры за 27 янв 2021 с 13ч 30м 0сек: "7 27.01.2021 13:30:00 10"
    # pathToresultFile - (тип строка) путь, по которому будет создан файл с видео. Например, "C:/video/"
    # fileName - (тип строка) имя файла с видео c расширением mp4/avi(mp4 лучше качество, большой размер/avi хуже качество, небольшой размер). Например, "video.mp4" или "robot_video.avi"
    try:
        logger.debug(f"getVideo {request} {pathToresultFile} {fileName} ")
        server_delay=120
        time.sleep(server_delay)
        logger.debug(f"Vidio server delay {server_delay}sec has passed")
        d = datetime.datetime.strptime(request.split(" ")[1] + " " + request.split(" ")[2],'%d.%m.%Y %H:%M:%S')     # Конвертируем строку вида '7.01.2021 13:30:00 5' в datetime
        duration = request.split(" ")[3]
        if int(duration)>maxDuration:
            logger.info(f"Длина видео болше {str(maxDuration)} секунд")
            print(f"Длина видео болше {str(maxDuration)} секунд")
            return 0
        fileNameWExt = re.match(r"(.*)\..*$", fileName).group(1)                                                    # имя файла видео без расширения. Например video1.mp4 -> video1

        if not os.path.exists(pathToresultFile):                                                                    # если не существует папка для будущего видео, создаем
            os.makedirs(pathToresultFile)

        startDateAndTime = []                                                                                       # Массив с датой и временем начала видеозаписи
        startDateAndTime.append(d.year)
        startDateAndTime.append(d.month)
        startDateAndTime.append(d.day)
        startDateAndTime.append(d.hour)
        startDateAndTime.append(d.minute)
        startDateAndTime.append(d.second)

        endDateAndTime = []                                                                                         # Массив с датой и временем конца видеозаписи
        endDateAndTime.append(d.year)
        endDateAndTime.append(d.month)
        endDateAndTime.append(d.day)
        endDateAndTime.append(d.hour)
        endDateAndTime.append(d.minute)
        endDateAndTime.append(d.second)
        duration = request.split(" ")[3]                                                                            # Длительность видео, введенная пользователем

        cameraTimeShift=cameras[int(request.split(" ")[0])].get("cameraTimeShift")  #Сдвиг времени нужной камеры
        startDateAndTime = getDateWithCorrection(startDateAndTime, 0+cameraTimeShift)  # Массив с датой и временем начала видеозаписи с поправкой на неверное время сервера видеонаблюдения
        endDateAndTime = getDateWithCorrection(endDateAndTime, int(duration)+cameraTimeShift)  # Массив с датой и временем конца видеозаписи с поправкой на неверное время сервера видеонаблюдения
        logger.info("Время начала записи: " + str(startDateAndTime)+", время конца записи: "+ str(endDateAndTime) + ". Камера "+str(int(request.split(" ")[0])))
        logger.info("Качаем видео с сервера...")

        resultOfGetVideoFromServer = getVideFromServer(int(request.split(" ")[0]), startDateAndTime, endDateAndTime, pathToresultFile + fileNameWExt+'.h264')
        if resultOfGetVideoFromServer > 0:
            logger.info("Конвертируем видео...")
            convertH264ToMP4(pathToresultFile + fileNameWExt+'.h264', pathToresultFile + fileName)
            logger.info("Видео готово...")
            logger.info("Видеофайл отправляется")
            telegram = Telegram_SQL_Error()
            telegram.send_video(f"{pathToresultFile}{fileName}.avi")
            return 1
        else:
            logger.info("В архиве нет видео с такими параметрами...")
            return 0
    except Exception as et:
        logger.error("Ошибка "+str(et))
        return 0




#result = getVideo("0 04.02.2021 12:00:00 10", "/home/void/Robot Videos/", "robot0.avi")
#result = getVideo("1 04.02.2021 12:00:00 10", "/home/void/Robot Videos/", "robot1.avi")
#result = getVideo("2 04.02.2021 00:00:00 10", "/home/void/Robot Videos/", "robot2.avi")
#result = getVideo("3 04.02.2021 16:00:00 10", "/home/void/Robot Videos/", "robot3.avi")
#result = getVideo("4 04.02.2021 00:00:00 10", "/home/void/Robot Videos/", "robot4.avi")
#result = getVideo("5 04.02.2021 00:00:00 10", "/home/void/Robot Videos/", "robot5.avi")
#result = getVideo("6 04.02.2021 12:00:00 10", "/home/void/Robot Videos/", "robot6.avi")
#result = getVideo("7 04.02.2021 12:00:00 10", "/home/void/Robot Videos/", "robot7.avi")
#logger.info(result)

#camera_number=2     # номер камеры
#movie_duration=10    # длительность видео в секундах
#print (datetime.datetime.now().strftime(f"{camera_number} %d.%m.%Y %H:%M:%S {movie_duration}"))
#print (f"robot{camera_number}.avi")
#result = getVideo(datetime.datetime.now().strftime(f"{camera_number} %d.%m.%Y %H:%M:%S {movie_duration}"), "/home/void/Robot Videos/", f"robot{camera_number}.avi")