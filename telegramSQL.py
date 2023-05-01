import datetime
import requests
import logging
import telebot
from telebot import types
import threading
from queue import Queue
import time

from logger import logger

class Telegram_SQL_Error(threading.Thread):

    def __init__(self):
        try:
            threading.Thread.__init__(self, args=(), name='telebot', kwargs=None)
            self.bot_token = ''
            self.bot_chatID = ''
            # self.logger = logging.getLogger("opc_py")
            self.logger = logger
            # self.logger.setLevel(logging.ERROR)
            self.bot = telebot.TeleBot(self.bot_token)
            self.bot.set_update_listener(self.listener)  # register listener
            #self.bot.polling(none_stop=False, interval=0, timeout=20)
            self.queue_error = Queue()
        except Exception as e:
            self.logger.error('Telegram __init__ error ' + str(e))
    def run(self):
        self.logger.info(f"Telegram started")
        cur_thread = threading.currentThread()
        while getattr(cur_thread, "do_run", True):
            #self.logger.info(f"Telegram run , self.queue_error.empty() = {self.queue_error.empty()}")
            try:
                if not self.queue_error.empty():
                    next_msg=self.queue_error.get()
                    #self.logger.info(f"Telegram message")
                    msg = next_msg.replace("\\","").replace("*", "\\*").replace("~", "\\~").replace("_", "\\_")
                    bot_message = str(datetime.datetime.now())[:-7] + ': "' + msg +'"'
                    send_text = 'https://api.telegram.org/bot' + self.bot_token + '/sendMessage?chat_id=' + self.bot_chatID + '&parse_mode=Markdown&text=' + bot_message
                    response = requests.get(send_text)
                    #return response.json()
            except Exception as e:
                self.logger.error('Telegram error ' + str(e)+'on_message ' + bot_message)
            time.sleep(1)
    
    # only used for console output now
    def listener(messages):
        """
        When new messages arrive TeleBot will call this function.
        """
        for m in messages:
            if m.content_type == 'text':
                # print the sent message to the console
                self.logger.debug(str(m.chat.first_name) + " [" + str(m.chat.id) + "]: " + m.text)


    def send_video(self, file_path):
        try:
            #self.bot.send_message(self.bot_chatID, 'Тест')
            video = open(file_path, 'rb')
            self.bot.send_video(self.bot_chatID, video)
            self.bot.send_video(self.bot_chatID, "FILEID")
            return 1
        except Exception as e:
            self.logger.error('Telegram error ' + str(e)+' on file ' + file_path)
            return 0

