import psycopg2
import json
import logging
import traceback

class SQL:
    def __init__(self, database, user, password, host):
        self.logger = logging.getLogger("opc_py")
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.connect2db()

    def connect2db(self):
        self.connect = psycopg2.connect(user=self.user,
                                        database=self.database,
                                        password=self.password,
                                        host=self.host,
                                        port="5432")
        #self.connect.set_session(readonly=True, autocommit=True)
        self.cursor = self.connect.cursor()
        self.exec_listener()

    # PLC. Получаем все контроллеры с текущими заданиями из БД
    def get_plcs_status(self):
        """
        Загрузка PLC и роботов
        """
        try:
            with self.connect:
                self.cursor.execute('SELECT get_plcs_status()')
                plcs_status = self.cursor.fetchall()
                assert plcs_status, "PLC не найдены"
                return plcs_status
        except Exception as e:
            self.logger.info(f'Error get_plcs_status {str(e)}.  {traceback.format_exc()}')
            return None

    # STATUS. Изменение Status
    def set_status(self, status_feedback):
        """
        Выполнение запроса к БД
        """
        try:
            if status_feedback['query']=='gen_robot_task':
                query=f"SELECT gen_robot_task({status_feedback['part_id']},{status_feedback['robot_id']});"
            elif status_feedback['query']=='reset_robot_task':
                query=f"SELECT reset_robot_task({status_feedback['robot_id']},{status_feedback['robot_task_id']},{status_feedback['part_id']},\'{status_feedback['part_status']}\');"
            elif status_feedback['query']=='process_xyacodes':
                query=f"SELECT process_xyacodes('{json.dumps(status_feedback)}');"
            elif status_feedback['query']=='process_label':
                query=f"SELECT process_label('{json.dumps(status_feedback)}');"
            elif status_feedback['query']=='pack_parts':
                query=f"SELECT pack_parts('{json.dumps(status_feedback)}');"
            elif status_feedback['query']=='set_robot_task_status':
                query=f"SELECT set_robot_task_status('{json.dumps(status_feedback)}');"
            elif status_feedback['query']=='process_plc_status':
                query=f"SELECT process_plc_status('{json.dumps(status_feedback)}');"
            elif status_feedback['query']=='reset_robot_pallet':
                query=f"SELECT reset_robot_pallet('{json.dumps(status_feedback)}');"
            elif status_feedback['query']=='expected_robot_trajectory':
                query=f"SELECT expected_robot_trajectory('{json.dumps(status_feedback)}');"
            elif status_feedback['query']=='write_error_log':
                query=f"SELECT write_error_log('{status_feedback['error']}','OPC');"
            elif status_feedback['query']=='order_remove':
                query=f"SELECT order_remove({status_feedback['order_id']});"
            elif status_feedback['query']=='part_add':
                query=f"SELECT part_add('{status_feedback['json_data']}');"
            #    query=f"SELECT part_add('{json.dumps(status_feedback)}');"
            else:
                return dict(error=f"Не корректный status_feedback в sql.set_status: {status_feedback}")
            return self.run_query(query)
        except Exception as e:
            self.logger.info(f'Ошибка обработки status_feedback {str(e)} {traceback.format_exc()}')

    # QUERY. Выполнить запрос
    def run_query(self, query):
        """
        Возвращение результата запроса
        """
        try:
            with self.connect:
                self.logger.debug(f"run query {query}") 
                self.cursor.execute(query)
                request_status = self.cursor.fetchall()
                return request_status[0][0]
        except Exception as e:
            self.logger.info(f'Ошибка выполнения запроса query {str(e)} {traceback.format_exc()}')
            if str(e).startswith('query message contents do not agree with length in message type "N"'):
                try:
                    with self.connect:
                        self.logger.debug(f"run query {query}") 
                        self.cursor.execute(query)
                        request_status = self.cursor.fetchall()
                        return request_status[0][0]
                except Exception as e:
                    self.logger.info(f'Ошибка 2 выполнения запроса query {str(e)} {traceback.format_exc()}')
    # LISTEN. Получаем сообщения notify
    def exec_listener(self):
        """
        Подписка на события в БД
        """
        try:
            with self.connect:
                self.cursor.execute("LISTEN robot_task;")
                self.cursor.execute("LISTEN part_layers;")
        except Exception as e:
            self.logger.info(f'Error exec_listener {str(e)} {traceback.format_exc()}')
