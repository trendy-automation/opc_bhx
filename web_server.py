from flask import Flask, render_template, make_response, request, jsonify, send_file, Response, url_for, redirect
# from instance.config import DevelopmentConfig
import logging
from queue import Queue
import requests
import socket
import psycopg2
from psycopg2.extras import RealDictCursor
from copy import deepcopy
from collections import defaultdict
from draw import DrawPlot, get_parts as get_parts_from_db


CONNECT_DATA = {"database": "mtk_production_db",
                "user": "db_admin",
                "password": "qazxsw23$",
                "host": "192.168.31.152",
                "port": 5432}

logger = logging.getLogger("opc_py")

web_app = Flask(__name__)
web_app.config['SECRET_KEY'] = 'SECRET_KEY'
web_app.queue_port = Queue()
web_app.queue_hmi = Queue()
web_app.queue_tags = Queue()
web_app.queue_tags.put(defaultdict(dict))
web_app.queue_part_layers = Queue()
web_app.queue_part_layers.put(defaultdict(dict))

log = logging.getLogger('werkzeug')  # отключаем логирование запросов
log.setLevel(logging.ERROR)

cache = defaultdict(list)

def shutdown_server():
    """выключение сервера"""
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()


def web_shutdown():
    """ручка для выключения сервера"""
    # requests.get(f"http://192.168.31.152:6004/shutdown")
    if not web_app.queue_port.empty():
        port = web_app.queue_port.queue[0]
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = str(s.getsockname()[0])
        s.close()
        requests.get(f"http://{ip}:{port}/shutdown")
    else:
        logger.error(f"web_app.queue_port.empty() {web_app.queue_port.empty()}")


web_app.shutdown = web_shutdown


@web_app.route('/shutdown', methods=['GET'])
def shutdown():
    """ручка для выключения сервера"""
    shutdown_server()
    return 'Server shutting down...'


@web_app.route('/details', methods=('GET', 'POST'))
def details():
    """Добавление новой детали интерфейс"""
    return render_template("add_details.html")


@web_app.route('/details_add', methods=('GET', 'POST'))
def details_add():
    """Добавление новой детали в базу"""
    data = {}
    if request.method == 'POST':
        try:
            data = request.get_json(force=True)
        except Exception as err:
            logger.error(f"{type(err)}:\n{err}.")
        else:
            print(data)
    return jsonify(data, 200)


@web_app.route('/', methods=('GET', 'POST'))
def main():
    """Главная страница"""
    plcs = {}
    if not web_app.queue_tags.empty():
        keys = list(web_app.queue_tags.queue[0].keys())
        values = [web_app.queue_tags.queue[0][plc_ip][1].get('robot_pause',dict(tag_value=False))['tag_value'] for plc_ip in keys]
        plcs = dict(zip(keys, values))
        # logger.info(f"plcs = {plcs}")
    return render_template('main.html', plcs=plcs)


@web_app.route('/tags', methods=('GET', 'POST'))
def tags():
    """Теги"""
    tags = {}
    if not web_app.queue_tags.empty():
        tags_ = web_app.queue_tags.queue[0]
        # plc_name = list(tags.keys())[0]
        # tags = dict(tags[plc_name])
    return render_template('tags.html', tags_=tags_)


@web_app.route('/tags_upd', methods=('GET', 'POST'))
def tags_upd():
    """Обновление тегов"""
    tags = {}
    if not web_app.queue_tags.empty():
        tags_ = web_app.queue_tags.queue[0]
        # plc_name = list(tags.keys())[0]
        # tags = dict(tags[plc_name])
        #logger.info(f"tags_upd Len: {len(web_app.queue_tags.queue)}")
    # html = render_template('include/tags_upd.html', tags_=tags_)
    return jsonify({"result": tags_})


@web_app.route('/tags_write/<plc_ip>', methods=('GET', 'POST'))
def tags_write(plc_ip):
    """запись тегов"""
    try:
        data = request.get_json(force=True)
        tag_name = data["name"]
        type_ = data["type"]
        # db = int(data['DB'])
        # bit = int(data['bit'])
        # byte = int(data['byte'])
        tag_value = data["newValue"]
        if "Int" in type_:
            if tag_value:
                tag_value = int(tag_value)
            else:
                tag_value = 0
        logger.debug(f"web_server:{plc_ip} --- {tag_name}={tag_value} <{type_}> DB{data['DB']} {data['bit']}.{data['byte']}")
        #web_app.queue_tags.queue[0][plc_ip][1][tag_name]["newValue"]=tag_value
        web_status = dict([(tag_name, tag_value), ("plc_ip", plc_ip)])
        web_app.queue_hmi.put(web_status)
    except Exception as err:
        logger.error(f"{type(err)}, {err}")
        logger.info(f"Receive tags {data}")
    return make_response("OK", 200)


@web_app.route('/push_start/<ip>', methods=('GET', 'POST'))
def push_start(ip):
    """действие на кнопку старт"""
    try:
        web_status = dict(start_job=True, plc_ip=ip)
        web_app.queue_hmi.put(web_status)
        logger.debug(f"web_app: {web_status}")
    except Exception as err:
        logger.error(f'Ошибка при отправки статуса HMI {web_status}  (push_start) в очередь queue_hmi {str(err)}')
    return make_response("OK", 200)


@web_app.route('/push_reset/<ip>', methods=('GET', 'POST'))
def push_reset(ip):
    """действие на кнопку сброс"""
    try:
        web_status = dict(reset_job=True, plc_ip=ip)
        web_app.queue_hmi.put(web_status)
        logger.debug(f"web_app: {web_status}")
    except Exception as err:
        logger.error(f'Ошибка при отправки статуса HMI {web_status}  (push_reset) в очередь queue_hmi {str(err)}')
    return make_response("OK", 200)


@web_app.route('/robot_alarm_reset/<ip>', methods=('GET', 'POST'))
def robot_alarm_reset(ip):
    """действие на кнопку сброс"""
    try:
        web_status = dict(robot_alarm_reset=True, plc_ip=ip)
        web_app.queue_hmi.put(web_status)
        logger.debug(f"web_app: {web_status}")
    except Exception as err:
        logger.error(
            f'Ошибка при отправки статуса HMI {web_status}  (robot_alarm_reset) в очередь queue_hmi {str(err)}')
    return make_response("OK", 200)


@web_app.route('/robot_pause/<ip>/<checked>', methods=('GET', 'POST'))
def robot_pause(ip, checked):
    """действие на кнопку сброс"""
    try:
        web_status = dict(robot_pause=(checked == 'true'), plc_ip=ip)
        web_app.queue_hmi.put(web_status)
        logger.debug(f"web_app: {web_status}")
    except Exception as err:
        logger.error(f'Ошибка при отправки статуса HMI {web_status}  (robot_pause) в очередь queue_hmi {str(err)}')
    return make_response("OK", 200)


@web_app.after_request
def add_header(r):
    """что бы избавиться от кеширования"""
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    r.headers['Cache-Control'] = 'public, max-age=0'
    return r


@web_app.route('/logs', methods=('GET', 'POST'))
def logs():
    """Логи"""
    logs = []
    with open("/home/kipia/opc_BHX/log_opcBHX.txt", "r") as f:
        logs = f.readlines()[-1::-1]
    return render_template('logs.html', logs=logs)


@web_app.route('/logs_upd', methods=('GET', 'POST'))
def logs_upd():
    """Обновление логов"""
    logs = []
    with open("/home/kipia/opc_BHX/log_opcBHX.txt", "r") as f:
        logs = f.readlines()[-1::-1]
    html = render_template('include/logs_upd.html', logs=logs)
    return jsonify({"result": html})


@web_app.route('/clear_logs', methods=('GET', 'POST'))
def clear_logs():
    """Очистка логов"""
    open("/home/kipia/opc_BHX/log_opcBHX.txt", "w").close()
    return make_response("OK", 200)


@web_app.route('/parts', methods=('GET', 'POST'))
def parts_view():
    """Детали"""
    col_names = []
    number = []
    statuses = []
    try:
        with psycopg2.connect(**CONNECT_DATA, cursor_factory=RealDictCursor) as con:
            with con.cursor() as cur:
                cur.execute("SELECT * FROM view_order_parts LIMIT 0;")
                col_names = [desc[0] for desc in cur.description]
                cur.execute("SELECT * FROM view_orders;")
                number = [row['order_number'] for row in cur.fetchall()]
                cur.execute("SELECT * FROM view_part_statuses")
                statuses = [row['status_alias'] for row in cur.fetchall()]
    except Exception as err:
        print(f"{type(err)}:\n{err}")
    return render_template('part_template.html', col_names=col_names, number=number, statuses=statuses)


@web_app.route('/part_upd', methods=('GET', 'POST'))
def get_parts():
    data = []
    col_names = []
    if request.method == 'POST':
        try:
            json_data = request.get_json(force=True)
            # print(f"От фронтеда приняты данные: {json_data}")
            with psycopg2.connect(**CONNECT_DATA, cursor_factory=RealDictCursor) as con:
                with con.cursor() as cur:
                    cur.execute("SELECT * FROM view_part_statuses")
                    translate = {row['status_alias']: row['part_status'] for row in cur.fetchall()}

                    query = f"SELECT * FROM view_order_parts " \
                            f"WHERE \"Номер заказа\" in ({str(list(map(int, json_data['number'])))[1:-1]})" \
                            f" AND \"Состояние детали\" in ({str(json_data['status'])[1:-1]})" \
                            f"ORDER BY \"Номер заказа\", \"Состояние детали\";"
                    # print(f"QUERY: {query}")
                    cur.execute(query)
                    if cur.rowcount > 0:
                        data = cur.fetchall()
                    col_names = [desc[0] for desc in cur.description]
        except Exception as err:
            print(f"{type(err)}:\n{err}")
    html = render_template('include/parts_upd.html', data=data, col_names=col_names, translate=translate)
    return jsonify({"result": html})


@web_app.route('/parts/<int:part_id>', methods=['PATCH'])
def part_update(part_id):
    data = request.get_json(force=True)
    part_status = data['part_status']
    try:
        with psycopg2.connect(**CONNECT_DATA) as con:
            with con.cursor() as cur:
                cur.execute(f"""
                UPDATE parts
                SET part_status = '{part_status}'
                WHERE id = {part_id}
                """)
        return {'status': 'ok'}
    except Exception as err:
        print(f"{type(err)}:\n{err}")
        return 'Something went wrong', 500


@web_app.route('/last_parts')
def last_parts():
    col_names = []
    data = []
    statuses = []
    try:
        with psycopg2.connect(**CONNECT_DATA, cursor_factory=RealDictCursor) as con:
            with con.cursor() as cur:
                cur.execute("SELECT * FROM view_last_part LIMIT 10;")
                if cur.rowcount > 0:
                    sql_data = cur.fetchall()
                    data = sql_data
                col_names = [desc[0] for desc in cur.description]
                cur.execute("SELECT * FROM view_part_statuses")
                statuses = {row['status_alias']: row['part_status'] for row in cur.fetchall()}

    except Exception as err:
        print(f"{type(err)}:\n{err}")
    return render_template('last_part.html', data=data, col_names=col_names, statuses=statuses)


@web_app.route('/update_last_parts')
def update_last_parts():
    col_names = []
    data = []
    statuses = []
    try:
        with psycopg2.connect(**CONNECT_DATA, cursor_factory=RealDictCursor) as con:
            with con.cursor() as cur:
                cur.execute("SELECT * FROM view_last_part LIMIT 10;")
                if cur.rowcount > 0:
                    sql_data = cur.fetchall()
                    data = sql_data
                col_names = [desc[0] for desc in cur.description]
                cur.execute("SELECT * FROM view_part_statuses")
                statuses = {row['status_alias']: row['part_status'] for row in cur.fetchall()}

    except Exception as err:
        print(f"{type(err)}:\n{err}")
    html = render_template('include/last_parts_upd.html', data=data, col_names=col_names, statuses=statuses)
    return jsonify({"result": html})


@web_app.route('/tasks')
def tasks_view():
    """Задания"""
    col_names = []
    data = []
    options = []
    try:
        with psycopg2.connect(**CONNECT_DATA, cursor_factory=RealDictCursor) as con:
            with con.cursor() as cur:
                cur.execute("SELECT * FROM view_robot_tasks LIMIT 10;")
                if cur.rowcount > 0:
                    sql_data = cur.fetchall()
                    data = sql_data
                col_names = [desc[0] for desc in cur.description]
                cur.execute("""
                    SELECT e.enumlabel
                    FROM pg_type t JOIN
                    pg_enum e ON t.oid = e.enumtypid JOIN
                    pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                    WHERE t.typname = 'type_task_status'
                """)
                options = [el['enumlabel'] for el in cur.fetchall()]

    except Exception as err:
        print(f"{type(err)}:\n{err}")
    return render_template('tasks_template.html', data=data, col_names=col_names, options=options)


@web_app.route('/tasks/<int:task_id>', methods=['PATCH'])
def task_update(task_id):
    data = request.get_json(force=True)
    task_status = data['task_status']
    try:
        with psycopg2.connect(**CONNECT_DATA) as con:
            with con.cursor() as cur:
                cur.execute(f"""
                UPDATE robot_tasks
                SET task_status = '{task_status}'
                WHERE id = {task_id}
                """)
        return {'status': 'ok'}
    except Exception as err:
        print(f"{type(err)}:\n{err}")
        return 'Something went wrong', 500


@web_app.route('/tasks_upd')
def get_tasks():
    data = []
    col_names = []
    options = []
    try:
        with psycopg2.connect(**CONNECT_DATA, cursor_factory=RealDictCursor) as con:
            with con.cursor() as cur:
                cur.execute("SELECT * FROM view_robot_tasks LIMIT 10;")
                if cur.rowcount > 0:
                    sql_data = cur.fetchall()
                    data = sql_data
                col_names = [desc[0] for desc in cur.description]
                cur.execute("""
                                    SELECT e.enumlabel
                                    FROM pg_type t JOIN
                                    pg_enum e ON t.oid = e.enumtypid JOIN
                                    pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                                    WHERE t.typname = 'type_task_status'
                                """)
                options = [el['enumlabel'] for el in cur.fetchall()]
    except Exception as err:
        print(f"{type(err)}:\n{err}")
    html = render_template('include/tasks_upd.html', data=data, col_names=col_names, options=options)
    return jsonify({"result": html})


@web_app.route('/get_lay_image/<robot_id>/<layer>/<status>/<part_slot>')
def get_visual(robot_id=1, layer=0, status='pallet_in', part_slot=0):
    parts = get_parts_from_db(CONNECT_DATA, int(robot_id), int(layer), status, int(part_slot))

    cache['parts'] = parts

    if parts:
        new_parts = deepcopy(parts)
        x = max([part['part_pos_x'] + part['part_length_x'] / 2 for part in new_parts]) - min(
            [part['part_pos_x'] - part['part_length_x'] / 2 for part in new_parts])
        y = max([part['part_pos_y'] + part['part_length_y'] / 2 for part in new_parts]) - min(
            [part['part_pos_y'] - part['part_length_y'] / 2 for part in new_parts])
        for part in new_parts:
            part['part_pos_x'] += x / 2 - part['part_length_x'] / 2
            part['part_pos_y'] += y / 2 - part['part_length_y'] / 2

        draw = DrawPlot(new_parts)
        image = draw.run(x, y)

        return Response(image, mimetype='image/png')

    draw = DrawPlot([])
    image = draw.run(800, 1200)
    return Response(image, mimetype='image/png')


@web_app.route('/update_lay_image/<robot_id>/<layer>/<status>/<part_slot>')
def update_visual(robot_id, layer, status, part_slot):
    parts = get_parts_from_db(CONNECT_DATA, int(robot_id), int(layer), status, int(part_slot))

    if parts == cache['parts']:
        return make_response('')

    if parts:
        new_parts = deepcopy(parts)
        x = max([part['part_pos_x'] + part['part_length_x'] / 2 for part in new_parts]) - min(
            [part['part_pos_x'] - part['part_length_x'] / 2 for part in new_parts])
        y = max([part['part_pos_y'] + part['part_length_y'] / 2 for part in new_parts]) - min(
            [part['part_pos_y'] - part['part_length_y'] / 2 for part in new_parts])
        for part in new_parts:
            part['part_pos_x'] += x / 2 - part['part_length_x'] / 2
            part['part_pos_y'] += y / 2 - part['part_length_y'] / 2

        draw = DrawPlot(new_parts)
        image = draw.run(x, y)

        return Response(image, mimetype='image/png')

    return make_response('')


@web_app.route('/layer')
def status_page():
    try:
        with psycopg2.connect(**CONNECT_DATA) as con:
            with con.cursor() as cur:
                cur.execute("SELECT id, name FROM robots;")
                if cur.rowcount > 0:
                    robots = cur.fetchall()

    except Exception as err:
        print(f"{type(err)}:\n{err}")

    robots = [{'id': robot[0], 'name': robot[1]} for robot in robots]
    statuses = ['pallet_in', 'pallet_out', 'part_slot']

    return render_template('layer.html', robots=robots, statuses=statuses)


@web_app.route('/get_current_lay_image')
def get_current_visual():
    data = web_app.queue_part_layers.queue[0]
    parts = data['part_layers']

    if parts:

        new_parts = deepcopy(parts)
        x = max([part['part_pos_x'] + part['part_length_x'] / 2 for part in new_parts]) - min(
            [part['part_pos_x'] - part['part_length_x'] / 2 for part in new_parts])
        y = max([part['part_pos_y'] + part['part_length_y'] / 2 for part in new_parts]) - min(
            [part['part_pos_y'] - part['part_length_y'] / 2 for part in new_parts])
        for part in new_parts:
            part['part_pos_x'] += x / 2 - part['part_length_x'] / 2
            part['part_pos_y'] += y / 2 - part['part_length_y'] / 2

        draw = DrawPlot(new_parts)
        image = draw.run(x, y)

        cache['current_visual'] = data

        return Response(image, mimetype='image/png')

    draw = DrawPlot([])
    image = draw.run(800, 1200)
    return Response(image, mimetype='image/png')


@web_app.route('/update_current_lay_image')
def update_current_visual():
    data = web_app.queue_part_layers.queue[0]
    if data == cache['current_visual']:
        return make_response('')

    parts = data['part_layers']
    cache['current_visual'] = data

    if parts:
        new_parts = deepcopy(parts)
        x = max([part['part_pos_x'] + part['part_length_x'] / 2 for part in new_parts]) - min(
            [part['part_pos_x'] - part['part_length_x'] / 2 for part in new_parts])
        y = max([part['part_pos_y'] + part['part_length_y'] / 2 for part in new_parts]) - min(
            [part['part_pos_y'] - part['part_length_y'] / 2 for part in new_parts])
        for part in new_parts:
            part['part_pos_x'] += x / 2 - part['part_length_x'] / 2
            part['part_pos_y'] += y / 2 - part['part_length_y'] / 2

        draw = DrawPlot(new_parts)
        image = draw.run(x, y)

        return Response(image, mimetype='image/png')

    return make_response('')


@web_app.route('/get_current_data')
def get_current_data():
    try:
        data = web_app.queue_part_layers.queue[0]
    except IndexError as error:
        return jsonify({'status': 'false'})

    part_location = data['part_location']

    return jsonify(part_location)


@web_app.route('/current_lay')
def current_status_page():
    return render_template('current_lay.html')

