from py3dbp import Bin, Item, Painter
import psycopg2
import numpy as np
import random
import json
import sys
import traceback
from logger import logger


def get_parts(connect_data: dict, robot_id: int, layer: int, status: str, part_slot: int) -> list:
    """
    Получение данных из бд
    :return: [список деталей]
    """
    args = {"part_status": status,
            "robot_id": robot_id,
            "lay_number": layer,
            "part_slot": part_slot,
            "pallet_length_x": 0,
            "pallet_length_y": 0}
    query = f"SELECT get_part_layers('{json.dumps(args)}');"
    with psycopg2.connect(**connect_data) as con:
        with con.cursor() as cur:
            cur.execute(query)
            if cur.rowcount > 0:
                db_data = cur.fetchall()[0][0]

    return db_data['part_layers']


class DrawPlot:
    def __init__(self, parts):
        try:
            self.parts = parts
            #sys.path.append('/py3dbp')
            #from py3dbp import Bin, Item, Packer, Painter
        except Exception as err:
            logger.error(f"{type(err)}:\n{err} {traceback.format_exc()}")

    def drawn(self, obj) -> None:
        """
        Визуализация заполнения
        :param obj: обьект с деталями
        :return:
        """
        painter = Painter(obj)
        painter.plotBoxAndItems()

    def get_drawn(self, obj) -> str:
        painter = Painter(obj)
        return painter.get_plot()

    def run(self, width, height):

        pallet = Bin(partno='Bin', WHD=(width, height, 0), max_weight=28080, corner=1, put_type=1)

        colors = ['black', 'red', 'green', 'blue', 'brown', 'yellow', 'purple', 'orange', 'pink']
        for part in self.parts:
            if len(colors) == 0:
                colors = ['black', 'red', 'green', 'blue', 'brown', 'yellow', 'purple', 'orange', 'pink']
            color = random.choice(colors)
            colors.remove(color)
            item = Item(partno=part['part_id'],
                        name='part',
                        typeof='cube',
                        WHD=(part['part_length_x'],
                             part['part_length_y'],
                             part['part_thickness_z']),
                        level=0,
                        weight=1,
                        loadbear=0,
                        updown=False,
                        color=color,
                        )
            item.position = [
                part['part_pos_x'],
                part['part_pos_y'],
                part['part_pos_z']
            ]
            item.rotation_type = 0 if (part['part_angle_a'] < 45 or part['part_angle_a'] > 315) or (part['part_angle_a'] < 225 and part['part_angle_a'] > 135) else 1
            pallet.items.append(item)

        filename = self.get_drawn(pallet)
        return filename
