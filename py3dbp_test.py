import traceback
import inspect
import json
import simplejson

import importlib.util
import sys
import random

from py3dbp import Bin, Item, Packer, Painter
import numpy as np

from collections import defaultdict
#from pprint import pprint
from decimal import Decimal

def level_generator(start: int, max_: int) -> int:
		"""
		Генератор уровней
		:param start: начало отсчета
		:param max_: максимальный уровень
		:return: уровень
		"""
		while True:
				if start >= max_:
						start = 0
				yield start
				start += 1

def sort_thickness_parts(parts_l: dict) -> dict:
		"""
		Сортировка по толщине
		:param parts_l: список деталей
		:return: детали сгруппированные по толщине
		"""
		parts_ = defaultdict(list)
		for part_ in parts_l:
				parts_[part_['part_thickness_z']].append(part_)
		return dict(parts_)


def drawn(obj) -> None:
		"""
		Визуализация заполнения
		:param obj: обьект с деталями
		:return:
		"""
		painter = Painter(obj)
		painter.plotBoxAndItems()

#def get_position(item, previous_lay, indent, offset_z):
#		"""Рассчет позиции деталей предыдущего слоя"""
#		for part in previous_lay:
#				if part['part_id'] == item.partno:
#						return [Decimal(part['part_pos_x']) - (item.width / 2 + indent // 2),
#										Decimal(part['part_pos_y']) - (item.height / 2 + indent // 2),
#										Decimal(part['part_pos_z']) - offset_z]

def put_fit_items(pallet_: Bin):
		for item in pallet_.items:
				dimension = item.getDimension()
				[w, h, d] = dimension
				y = Decimal(item.position[0])
				x = Decimal(item.position[1])
				z = Decimal(item.position[2])
				pallet_.fit_items = np.append(pallet_.fit_items,
																			np.array([[Decimal(x), Decimal(x + w), Decimal(y), Decimal(y + h), Decimal(z), Decimal(z + d)]]),
																			axis=0)
		return pallet_


def create_fit_items(pallet, previous_lay, indent, packer, offset_z,pallet_w,pallet_h):
		"""Заполнение fit_items при ручном формировании слоя"""
		item_list = []
		for num, part in enumerate(previous_lay, start=0):
		#for part in previous_lay:
				item = Item(partno=part['part_id'],
										name='part',
										typeof='cube',
										WHD=(Decimal(part['part_length_x'] + indent),
												 Decimal(part['part_length_y'] + indent),
												 Decimal(part['part_thickness_z'])),
										level=0,
										weight=1,
										loadbear=0,
										updown=False,
										color="#" + ''.join([random.choice('0123456789ABCDEF') for j in range(6)])
										)
				item.rotation_type = 0 if part['part_angle_a'] == 0 else 1
				item.position= [part['part_pos_x']-(part['part_length_x']/2+indent//2)+Decimal(pallet_w/2),
												part['part_pos_y']-(part['part_length_y']/2+indent//2)+Decimal(pallet_h/2),
												part['part_pos_z']-offset_z]
				item_list.append(item)
				previous_lay[num]['part_length_x']=Decimal(part['part_length_x'])
				previous_lay[num]['part_length_y']=Decimal(part['part_length_y'])
				previous_lay[num]['part_thickness_z']=Decimal(part['part_thickness_z'])
		#for item in item_list:
		#		item.position = get_position(item, previous_lay, indent, offset_z)
				pallet.items.append(item)
		pallet = put_fit_items(pallet)
		#packer.pack(
		#		bigger_first=False,
		#		fix_point=True
		#)
		print(f'pallet item_list {" ".join(str(x.partno) + " pos " + str(x.position) for x in item_list)}')
		return pallet
	

def main(parts, previous_lay, pallet_w, pallet_h, indent, next_lay) -> dict:
	"""
	Главная функция
	:return: результат работы функции словарь с информацией о процентном заполнении слоя
	и списком деталей с центрами координат
	"""
	arg=parts
	try:
		# pprint(parts)
		if not parts:
			return {
								"filling": 0,
								"current_lay": []
						 }
		pallet_s = pallet_w * pallet_h	# площадь палеты
		sort_parts = sort_thickness_parts(parts)
		max_percent = 0	# наилучший результат заполнения
		result_parts = None	# список деталей при наилучшем заполнении
		# ищем лучший результат
		arg=sort_parts
		print('sort_parts', sort_parts)
		for thickness in sort_parts:
				offset_z = 0	# сдвиг который вычтем, а потом вернем что бы небыло висяков
				part_thickness = thickness
				if previous_lay:
						if next_lay:
								part_thickness = max([part['part_thickness_z'] for part in revious_lay]) + thickness
								offset_z = min([part['part_pos_z'] for part in previous_lay])
								part_thickness -= offset_z
						else:
								part_thickness = max([part['part_thickness_z'] for part in previous_lay])
								offset_z = min([part['part_pos_z'] for part in previous_lay])
								part_thickness -= offset_z
				pallet = Bin(partno='Bin', WHD=(Decimal(pallet_w), Decimal(pallet_h), Decimal(part_thickness)), max_weight=28080, corner=1, put_type=1)
				# если есть предыдущий слой востановим его
				# формируем паллету по толщине деталей
				# мы ограничили высоту палеты больше одного слоя не может быть
				
				for i in range(len(sort_parts[thickness])):
						print ('sort_parts[thickness]',sort_parts[thickness])
						pallet.clearBin()
						packer = Packer()
						packer.addBin(pallet)
						if previous_lay:
								pallet = create_fit_items(pallet, previous_lay, indent, packer, offset_z,pallet_w,pallet_h)
						#pack_res=packer.pack(
						#							bigger_first=False,
						#							fix_point=True
						#					)
						# меняем приоритет деталей
						level = level_generator(i, len(sort_parts[thickness]))
						item_list = []
						for part in previous_lay:
								item = Item(partno=part['part_id'],
														name='part',
														typeof='cube',
														WHD=(Decimal(part['part_length_x'] + indent),
																 Decimal(part['part_length_y'] + indent),
																 Decimal(part['part_thickness_z'])
																),
														level=next(level),
														weight=1,
														loadbear=0,
														updown=False,
														color="#" + ''.join([random.choice('0123456789ABCDEF') for j in range(6)]))
								item_list.append(item)
						for part in sort_parts[thickness]:
								item = Item(partno=part['part_id'],
														name='part',
														typeof='cube',
														WHD=(Decimal(part['part_length_x'] + indent),
																 Decimal(part['part_length_y'] + indent),
																 Decimal(part['part_thickness_z'])
																 ),
														level=next(level),
														weight=1,
														loadbear=0,
														updown=False,
														color="#" + ''.join([random.choice('0123456789ABCDEF') for j in range(6)]))
								item_list.append(item)
						print(f'unpacked item_list {" ".join(str(x.partno) + " pos " + str(x.position) for x in item_list)}')
						for item in item_list:
								packer.addItem(item)
						# пакуем
						pack_res=packer.pack(
													bigger_first=False,
													fix_point=True
											)
						assert not pack_res is None, 'pack_res is None'
						print(f'pack_res {pack_res}')					
						#print('pack_res',pack_res)
						assert not ('error' in pack_res), f'{pack_res["error"]}'
						b = packer.bins[0]
						# drawn(b)
						S = 0	# общая площадь занимаемая деталями
						layer_parts = []	# детали 0 слоя
						print(f'packed item_list {" ".join(str(x.partno) + " pos " + str(x.position) for x in b.items)}')					
						for item in b.items:
								# нужно собрать последний слой
								if item.name != 'corner':
										S += item.width * item.height
										item.center = (Decimal(item.position[0] + int((item.width+ indent) / 2 )), Decimal(item.position[1] + int((item.height + indent) / 2)),Decimal(item.position[2]))
										layer_parts.append({'part_pos_x': Decimal(item.center[0])-Decimal(pallet_w/2),
																					'part_pos_y': Decimal(item.center[1])-Decimal(pallet_h/2),
																					'part_pos_z': Decimal(item.center[2]) + offset_z,
																					'part_id': item.partno,
																					'lay_number': (Decimal(item.center[2]) + offset_z)/thickness + 1,
																					'part_angle_a': 0 if item.rotation_type == 0 else 90})
						percent = int((S / pallet_s) * 100)	# процент заполнения слоя
						if percent > max_percent:
								max_percent = percent
								result_parts = layer_parts.copy()
		print('result_parts	',result_parts)						
		# нужно вернуть данные только о новых деталях
		result_parts = [part for part in result_parts
										if part['part_id'] not in [part_['part_id'] for part_ in previous_lay]]
		total = {
				"filling": max_percent,
				"current_lay": result_parts
		}
		unfitted_items = [part for part in parts
											if part['part_id'] not in [item['part_id'] for item in total['current_lay']]]
		fitted_items = [part for part in parts
										if part['part_id'] in [item['part_id'] for item in total['current_lay']]]
		return total #, unfitted_items, fitted_items
	except Exception as e:
		return dict(error=str(e),traceback=traceback.format_exc().splitlines()[1].split(', '))


main_res=main([{'part_id': 209758000309001, 'lay_number': 2, 'part_pos_x': -111, 'part_pos_y': -108, 'part_pos_z': 16, 'part_angle_a': 0, 'part_length_x': 1471, 'part_length_y': 548, 'part_thickness_z': 16}], [], 800, 2000, 50, True)

print (main_res)