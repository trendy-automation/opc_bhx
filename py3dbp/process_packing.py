import traceback
import inspect
import json
import simplejson

import importlib.util
import sys
import random


try:
	arg=json.loads(simplejson.dumps(args[0], use_decimal=True))
	DEBUG = plpy.execute('SELECT is_debug()')[0]['is_debug']
	assert isinstance(arg, dict),'Проверь формат JSON'
	assert 'parts' in arg,'Нужен параметр parts'
	assert 'previous_lay' in arg,'Нужен параметр previous_lay'
	assert 'pallet_length_x' in arg,'Нужен параметр pallet_length_x'
	assert 'pallet_length_y' in arg,'Нужен параметр pallet_length_y'
	assert 'part_indent' in arg,'Нужен параметр part_indent'

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
		parts_ = dict()
		for part_ in parts_l:
				parts_[part_['part_thickness_z']]=list()
				parts_[part_['part_thickness_z']].append(part_)
		return dict(parts_)
		
	sys.path.append('/home/kipia/opc_BHX')
	from py3dbp import Bin, Item, Packer, Painter
	
	parts, previous_lay, pallet_w, pallet_h, indent = (arg['parts'], arg['previous_lay'], arg['pallet_length_x'], arg['pallet_length_y'], arg['part_indent'])
	pallet_s = pallet_w * pallet_h  # площадь палеты
	sort_parts = sort_thickness_parts(parts)
	max_percent = 0  # наилучший результат заполнения
	result_parts = None  # список деталей при наилучшем заполнении
	# ищем лучший результат
	for thickness in sort_parts:
		# формируем паллету по толщине деталей
		pallet = Bin(partno='Bin', WHD=(pallet_w, pallet_h, thickness), max_weight=28080, corner=1, put_type=1)
		for i in range(len(parts)):
			# меняем приоритет деталей
			level = level_generator(i, len(sort_parts[thickness]))
			item_list = []
			for part in parts:
				item_list.append(Item(partno=part['part_id'],
									  name='part',
									  typeof='cube',
									  WHD=(part['part_length_x'] + indent,
										   part['part_length_y'] + indent,
										   part['part_thickness_z']),
									  level=next(level),
									  weight=1,
									  loadbear=0,
									  updown=False,
									  color="#" + ''.join([random.choice('0123456789ABCDEF') for j in range(6)])))
			packer = Packer()
			packer.addBin(pallet)
			for item in item_list:
				packer.addItem(item)
			# пакуем
			packer.pack(
				bigger_first=False,
				fix_point=True,
				binding=[('server', 'cabint')]
			)
			b = packer.bins[0]
			S = 0   # общая площадь занимаемая деталями
			layer_parts = []  # детали 0 слоя
			for item in b.items:
				if item.name != 'corner' and item.position[2] == 0:
					S += (item.width * item.height)
					item.center = (item.position[0] + int(item.width / 2), item.position[1] + int(item.height / 2))
					layer_parts.append({'part_pos_x': int(item.center[0]),
                                        'part_pos_y': int(item.center[1]),
                                        'part_id': item.partno,
                                        'part_angle_a': 0 if item.rotation_type == 0 else 90})
			percent = int((S / pallet_s) * 100)  # процент заполнения слоя
			if percent > max_percent:
				max_percent = percent
				result_parts = layer_parts.copy()
	res = {
		"filling": max_percent,
		"parts": result_parts
	}
	return res
except Exception as e:
	traceback_info=traceback.format_exc().splitlines()[1].split(', ')
	source=traceback_info[1]+' '+'_'.join(traceback_info[2].split('_')[slice(4,-1)])
	err_arg = [traceback_info[1]+': '+str(e),simplejson.dumps(args,ensure_ascii=False,use_decimal=True)]
	res = plpy.execute(plpy.prepare(f'SELECT write_error_log($1,$2)', ['text','text']), err_arg)
	return dict(source=source,error=str(e),args=arg)