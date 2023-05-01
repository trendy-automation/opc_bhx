--непосредственная упаковка слоя с помощью внешней библиотеки
DO $$ --process_packing
BEGIN
DROP FUNCTION IF EXISTS process_packing;
CREATE OR REPLACE FUNCTION process_packing(arg jsonb)
	RETURNS jsonb 
	TRANSFORM FOR TYPE jsonb
AS $BODY$
# TRANSFORM FOR TYPE jsonb
import traceback
import inspect
import json
import simplejson


import importlib.util
import sys
import random

#from py3dbp import Bin, Item, Packer, Painter
import numpy as np

from collections import defaultdict
#from pprint import pprint
from decimal import Decimal

try:
	arg=json.loads(simplejson.dumps(args[0], use_decimal=True))
	DEBUG = plpy.execute('SELECT is_debug()')[0]['is_debug']
	assert isinstance(arg, dict),'Проверь формат JSON'
	assert 'parts' in arg,'Нужен параметр parts'
	for part in arg['parts']:
			assert 'part_id' in part,'Нужен параметр part_id в part'
			assert 'part_length_x' in part,'Нужен параметр part_length_x в part'
			assert 'part_length_y' in part,'Нужен параметр part_length_y в part'
			assert 'part_thickness_z' in part,'Нужен параметр part_thickness_z в part'
	assert 'part_base' in arg,'Нужен параметр part_base'
	assert 'previous_lay' in arg,'Нужен параметр previous_lay'
	assert 'pallet_length_x' in arg,'Нужен параметр pallet_length_x'
	assert 'pallet_length_y' in arg,'Нужен параметр pallet_length_y'
	assert 'pallet_limit_x' in arg,'Нужен параметр pallet_limit_x'
	assert 'pallet_limit_y' in arg,'Нужен параметр pallet_limit_y'
	assert 'part_indent' in arg,'Нужен параметр part_indent'
	assert 'next_lay' in arg,'Нужен параметр next_lay'
	#assert 'bin_z_max' in arg,'Нужен параметр bin_z_max'

	sys.path.append('/home/kipia/opc_BHX')
	from py3dbp import Bin, Item, Packer, Painter


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


	def put_fit_items(pallet_: Bin):
			#plpy.info(f'put_fit_items items {pallet_.items}')
			for item in pallet_.items:
					dimension = item.getDimension()
					[w, h, d] = dimension
					x = Decimal(item.position[0])
					y = Decimal(item.position[1])
					z = Decimal(item.position[2])
					pallet_.fit_items = np.append(pallet_.fit_items,
																				np.array([[Decimal(x), Decimal(x + w), Decimal(y), Decimal(y + h), Decimal(z), Decimal(z + d)]]),
																				axis=0)
			return pallet_

	def create_fit_items(pallet, previous_lay, indent, packer, offset_z,pallet_w,pallet_h):
			"""Заполнение fit_items при ручном формировании слоя"""
			item_list = []
			#plpy.info(f'create_fit_items previous_lay {previous_lay}')
			for num, part in enumerate(previous_lay, start=0):
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
					item.position= [Decimal(part['part_pos_x'])-(Decimal(part['part_length_x']+indent)/2)+Decimal(pallet_w/2),
													Decimal(part['part_pos_y'])-(Decimal(part['part_length_y']+indent)/2)+Decimal(pallet_h/2),
													Decimal(part['part_pos_z'])-offset_z]
					item_list.append(item)
					previous_lay[num]['part_length_x']=Decimal(part['part_length_x'])
					previous_lay[num]['part_length_y']=Decimal(part['part_length_y'])
					previous_lay[num]['part_thickness_z']=Decimal(part['part_thickness_z'])
					pallet.items.append(item)
					if num==(len(previous_lay)-1) and False:
							item = Item(partno=1,
							name='part',
							typeof='cube',
							#WHD=(Decimal(0),Decimal(0),Decimal(0)),
							WHD=(Decimal(part['part_length_x'] + indent),
										Decimal(part['part_length_y'] + indent),
										Decimal(part['part_thickness_z'])),
							level=0,
							weight=1,
							loadbear=0,
							updown=False,
							color="#" + ''.join([random.choice('0123456789ABCDEF') for j in range(6)])
							)
							item.rotation_type = 0
							item.position= [part['part_pos_x']-((part['part_length_x']+indent)/2)+Decimal(pallet_w/2),
															part['part_pos_y']-((part['part_length_y']+indent)/2)+Decimal(pallet_h/2),
															part['part_pos_z']-offset_z]
							item_list.append(item)
							pallet.items.append(item)
			pallet = put_fit_items(pallet)
			if DEBUG: plpy.info(f'pallet item_list {" part_id " + " ".join(str(x.partno) + " pos " + str(x.position) for x in pallet.items)}')
			return pallet
	
	def main(parts, part_base, previous_lay, pallet_w, pallet_h, indent, next_lay, lay_number) -> dict:
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
			
			for thickness in sort_parts:
					offset_z = 0	# сдвиг который вычтем, а потом вернем что бы небыло висяков
					part_base['part_thickness_z']=1
					part_thickness = thickness + part_base['part_thickness_z']
					part_base['part_id']=0
					if previous_lay:
							if next_lay:
									part_thickness = max([part['part_thickness_z'] for part in previous_lay]) + thickness
									offset_z = min([part['part_pos_z'] for part in previous_lay])
									part_thickness -= offset_z
							else:
									part_thickness = max([part['part_thickness_z'] for part in previous_lay])
									offset_z = min([part['part_pos_z'] for part in previous_lay])
									part_thickness -= offset_z
							part_base['part_pos_z']=offset_z-part_base['part_thickness_z']
					else:
							part_base['part_pos_z']=0
							part_base['lay_number']=1
							#part_length_x = part_base['part_length_x']
							part_base['part_length_x']=part_base['part_length_x']-indent
							part_base['part_length_y']=part_base['part_length_y']-indent
					previous_lay.insert(0,part_base)
					#plpy.info(f'previous_lay {previous_lay}')
					pallet = Bin(partno='Bin', WHD=(Decimal(pallet_w), Decimal(pallet_h), Decimal(part_thickness)), max_weight=28080, corner=1, put_type=1)
					# если есть предыдущий слой востановим его
					# формируем паллету по толщине деталей
					# мы ограничили высоту палеты больше одного слоя не может быть
					for i in range(len(sort_parts[thickness])):
							pallet.clearBin()
							packer = Packer()
							packer.addBin(pallet)
							if previous_lay:
									pallet = create_fit_items(pallet, previous_lay, indent, packer, offset_z,pallet_w,pallet_h)
							for item in packer.bins[0].items:
									# нужно собрать последний слой
									if item.name != 'corner':
											item.center = (Decimal(item.position[0] + (item.width) // 2)-Decimal(pallet_w/2), Decimal(item.position[1] + (item.height) // 2)-Decimal(pallet_h/2),
																		 Decimal(item.position[2]) + offset_z)
											if DEBUG: plpy.info(f'previous_lay part_id {item.partno}  X {item.center[0]} Y {item.center[1]}')							
							# меняем приоритет деталей
							level = level_generator(i, len(sort_parts[thickness]))
							item_list = []
							for part in sort_parts[thickness]:
									item = Item(partno=part['part_id'],
															name='part',
															typeof='cube',
															WHD=(Decimal(part['part_length_x']) + indent,
																	 Decimal(part['part_length_y']) + indent,
																	 Decimal(part['part_thickness_z'])
																	 ),
															level=next(level),
															weight=1,
															loadbear=0,
															updown=False,
															color="#" + ''.join([random.choice('0123456789ABCDEF') for j in range(6)]))
									item_list.append(item)
							if DEBUG: plpy.info(f'unpacked item_list {", ".join("(part_id " + str(x.partno) + " pos " + str(x.position) + " dim " + str(x.getDimension()) + ")" for x in item_list)}')
							for item in item_list:
									packer.addItem(item)
							# пакуем
							pack_res=packer.pack(
														bigger_first=False,
														fix_point=True
												)
							#assert not pack_res is None, 'pack_res is None'
							if not pack_res is None:
									assert not ('error' in pack_res), f'{pack_res["error"]}'
							if DEBUG: plpy.info(f'pack_res {pack_res}')												
							b = packer.bins[0]
							S = 0	# общая площадь занимаемая деталями
							layer_parts = []	# детали 0 слоя
							if DEBUG: plpy.info(f'packed item_list {", ".join("(part_id " + str(x.partno) + " pos " + str(x.position)  + " dim " + str(x.getDimension()) + ")" for x in b.items)}')					
							for item in b.items:
									# нужно собрать последний слой
									if item.name != 'corner' and item.partno not in [part_['part_id'] for part_ in previous_lay]:
											S += item.width * item.height
											#if DEBUG: plpy.info(f'item.width {item.width} item.position[0] {item.position[0]} item.height {item.height} item.position[1] {item.position[1]}')
											item.center = (Decimal(item.position[0] + Decimal(item.width) // 2)-Decimal(pallet_w/2), Decimal(item.position[1] + Decimal(item.height) // 2)-Decimal(pallet_h/2), Decimal(item.position[2]) + Decimal(offset_z))
											layer_parts.append({'part_pos_x': Decimal(item.center[0]),
																					'part_pos_y': Decimal(item.center[1]),
																					'part_pos_z': Decimal(item.center[2]),
																					'part_id': item.partno,
																					'lay_number': (Decimal(item.center[2]) + Decimal(offset_z))//thickness + lay_number,
																					'part_angle_a': 0 if item.rotation_type == 0 else 90})
											if DEBUG: plpy.info(f'part_id {item.partno}  X {item.center[0]} Y {item.center[1]}')
							percent = int((S / pallet_s) * 100)	# процент заполнения слоя
							if percent >= max_percent:
									max_percent = percent
									result_parts = layer_parts.copy()
			# нужно вернуть данные только о новых деталях
			if DEBUG: plpy.info(f'result_parts {result_parts} previous_lay {previous_lay}')					
			#result_parts = [part for part in result_parts
			#								if part['part_id'] not in [part_['part_id'] for part_ in previous_lay]]
			if len(result_parts)==0:	
					for item in item_list:
							result_parts.append({'part_pos_x': 0,
																					'part_pos_y': 0,
																					'part_pos_z': 0,
																					'part_id': item.partno,
																					'lay_number': offset_z//thickness + lay_number,
																					'part_angle_a': 0 if item.rotation_type == 0 else 90})
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
			traceback_info=traceback.format_exc().splitlines()[1].split(', ')
			source=traceback_info[1]+' '+'_'.join(traceback_info[2].split('_')[slice(4,-1)])
			#source=traceback_info
			err_arg = [traceback_info[1]+': '+str(e),simplejson.dumps(args,ensure_ascii=False,use_decimal=True)]
			res = plpy.execute(plpy.prepare(f'SELECT write_error_log($1,$2)', ['text','text']), err_arg)
			return dict(source=source,error=str(e),args=arg,sort_parts=sort_parts)
	
	parts, part_base, previous_lay, pallet_w, pallet_h, indent, next_lay = (arg['parts'], arg['part_base'], arg['previous_lay'], arg['pallet_length_x'], arg['pallet_length_y'], arg['part_indent'], arg['next_lay'])

	# сформированный слой, невошедшие детали, вошедшие детали
	if len(previous_lay)>0:	
			for num, part in enumerate(previous_lay, start=0):
					previous_lay[num]['part_length_x']=Decimal(part['part_length_x'])
					previous_lay[num]['part_length_y']=Decimal(part['part_length_y'])
					previous_lay[num]['part_thickness_z']=Decimal(part['part_thickness_z'])
			lay_number=previous_lay[0]['lay_number']
	else:
	#		previous_lay=[dict(part_pos_x=0,part_pos_y=0,part_pos_z=0,lay_number=1,part_length_x=800,part_length_y=1200,part_thickness_z=10,part_id=0,part_angle_a=0)]
			lay_number=1
	if DEBUG: plpy.info(f'arg {parts}, {previous_lay}, {pallet_w}, {pallet_h}, {indent}, {next_lay}, {lay_number}')
	res = main(parts, part_base, previous_lay, pallet_w, pallet_h, indent, next_lay, lay_number)
	assert not 'error' in res, f'{res["error"]} {res["source"]} {res["sort_parts"]} main args {parts}, {previous_lay}, {pallet_w}, {pallet_h}, {indent}'
	return res
except Exception as e:
	traceback_info=traceback.format_exc().splitlines()[1].split(', ')
	source=traceback_info[1]+' '+'_'.join(traceback_info[2].split('_')[slice(4,-1)])
	#source=traceback_info
	err_arg = [traceback_info[1]+': '+str(e),simplejson.dumps(args,ensure_ascii=False,use_decimal=True)]
	res = plpy.execute(plpy.prepare(f'SELECT write_error_log($1,$2)', ['text','text']), err_arg)
	return dict(source=source,error=str(e),args=arg)
$BODY$
	LANGUAGE plpython3u
	COST 100;
COMMENT ON FUNCTION process_packing IS 'Упаковка слоя';
END$$;

/*
SELECT debug_off();
SELECT debug_on();
SELECT process_packing('{"parts": [{"part_id": 209758001800001, "lay_number": 3, "part_pos_x": -246.13284, "part_pos_y": -97.377205, "part_pos_z": 0, "part_angle_a": 91, "part_length_x": 400, "part_length_y": 184, "part_thickness_z": 16}, {"part_id": 209758001800002, "lay_number": 3, "part_pos_x": 3.701493, "part_pos_y": -175.41832, "part_pos_z": 0, "part_angle_a": 269, "part_length_x": 400, "part_length_y": 184, "part_thickness_z": 16}], "next_lay": true, "part_indent": 50, "previous_lay": [{"part_id": 209758003000001, "lay_number": 1, "part_pos_x": 226, "part_pos_y": 117, "part_pos_z": 0, "part_angle_a": 0, "part_length_x": 400, "part_length_y": 184, "part_thickness_z": 16}, {"part_id": 209758003000002, "lay_number": 1, "part_pos_x": 676, "part_pos_y": 117, "part_pos_z": 0, "part_angle_a": 0, "part_length_x": 400, "part_length_y": 184, "part_thickness_z": 16}], "pallet_length_x": 2800, "pallet_length_y": 864}');

SELECT process_packing('{"parts": [{"part_id": 209758000309001, "lay_number": 2, "part_pos_x": -615, "part_pos_y": -108, "part_pos_z": 16, "part_angle_a": 0, "part_length_x": 1471, "part_length_y": 548, "part_thickness_z": 16}], "next_lay": true, "part_indent": 50, "previous_lay": [], "pallet_length_x": 800, "pallet_length_y": 1200}');
*/

--Исходные данные:
--размер основания (прямоугольник, под которым есть основание на всех слоях)
--размер корзины (прямоугольник, за пределы которого не может выходить свисающая деталь)

--Стратегии упаковки
--1. Положить деталь, которая не помещается на паллету целиком (свисает) по центру паллеты
--2. Доложить деталь в слой, которая помещается

DO $$ --get_part_layers
BEGIN
DROP FUNCTION IF EXISTS get_part_layers(json_data jsonb);
CREATE OR REPLACE FUNCTION get_part_layers(json_data jsonb)
RETURNS jsonb
AS $BODY$
DECLARE
	err_context text;
	jpart_layers jsonb;
	jpart_base jsonb;
	max_lay int4;
	pos_slot bool;
	RESULT jsonb;
BEGIN
		pos_slot:=json_data->>'part_status'='part_slot';
		SELECT COALESCE(MAX(lay_number),1)
				FROM parts 
																			WHERE part_status::text=(json_data->>'part_status') AND robot_id=(json_data->>'robot_id')::int8
																						AND (json_data->>'part_slot' IS NULL OR part_slot=(json_data->'part_slot')::int4 OR json_data->>'part_status'!='part_slot')
																						AND (json_data->>'order_id' IS NULL OR order_id=(json_data->'order_id')::int8)
				INTO max_lay;
				RAISE INFO 'max_lay %', max_lay;
		SELECT COALESCE((SELECT jsonb_AGG(
									 jsonb_build_object( 'part_id',id
																			,'part_length_x',part_length_x
																			,'part_length_y',part_length_y
																			,'part_thickness_z', part_thickness_z
																			,'part_pos_x',CASE WHEN pos_slot THEN slot_pos_x ELSE part_pos_x END
																			,'part_pos_y',CASE WHEN pos_slot THEN slot_pos_y ELSE part_pos_y END
																			,'part_pos_z',CASE WHEN pos_slot THEN slot_pos_z ELSE part_pos_z END
																			,'part_angle_a',part_angle_a
																			,'lay_number',lay_number
																			))
																													FROM parts
																			WHERE id!=0 AND part_status::text=(json_data->>'part_status') AND robot_id=(json_data->>'robot_id')::int8
																						AND (lay_number=(json_data->'lay_number')::int4 OR ((json_data->>'lay_number' IS NULL OR (json_data->>'lay_number')::int4=0) AND lay_number=max_lay)) 
																						AND ((part_slot=(json_data->'part_slot')::int4)
																										OR json_data->>'part_slot' IS NULL OR json_data->>'part_status'!='part_slot')
																						AND (json_data->>'order_id' IS NULL OR order_id=(json_data->'order_id')::int8)
											),'[]')
		INTO jpart_layers;
		
		

		jpart_layers := jsonb_build_object('part_layers',jpart_layers,'part_location',json_data);
		
		IF json_data->>'part_status'!='pallet_in' THEN
				SELECT get_base_dim(jsonb_build_object('lays', get_pallet_parts(json_data)
																							,'buffer', setting_get('pallet_buffer')
																							,'pallet_length_x', setting_get(json_data->>'part_status' || '_length_x')
																							,'pallet_length_y', setting_get(json_data->>'part_status' || '_length_y')))
				INTO jpart_base;
				jpart_layers := jpart_layers || jsonb_build_object('part_base',jpart_base);
		END IF;
		
		
		RESULT := jsonb_build_object('result','OK')||jpart_layers;
		PERFORM pg_notify('part_layers',jpart_layers::text);

		EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'get_part_layers',	json_data::text);
		RETURN RESULT;
	EXCEPTION
			WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
			err_context := SUBSTRING ( err_context FROM '%function #"%%#" line%' FOR '#' ) || SUBSTRING ( err_context FROM '%[)]#" line ___#"%' FOR '#' );
			RAISE INFO '%', SQLERRM;
			PERFORM write_error_log ( SQLERRM, '', err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
COMMENT ON FUNCTION get_part_layers IS 'Выгрузка слоёв';	
END$$;

/*

SELECT get_part_layers('{"part_status":"pallet_out", "robot_id":1,"lay_number":1,"pallet_length_x":1200,"pallet_length_y":800}');
SELECT get_part_layers('{"part_status":"pallet_in", "robot_id":1,"pallet_length_x":600,"pallet_length_y":500}');
SELECT get_part_layers('{"part_status":"part_slot", "robot_id":3,"part_slot":1}');

		SELECT COALESCE((SELECT jsonb_AGG(
									 jsonb_build_object( 'part_id',id
																			,'part_length_x',part_length_x
																			,'part_length_y',part_length_y
																			,'part_thickness_z', part_thickness_z
																			,'part_pos_x',part_pos_x --+ COALESCE(json_data->'pallet_length_x',setting_get('pallet_length_x'))::int4
																			,'part_pos_y',part_pos_y --+ COALESCE(json_data->'pallet_length_y',setting_get('pallet_length_y'))::int4
																			,'part_pos_z',part_pos_z
																			,'part_angle_a',part_angle_a
																			,'lay_number',lay_number
																			))
																													FROM parts 
																			WHERE part_status::text='part_slot' AND robot_id=1
																						AND (lay_number=NULL OR ((NULL IS NULL OR NULL=0) AND lay_number=1)) 
																						AND ((part_slot=1)
																										OR 1 IS NULL OR 'part_slot'!='part_slot')
											),'[]')

*/



DO $$ --pack_parts
BEGIN
DROP FUNCTION IF EXISTS pack_parts;
CREATE OR REPLACE FUNCTION pack_parts(json_data jsonb)
RETURNS jsonb AS $BODY$
DECLARE
	err_context text;
	jprevious_lay jsonb;
	jpacking_data jsonb;
	jpacking_output jsonb;
	jpart_layers jsonb;
	jparts jsonb;
	jpart_base jsonb;
	jpart_ids jsonb;
	jtask jsonb;
	jslotpart jsonb;
	id_robot int8;
	count_slot int4;
	upd_slot_part int4;
	slot_part int4;
	thickness_part_z int4;
	--start_slot int4;
	RESULT jsonb;
BEGIN
		json_data:=json_data-'part_slot'-'lay_number'-'pallet_length_x'-'pallet_length_y';
		
		
		--TODO проверять, если несколько толщин
		--SELECT jsonb_agg(
		--									 jsonb_build_object('part_id', id
		--																		 ,'part_length_x', part_length_x
		--																		 ,'part_length_y', part_length_y
		--																		 ,'part_thickness_z', part_thickness_z)), part_thickness_z
		--			FROM parts
		--			WHERE json_data->'part_ids' @> to_jsonb(id)
		--			GROUP BY part_thickness_z
		--			INTO jparts, thickness_part_z;
					

		id_robot:=(json_data->'robot_id')::int8;
		SELECT get_part_layers(jsonb_build_object('part_status', json_data->'part_source','robot_id', id_robot)) --,'order_id', json_data->'order_id'
				INTO jpart_layers;
		IF jpart_layers ? 'error' THEN
				RAISE EXCEPTION 'In get_part_layers % %', jpart_layers->'error', jpart_layers->'source';
		END IF;
		jparts:=jpart_layers->'part_layers';
		jpart_ids:=jsonb_path_query_array(jparts, '$[*].part_id');
		IF is_debug() THEN
				RAISE INFO 'jparts %', jparts;
		END IF;
		thickness_part_z:=COALESCE((jparts->0->'part_thickness_z')::int4,0);
		IF is_debug () THEN
				RAISE INFO 'part_thickness_z %', thickness_part_z;
		END IF;
		IF (json_data->>'part_destination')='part_slot' THEN
				SELECT slot_count FROM robots WHERE id=id_robot INTO count_slot;
				IF is_debug () THEN
						RAISE INFO 'count_slot %', count_slot;
				END IF;
				FOR slot_part IN 1..count_slot LOOP
						IF is_debug () THEN
								RAISE INFO 'slot_part %', slot_part;
						END IF;
						upd_slot_part:=slot_part;
						SELECT get_part_layers(jsonb_build_object('part_status', 'part_slot'
																															,'robot_id', id_robot
																															,'part_slot', slot_part))
								INTO jpart_layers;
						IF jpart_layers IS NULL THEN
								RAISE EXCEPTION 'jpart_layers is NULL id_robot % part_slot % ', id_robot, slot_part;
						END IF;						
						IF jpart_layers ? 'error' THEN
								RAISE EXCEPTION 'In get_part_layers % %', jpart_layers->'error', jpart_layers->'source';
						END IF;
						jprevious_lay:=jpart_layers->'part_layers';
						jpart_base:=jpart_layers->'part_base';
						IF is_debug () THEN
								RAISE INFO 'jprevious_lay %', jprevious_lay;
						END IF;
						SELECT jsonb_build_object( 'parts', jparts
																			,'pallet_length_x', setting_get('part_slot_length_x')
																			,'pallet_length_y', setting_get('part_slot_length_y')
																			,'pallet_limit_x', setting_get('part_slot_limit_x')
																			,'pallet_limit_y', setting_get('part_slot_limit_y')
																			,'part_indent', setting_get('part_indent')
																			,'next_lay', ((COALESCE((jprevious_lay->0->'part_pos_z')::int4+
																														 (jprevious_lay->0->'part_thickness_z')::int4,0)+
																														 thickness_part_z) < setting_get('slot_thickness_z')::int4)
																			,'part_base',jpart_base
																			,'previous_lay',jprevious_lay)
							
							INTO jpacking_data;
						IF is_debug () THEN
								RAISE INFO 'jpacking_data %', jpacking_data;
						END IF;
						SELECT process_packing(jpacking_data) INTO jpacking_output;
						IF is_debug () THEN
								RAISE INFO 'jpacking_output %', jpacking_output;
						END IF;
						IF jpacking_output ? 'error' THEN
								RAISE EXCEPTION 'In process_packing % %', jpacking_output->'error', jpacking_output->'source';
						END IF;
						--IF NOT (jpacking_output ? 'error') THEN
								IF NOT (jpacking_output ? 'error') AND jsonb_array_length(jpacking_output->'current_lay')>0 THEN
										IF is_debug () THEN
												RAISE INFO 'upd_slot_part %', upd_slot_part;
										END IF;
										UPDATE parts SET slot_pos_x = p.part_pos_x
																	,slot_pos_y = p.part_pos_y
																	,slot_pos_z = p.part_pos_z
																	,slot_angle_a = p.part_angle_a
																	,part_slot = COALESCE(upd_slot_part, part_slot)
																	--,part_status = 'slot2out'
																	,lay_number = p.lay_number
													FROM (SELECT * FROM jsonb_array_elements(jpacking_output->'current_lay') a(parts)
																						, jsonb_to_record (parts) AS x ( 
																																						part_id int8,
																																						lay_number int2,
																																						part_pos_x real,
																																						part_pos_y real,
																																						part_pos_z real,
																																						part_angle_a real)
																	WHERE jpart_ids @> to_jsonb(part_id)
																) AS p
														WHERE id=p.part_id;
						--ELSE
						--		RETURN jsonb_build_object('result','NOK','error', 'NO PLACE FOR TRANSFER');						
						--END IF;
								EXIT;
						END IF;
				END LOOP;
		END IF;
		IF (json_data->>'part_destination')='pallet_out' THEN
						jpart_layers:=get_part_layers(jsonb_build_object( 'part_status', 'pallet_out'
																															,'robot_id', id_robot)); --,'order_id', json_data->'order_id'
						IF jpart_layers IS NULL THEN
								RAISE EXCEPTION 'jpart_layers is NULL id_robot % part_slot % ', id_robot, slot_part;
						END IF;
						IF jpart_layers ? 'error' THEN
								RAISE EXCEPTION 'In get_part_layers % %', jpart_layers->'error', jpart_layers->'source';
						END IF;
						jprevious_lay:=jpart_layers->'part_layers';
						jpart_base:=jpart_layers->'part_base';
						IF is_debug() THEN
								RAISE INFO 'jprevious_lay %', jprevious_lay;
						END IF;
						SELECT jsonb_build_object( 'parts', jparts
																			,'pallet_length_x', setting_get('pallet_out_length_x')
																			,'pallet_length_y', setting_get('pallet_out_length_y')
																			,'pallet_limit_x', setting_get('pallet_out_limit_x')
																			,'pallet_limit_y', setting_get('pallet_out_limit_y')
																			,'part_indent', setting_get('part_indent')
																			,'next_lay', TRUE
																			,'part_base',jpart_base
																			,'previous_lay',jprevious_lay)
							INTO jpacking_data;
						IF is_debug () THEN
								RAISE INFO 'jpacking_data %', jpacking_data;
						END IF;
						SELECT process_packing(jpacking_data) INTO jpacking_output;																															
						IF jpacking_output ? 'error' THEN
								RAISE EXCEPTION 'In process_packing % %', jpacking_output->'error', jpacking_output->'source';
						END IF;
						RAISE INFO 'jpacking_output %', jpacking_output;
						
						PERFORM task_add(jsonb_build_object( 'part_id', part_id::int8
																								,'robot_id', id_robot
																							--,'gripper_id', lay.gripper_id
																								,'operation_type', 'transfer_slot2out'
																								,'part_number', 1
																								,'operation_number', 1
																								,'operation_content', jsonb_build_object()
																								,'operation_side', ' '
																								)
																)
										FROM jsonb_array_elements(jpart_ids) p(part_id);
														IF NOT (jpacking_output ? 'error') AND jsonb_array_length(jpacking_output->'current_lay')>0 THEN
								RAISE INFO 'upd_slot_part %', upd_slot_part;
								UPDATE parts SET part_pos_x = p.part_pos_x
																	,part_pos_y = p.part_pos_y
																	,part_pos_z = p.part_pos_z
																	,part_angle_a = p.part_angle_a
																	,part_slot = COALESCE(upd_slot_part, part_slot)
																	--,part_status = 'slot2out'
																	,lay_number = p.lay_number
													FROM (SELECT * FROM jsonb_array_elements(jpacking_output->'current_lay') a(parts)
																						, jsonb_to_record (parts) AS x ( 
																																						part_id int8,
																																						lay_number int2,
																																						part_pos_x real,
																																						part_pos_y real,
																																						part_pos_z real,
																																						part_angle_a real)
																	WHERE jpart_ids @> to_jsonb(part_id)
																) AS p
														WHERE id=p.part_id;
														ELSE
								RETURN jsonb_build_object('result','NOK','error', 'NO PLACE FOR TRANSFER');						
						END IF;
		END IF;
													--RETURNING jsonb_build_object('result','OK'
													--														,'filling',jpacking_output->'filling'
													--														,'part_id',id
													--														,'part_pos_x',slot_pos_x
													--														,'part_pos_y',slot_pos_y
													--														,'part_angle_a',slot_angle_a)
													--INTO jslotpart;
											--IF jslotpart IS NULL THEN
											--		RAISE EXCEPTION 'jprevious_lay is NULL jpacking_output % ', jpacking_output;
											--END IF;
											--RAISE INFO 'jslotpart %', jslotpart;
								--TODO проверка (захвата из слота?) и укладки
								/*
								PERFORM task_add(jsonb_build_object( 'part_id', (jslotpart->'part_id')::int8
																										,'robot_id', id_robot
																										--,'gripper_id', lay.gripper_id
																										,'operation_type', 'transfer_in2slot'
																										,'part_number', 1
																										,'operation_number', 1
																										,'operation_content', jsonb_build_object()
																										,'operation_side', ' '
																										)
																);
								*/
								--SELECT gen_robot_task((jslotpart->'part_id')::int8,id_robot) INTO jtask;
								--RESULT := jsonb_build_object('result','OK','robot_task',jtask,'filling',jslotpart->'filling');
								RESULT := jsonb_build_object('result','OK','filling',jpacking_output->'filling');
								EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'pack_parts',	json_data);
								RETURN RESULT;
	EXCEPTION 
			WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
			err_context := SUBSTRING ( err_context FROM '%function #"%%#" line%' FOR '#' ) || SUBSTRING ( err_context FROM '%[)]#" line ___#"%' FOR '#' );
			RAISE INFO '%', SQLERRM;
			PERFORM write_error_log ( SQLERRM, '', err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
COMMENT ON FUNCTION pack_parts IS 'Упаковка следующей детали';	
END$$;

/*

SELECT '[{"part_id": 150, "lay_number": 1, "part_pos_x": -182.46588, "part_pos_y": -80.03568, "part_pos_z": 0, "part_angle_a": 352, "part_length_x": 580, "part_length_y": 380, "part_thickness_z": 16}, {"part_id": 151, "lay_number": 1, "part_pos_x": -548.4367, "part_pos_y": -118.98344, "part_pos_z": 0, "part_angle_a": 357, "part_length_x": 568, "part_length_y": 382, "part_thickness_z": 16}, {"part_id": 152, "lay_number": 1, "part_pos_x": 21.545185, "part_pos_y": 259.031, "part_pos_z": 0, "part_angle_a": 179, "part_length_x": 568, "part_length_y": 382, "part_thickness_z": 16}]'::jsonb->0->'part_thickness_z';

SELECT pack_parts('{"part_source":"pallet_in","part_destination":"part_slot", "robot_id":3}');
SELECT pack_parts('{"part_source":"part_slot","part_destination":"pallet_out","robot_id":3}');

*/
--


DO $$ --get_pallet_parts
BEGIN
DROP FUNCTION IF EXISTS get_pallet_parts(json_data jsonb);
CREATE OR REPLACE FUNCTION get_pallet_parts(json_data jsonb)
RETURNS jsonb
AS $BODY$
DECLARE
	err_context text;
	jpart_layers jsonb;
	max_lay int4;
	id_order int8;
	pos_slot bool;
	RESULT jsonb;
BEGIN
	pos_slot:= json_data->>'part_status'='part_slot';
	SELECT order_id FROM parts
			WHERE change_time=(SELECT MAX(change_time) FROM parts WHERE id!=0) 
			LIMIT 1
			INTO id_order;
		SELECT COALESCE((SELECT 
									 jsonb_build_object( --'part_id',jsonb_AGG(id),
																			 'part_length_x',jsonb_AGG(part_length_x)
																			,'part_length_y',jsonb_AGG(part_length_y)
																			--,'part_thickness_z', jsonb_AGG(part_thickness_z)
																			,'part_pos_x',jsonb_AGG(part_pos_x)
																			,'part_pos_y',jsonb_AGG(part_pos_y)
																			,'part_pos_z',jsonb_AGG(part_pos_z)
																			,'part_angle_a',jsonb_AGG(part_angle_a)
																			,'lay_number',jsonb_AGG(lay_number)
																			)
																			FROM ( SELECT jsonb_AGG(p.part_length_x) AS part_length_x,
																										jsonb_AGG(p.part_length_y) AS part_length_y,
																										jsonb_AGG(p.part_angle_a) AS part_angle_a,
																										jsonb_AGG(round((CASE WHEN pos_slot THEN slot_pos_x ELSE part_pos_x END)::numeric,1)) AS part_pos_x,
																										jsonb_AGG(round((CASE WHEN pos_slot THEN slot_pos_y ELSE part_pos_y END)::numeric,1)) AS part_pos_y,
																										jsonb_AGG(round((CASE WHEN pos_slot THEN slot_pos_z ELSE part_pos_z END)::numeric,1)) AS part_pos_z,
																										p.lay_number
																			FROM parts p --JOIN orders o on p.order_id=o.id
																			WHERE 
																			p.order_id=id_order
																				--	(json_data->>'order_number' IS NULL OR o.order_number=(json_data->'order_number')::int8)
																			AND p.part_status::text=(json_data->>'part_status') 
																			AND (json_data->>'robot_id' IS NULL	OR p.robot_id=(json_data->'robot_id')::int8)
																			AND (json_data->>'part_slot' IS NULL	OR p.part_slot=(json_data->'part_slot')::int2 OR NOT pos_slot)
																			GROUP BY lay_number
																			ORDER BY lay_number
					--LIMIT 10
					) prt
					
											),'[]')
	
		INTO jpart_layers;
		
		IF jpart_layers->>'lay_number' IS NULL THEN
				jpart_layers:=jsonb_build_array();
		END IF;
		
		

		--jpart_layers := jsonb_build_object('part_layers',jpart_layers,'part_location',json_data);
		--RESULT := jsonb_build_object('result','OK')||jpart_layers;
		RESULT := jpart_layers;
		--PERFORM pg_notify('part_layers',jpart_layers::text);

		EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'get_pallet_parts',	json_data::text);
		RETURN RESULT;
	EXCEPTION
			WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
			err_context := SUBSTRING ( err_context FROM '%function #"%%#" line%' FOR '#' ) || SUBSTRING ( err_context FROM '%[)]#" line ___#"%' FOR '#' );
			RAISE INFO '%', SQLERRM;
			PERFORM write_error_log ( SQLERRM, '', err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
COMMENT ON FUNCTION get_pallet_parts IS 'Выгрузка слоёв';	
END$$;

/*
--SELECT get_pallet_parts('{"part_status":"pallet_out","order_number":3860,"robot_id":1}');
SELECT get_pallet_parts('{"part_status":"pallet_out","robot_id":1}');
SELECT get_pallet_parts('{"part_status":"part_slot","robot_id":1}');
SELECT get_pallet_parts('{"part_status":"part_slot","part_slot":1,"robot_id":1}');
--substring(::text FROM 1 FOR 8000)
*/

DO $$ --get_base_dim
BEGIN
DROP FUNCTION IF EXISTS get_base_dim;
CREATE OR REPLACE FUNCTION get_base_dim(arg jsonb)
  RETURNS jsonb 
	TRANSFORM FOR TYPE jsonb
AS $BODY$
# TRANSFORM FOR TYPE jsonb
import traceback
import inspect
import json
import simplejson
#import logging
import shapely.geometry
#import shapely.affinity
#import math
from shapely.geometry import MultiPoint, Point
import numpy as np

try:
	arg=json.loads(simplejson.dumps(args[0], use_decimal=True))
	DEBUG = plpy.execute('SELECT is_debug()')[0]['is_debug']
	assert isinstance(arg, dict),'Проверь формат JSON'
	assert 'lays' in arg,'Нужен объект lays'
	assert 'buffer' in arg,'Нужен параметер buffer'
	assert 'pallet_length_x' in arg,'Нужен параметер pallet_length_x'
	assert 'pallet_length_y' in arg,'Нужен параметер pallet_length_y'
	#assert 'part_pos_x' in arg['lays'],'Нужен массив part_pos_x в lays'
	#assert len(arg['lays']['part_pos_x'])>0,'Нужен не пустой массив parts'

	#logger = logging.getLogger("opc_py")

	class Rect:
			def __init__(self, centre_X, centre_Y, length_X, length_Y, thickness_Z, angle):
					self.centre_X = centre_X
					self.centre_Y = centre_Y
					self.length_X = length_X
					self.length_Y = length_Y
					self.thickness_Z = thickness_Z
					self.angle = angle

			def get_contour(self):
					w = self.length_X
					h = self.length_Y
					c = shapely.geometry.box(-w/2.0, -h/2.0, w/2.0, h/2.0)
					rc = shapely.affinity.rotate(c, self.angle)
					return shapely.affinity.translate(rc, self.centre_X, self.centre_Y)

			def translate(self,x,y):
					theta = math.radians(self.angle)
					cos_theta, sin_theta = math.cos(theta), math.sin(theta)
					self.centre_X = self.centre_X + x * cos_theta - y * sin_theta
					self.centre_Y = self.centre_Y + x * sin_theta + y * cos_theta

			def intersection(self, rect):
					contour = rect.get_contour()
					if DEBUG: plpy.info(f"Периметр детали {rect.part_id}: {contour} ")    
					return bool(contour.intersection(self.get_contour()))

    
	pallet = Rect(0, 0, arg['pallet_length_x'], arg['pallet_length_y'], 0, 0)
	lay_base = pallet.get_contour()
	lays=arg['lays']
	buffer=arg['buffer']
	#plpy.info('for i, lay in lay_number')
	if lays:
		for i, lay in enumerate(lays['lay_number']):
			point_list = []
			#plpy.info('i',i)
			for j, px in enumerate(lays['part_pos_x'][i]):
				#plpy.info('i',i,'j',j)
				lay_part = Rect(lays['part_pos_x'][i][j], lays['part_pos_y'][i][j], lays['part_length_x'][i][j],lays['part_length_y'][i][j], 0, lays['part_angle_a'][i][j])
				point_list = point_list + list(lay_part.get_contour().exterior.coords)
				#plpy.info('point_list',point_list)
			lay_polygon = MultiPoint(point_list).convex_hull
			#plpy.info('lay_polygon',lay_polygon.exterior.coords.xy)
			#plpy.info('lay_base',lay_base.exterior.coords.xy)
			cur_base = lay_base.intersection(lay_polygon)
			if not cur_base:
				plpy.info(f'wrong part on lay {i} {list(lay_polygon.exterior.coords)}')
			else:
				#lay_base = lay_base.simplify(100)			
				#lay_base = cur_base
				lay_base = cur_base.buffer(buffer)
			#lay_base = [poly.exterior.coords for poly in list(lay_polygon)]
			#plpy.info('lay_base',lay_base.exterior.coords.xy)
	#plpy.info('lay_base',lay_base.exterior.coords)
	#

	#plpy.info('lay_base simplify 100',lay_base.exterior.coords)
	# get minimum bounding box around polygon
	base_rect = lay_base.minimum_rotated_rectangle
	# get coordinates of polygon vertices
	x, y = base_rect.exterior.coords.xy
	# get length of polygon as the longest edge of the bounding box
	pallet.length_X = round(Point(x[0], y[0]).distance(Point(x[1], y[1])), 1)
	#plpy.info(2)

	# get width of polygon as the shortest edge of the bounding box
	pallet.length_Y = round(Point(x[1], y[1]).distance(Point(x[2], y[2])), 1)
	#plpy.info('base_rect.centroid.coords',base_rect.centroid.coords)
	centre_X, centre_Y = base_rect.centroid.coords.xy
	pallet.centre_X, pallet.centre_Y = round(centre_X[0], 1), round(centre_Y[0], 1)

	angle = np.arctan2(y[1] - y[0], x[1] - x[0])
	pallet.angle = round(int(np.degrees(angle) if angle > 0 else np.degrees(angle) + 180), 1)-90
	#plpy.info('pallet.centre_X', pallet.centre_X, 'pallet.centre_Y', pallet.centre_Y,'pallet.angle',pallet.angle)

	res = dict(part_pos_x=pallet.centre_X, part_pos_y=pallet.centre_Y, part_angle_a=pallet.angle, part_length_x=pallet.length_X, part_length_y=pallet.length_Y) 
	#,part_pos_z=-1,lay_number=1,part_thickness_z=1,part_id=0
	#plpy.info('res',res)
	return res
except Exception as e:
  traceback_info = traceback.format_exc().splitlines()[1].split(', ')
  source = traceback_info[1] + ' ' + '_'.join(traceback_info[2].split('_')[slice(4, -1)])
  err_arg = [traceback_info[1] + ': ' + str(e), simplejson.dumps(args, ensure_ascii=False, use_decimal=True)]
  res = plpy.execute(plpy.prepare(f'SELECT write_error_log($1,$2)', ['text', 'text']), err_arg)
  return dict(source=source, error=str(e), args=arg)
$BODY$
	LANGUAGE plpython3u
	COST 100;
COMMENT ON FUNCTION get_base_dim IS 'Размеры основания';	
END$$;



/*
SELECT get_base_dim(jsonb_build_object('lays', get_pallet_parts('{"part_status":"part_slot","robot_id":1}')
																			,'buffer', setting_get('pallet_buffer')
																			,'pallet_length_x', setting_get('pallet_length_x')
																			,'pallet_length_y', setting_get('pallet_length_y')));

SELECT get_base_dim(jsonb_build_object('lays', get_pallet_parts('{"part_status":"pallet_out","robot_id":1}')
																			,'buffer', setting_get('pallet_buffer')
																			,'pallet_length_x', setting_get('pallet_length_x')
																			,'pallet_length_y', setting_get('pallet_length_y')));

SELECT get_pallet_parts('{"part_status":"pallet_out","order_number":3860,"robot_id":1}');

--substring(::text FROM 1 FOR 8000)
*/

--TODO
--добавить вычисление lay_number в current_lay
--после перекладки обновлять номер слоя!
--проверка (захвата из слота?) и укладки
--режим упаковки слоя - по одной детали или скопом
