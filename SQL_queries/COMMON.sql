/*
create extension jsonb_plpython3u cascade;
*/
--Включить отладку
DO $$ --DEBUG_ON
BEGIN
	CREATE 
		OR REPLACE FUNCTION debug_on ( ) RETURNS VOID AS $BODY$ 
		SET GLOBAL.debug TO TRUE;
	$BODY$ LANGUAGE SQL;
	COMMENT ON FUNCTION debug_on IS 'Включить отладку';
	
END $$;
--SELECT debug_on();

--Выключить отладку
DO $$ --DEBUG_OFF
BEGIN
	CREATE 
		OR REPLACE FUNCTION debug_off ( ) RETURNS VOID AS $BODY$ 
		SET GLOBAL.debug TO FALSE;
	$BODY$ LANGUAGE SQL;
	COMMENT ON FUNCTION debug_off IS 'Выключить отладку';
	
END $$;
--SELECT debug_off();

--Включена ли отладка
DO $$ --is_debug
BEGIN
	CREATE 
		OR REPLACE FUNCTION is_debug ( ) RETURNS BOOLEAN AS $BODY$ SELECT COALESCE
	( CASE current_setting ( 'global.debug', TRUE ) WHEN '' THEN FALSE ELSE current_setting ( 'global.debug', TRUE ) :: BOOLEAN END, FALSE ); 
$BODY$ LANGUAGE SQL IMMUTABLE;
COMMENT ON FUNCTION is_debug IS 'Включена ли отладка';

END $$;
--SELECT is_debug();

--Записать ошибку в базу данных
DO $$ --write_error_log()
BEGIN
DROP FUNCTION IF EXISTS write_error_log;
CREATE OR REPLACE FUNCTION write_error_log(msg text, data text, err_context text=NULL)
  RETURNS pg_catalog.jsonb 
	TRANSFORM FOR TYPE jsonb
	AS $BODY$
#	TRANSFORM FOR TYPE jsonb
import traceback
import inspect
import simplejson
import time
import json
try: 
	DEBUG = False
	DEBUG = plpy.execute('SELECT is_debug()')[0]['is_debug']
	source=err_context
	#if DEBUG: plpy.info(f'inspect.stack() {inspect.stack()}')
	if len(inspect.stack())>2:
		if len(inspect.stack()[2])>3:
			source='_'.join(inspect.stack()[2][3].split('_')[slice(4,-1)])
	if DEBUG: plpy.info(f'write_error_log {source}, msg {msg}, data {data}')
	plan = plpy.prepare('INSERT INTO error_log (error_text,source,data) VALUES ($1,$2,$3)', ['text','text','text'])
	res = plpy.execute(plan, [msg,source,data])
	opc_msg=dict()
	if not source:
		source='OPC:'
	opc_msg['error']=source+' '+msg
	#opc_msg['args']=json.loads(data)
	#if DEBUG: plpy.info(f'opc_msg["args"]={opc_msg["args"]}')
	#opc_msg['args']=data#[:50] - закомментировал аргументы для телеграмма 210421
	#opc_msg['schema']=plpy.execute('select current_schema')[0]['current_schema']
	opc_msg['database']=plpy.execute('select current_database()')[0]['current_database']
	#if DEBUG: plpy.info(f'opc_msg {opc_msg}')
	plan = plpy.prepare('SELECT send2opc($1,$2)', ['jsonb','int'])
	port=6003
	res = plpy.execute(plan, [json.dumps(opc_msg, ensure_ascii=False),port])
	#if DEBUG: plpy.info(f'res {res}')
	notify_text = simplejson.dumps(dict(source='error in '+source+'<br/>'+data,time_clock=time.ctime(),message=msg), use_decimal=True,ensure_ascii=False)[:4999]
	pg_notify = plpy.execute(plpy.prepare(f'SELECT pg_notify($1,$2)', ['text','text']), ['debug',notify_text])
	return dict(result='OK')
except Exception as e:
	if DEBUG: plpy.info(f'error in write_error_log {str(traceback.format_exc().splitlines()[1].split(", ")[1])} {str(e)}')
	return dict(error=str(e),source=inspect.stack()[0][3])
$BODY$
  LANGUAGE plpython3u VOLATILE
  COST 100;
COMMENT ON FUNCTION write_error_log IS 'Лог ошибок';	
END$$;


/*
SELECT debug_on();
SELECT write_error_log('Ошибка 1','{"function":"get_sheets","data":{"number":756945}}');
*/

--записать событие в базу данных
DO $$ --write_info_log()
BEGIN
DROP FUNCTION IF EXISTS write_info_log;
CREATE OR REPLACE FUNCTION "write_info_log"(msg text, data text)
  RETURNS "pg_catalog"."jsonb" 
	TRANSFORM FOR TYPE jsonb
	AS $BODY$
# TRANSFORM FOR TYPE jsonb
import traceback
import inspect
import json
try: 
	DEBUG = True # plpy.execute('SELECT is_debug()')[0]['is_debug']
	source='_'.join(inspect.stack()[2][3].split('_')[slice(4,-1)])
	if DEBUG: plpy.info(f'write_info_log {source}, msg {msg}')
	plan = plpy.prepare('INSERT INTO info_log (info_text,source,data) VALUES ($1,$2,$3)', ['text','text','text'])
	res = plpy.execute(plan, [msg,source,data])
	plan = plpy.prepare('SELECT send2opc ($1,6001)', ['jsonb'])
	opc_msg=dict()
	opc_msg['info']=source+' '+msg
	opc_msg['database']=plpy.execute('select current_database()')[0]['current_database']
	#opc_msg['args']=json.loads(data)
	#opc_msg['schema']=plpy.execute('select current_schema')[0]['current_schema']
	if DEBUG: plpy.info(f'opc_msg {opc_msg}')
	#res = plpy.execute(plan, [json.dumps(opc_msg, ensure_ascii=False)])
	return dict(result='OK')
except Exception as e:
	if DEBUG: plpy.info(f'error in write_info_log {str(traceback.format_exc().splitlines()[1].split(", ")[1])} {str(e)}')
	return dict(error=str(e),source=inspect.stack()[0][3])
$BODY$
  LANGUAGE plpython3u VOLATILE
  COST 100;
COMMENT ON FUNCTION write_info_log IS 'Лог событий и т.п.';	
END$$;

/*
SELECT write_info_log('exception 1','{"function":"get_sheets","data":{"number":756945}}');
*/


--Удалить заказ, все его задания и детали
DO $$ --order_remove()
BEGIN
	DROP FUNCTION IF EXISTS order_remove;
	CREATE OR REPLACE FUNCTION order_remove ( number_order int8 ) 
		RETURNS jsonb AS $BODY$ 
	DECLARE
		err_context TEXT;
		id_order int8;
	BEGIN
		SELECT id FROM orders WHERE order_number = number_order INTO id_order;
		DELETE FROM robot_tasks rt USING parts p WHERE rt.part_id=p.id AND p.order_id=id_order;
		DELETE FROM parts WHERE order_id=id_order;
		DELETE FROM orders WHERE id=id_order;
		PERFORM write_info_log('order_remove',id_order::text);
		RETURN jsonb_build_object ( 'result', 'OK','order_id', id_order);
		EXCEPTION 
			WHEN OTHERS THEN GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
			RAISE INFO '%', SQLERRM;
			PERFORM write_error_log ( SQLERRM, id_order :: TEXT, err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
	END;
$BODY$
LANGUAGE plpgsql VOLATILE;
END $$;

/*

SELECT order_remove(1234567);
--
*/

--Добавить деталь
DO $$ --part_add()
BEGIN
	DROP FUNCTION IF EXISTS part_add;
	CREATE OR REPLACE FUNCTION part_add ( json_data jsonb ) 
		RETURNS jsonb AS $BODY$ 
	DECLARE
		err_context TEXT;
		part record;
		id_order int8;
		id_part int8;		
		jpart_id jsonb;
		jrobot_task_ids jsonb;
		jresult jsonb;
	BEGIN
		IF json_data ? 'order_number' THEN
			SELECT order_add(json_data) || json_data INTO json_data;
		END IF;
		INSERT INTO parts (id, external_id, part_number, order_id, order_position, part_length_x, part_length_y, part_thickness_z, label_pos_x, label_pos_y, label_angle_a) 
		SELECT  
		--(RPAD(date_part('year', now())::text,1) || LPAD(json_data->>'order_number', 5, '0') || LPAD(order_position::text, 4, '0') || LPAD(json_data->>'part_counter', 3, '0'))::int8 AS id , 
		x.*
					FROM	jsonb_to_record ( json_data ) AS x (
																												id int8, 
																												external_id VARCHAR (38), 
																												part_number int8, 
																												order_id int8, 
																												order_position int2,
																												part_length_x real,
																												part_length_y real,
																												part_thickness_z int2,
																												label_pos_x int2,
																												label_pos_y int2,
																												label_angle_a int2
					 ) 
					 --ON CONFLICT (part_number) do update set id=x.id
					 RETURNING id INTO id_part;
	  --TODO on error UPDATE
		--RAISE INFO 'jpart_id %',jpart_id;
		--RAISE INFO 'robot_task %',json_data->'robot_task' || jpart_id;
		--FOREACH r IN ('A','B')
		--LOOP
		--END LOOP;
		INSERT INTO robot_tasks (part_id, robot_id, operation_type, operation_number, operation_side, gripper_id, operation_content) 
		SELECT id_part
					, x.*
					, 'A' AS operation_side
					, (SELECT id FROM grippers g WHERE g.robot_id=x.robot_id AND g.operation_type=x.operation_type ) AS gripper_id 
					--, jsonb_build_object('program_fullpath',lpad((json_data->>'order_number') ||'/'|| (robot_task->'operation_content'->>'program_A'), 16)) AS operation_content
					, jsonb_build_object('program_fullpath',lpad((robot_task->'operation_content'->>'program_A'), 16)) AS operation_content
						FROM	jsonb_array_elements(json_data->'robot_tasks') rt(robot_task)
								 ,jsonb_to_record (robot_task) AS x ( 
																												robot_id	int8,
																												operation_type	type_task_type,
																												operation_number	int4)
					 WHERE robot_task->'operation_content'->>'program_A'!=''
					 RETURNING COALESCE(jrobot_task_ids,'[]') || to_jsonb(id) INTO jrobot_task_ids;

		INSERT INTO robot_tasks (part_id, robot_id, operation_type, operation_number, operation_side, gripper_id, operation_content) 
		SELECT id_part
					, x.*
					, 'B' AS operation_side
					, (SELECT id FROM grippers g WHERE g.robot_id=x.robot_id AND g.operation_type=x.operation_type ) AS gripper_id 
					--, jsonb_build_object('program_fullpath',lpad((json_data->>'order_number') ||'/'|| (robot_task->'operation_content'->>'program_B'), 16)) AS operation_content
					, jsonb_build_object('program_fullpath',lpad((robot_task->'operation_content'->>'program_B'), 16)) AS operation_content
						FROM	jsonb_array_elements(json_data->'robot_tasks') rt(robot_task)
								 ,jsonb_to_record (robot_task) AS x ( 
																												robot_id	int8,
																												operation_type	type_task_type,
																												operation_number	int4)
					 WHERE robot_task->'operation_content'->>'program_B'!=''
					 RETURNING COALESCE(jrobot_task_ids,'[]') || to_jsonb(id) INTO jrobot_task_ids;
		jresult:=jsonb_build_object ('result','OK','part_id',id_part,'robot_task_ids',jrobot_task_ids);
		PERFORM write_info_log(jresult::text,json_data::text);
		RETURN jresult;
		EXCEPTION 
		WHEN OTHERS THEN
			GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
		RAISE INFO '%',
		SQLERRM;
		PERFORM write_error_log ( SQLERRM, json_data :: TEXT, err_context );
		RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
		END;
$BODY$
LANGUAGE plpgsql VOLATILE;
END $$;

/*

SELECT part_add('{"order_number": 14599, "label_angle_a": 0, "id": 214599000209001, "part_number": 145990001, "part_counter": 1, "order_position": 1, "part_length_x": 400, "part_length_y": 600, "part_thickness_z": 16, "robot_tasks":[{"robot_id": 1, "operation_type": "machining", "operation_number": 1, "operation_content": {"program_A": "kwadrat16", "program_B": "kwadrat16"}}]}');

*/


--Добавить задание
DO $$ --task_add()
BEGIN
	DROP FUNCTION IF EXISTS task_add;
	CREATE OR REPLACE FUNCTION task_add ( json_data jsonb ) 
		RETURNS jsonb AS $BODY$ 
			DECLARE
		err_context TEXT;
		jrobot_task_id jsonb;
	BEGIN
		SELECT jsonb_build_object ('robot_task_id', MAX(id)) FROM robot_tasks rt WHERE task_status='not_sended' AND to_jsonb(rt.*) @> json_data INTO jrobot_task_id;
		IF (jrobot_task_id->>'robot_task_id') IS NULL THEN
				INSERT INTO robot_tasks (part_id, robot_id, operation_type, operation_number, operation_content, operation_side, gripper_id , part_number) 
				SELECT x.*
							,COALESCE(
										(json_data->'gripper_id')::int8,
										(SELECT id FROM grippers g WHERE g.robot_id=x.robot_id AND g.operation_type::text=COALESCE(SUBSTRING(x.operation_type::text FROM '#"%%#"#_%' FOR '#' ), x.operation_type::text)  LIMIT 1),
										1) AS gripper_id
							,COALESCE(
										(json_data->'part_number')::int4,1) AS part_number
							--,COALESCE((json_data->'part_number')::int4,1)
								FROM jsonb_to_record (json_data) AS x (   
																														part_id	int8,
																														robot_id int8,
																														operation_type type_task_type,
																														operation_number int4,
																														operation_content	jsonb,
																														operation_side	char)
							 RETURNING jsonb_build_object ('robot_task_id', id) INTO jrobot_task_id;
		END IF;
		PERFORM write_info_log((COALESCE ( jrobot_task_id, '""' ))::text,json_data::text);
		RETURN jsonb_build_object ( 'result', 'OK') || COALESCE ( jrobot_task_id, '{}' ) ;

		EXCEPTION 
			WHEN OTHERS THEN GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
			RAISE INFO '%', SQLERRM;
			PERFORM write_error_log ( SQLERRM, json_data :: TEXT, err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
	END;
	$BODY$ LANGUAGE plpgsql VOLATILE;
END $$;

/*

*/

--Представление с ошибками
DO $$ --view_error_log
BEGIN
	CREATE OR REPLACE VIEW view_error_log AS
		SELECT * FROM error_log ORDER BY id DESC LIMIT 20;
	COMMENT ON VIEW view_error_log IS 'Последние 20 ошибок';
END $$;

/*
SELECT * FROM view_error_log;
*/

--Представление с событиями
DO $$ --view_info_log
BEGIN
	CREATE OR REPLACE VIEW view_info_log AS
		SELECT * FROM info_log ORDER BY id DESC LIMIT 20;
	COMMENT ON VIEW view_info_log IS 'Последние 20 событий';
END $$;

/*
SELECT * FROM view_info_log;
*/

--Представление с активными блокировками
DO $$ --view_active_locks
BEGIN
	CREATE OR REPLACE VIEW view_active_locks AS
		 SELECT t.schemaname,
				t.relname,
				l.locktype,
				l.page,
				l.virtualtransaction,
				l.pid,
				l.mode,
				l.granted,
				pg_backend_pid() AS backend_pid,
				a.client_addr,
				a.datname
			 FROM pg_locks l
				 JOIN pg_stat_all_tables t ON l.relation = t.relid
				 JOIN pg_stat_activity a ON l.pid = a.pid
			WHERE t.schemaname <> 'pg_toast'::name AND t.schemaname <> 'pg_catalog'::name
		ORDER BY t.schemaname, t.relname;
	COMMENT ON VIEW view_active_locks IS 'Активные блокировки';
END $$;

/*
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid();
SELECT * FROM view_active_locks;
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE client_addr='127.0.0.1';
SELECT * FROM view_active_locks;

SELECT pg_terminate_backend(1271071);

SELECT * FROM pg_stat_activity WHERE 
pid <> pg_backend_pid() 
AND state = 'active' 
--AND client_addr='192.168.202.77' 
AND datname='mtk_production_db' ;

SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND client_addr='127.0.0.1' AND datname='moscow_test_db';
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND client_addr='192.168.202.77' AND datname='moscow_test_db';
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE client_addr='192.168.204.25';
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid = 1718592;

*/

--Добавить заказ
DO $$ --order_add()
BEGIN
	DROP FUNCTION IF EXISTS order_add;
	CREATE OR REPLACE FUNCTION order_add (json_data jsonb) 
	RETURNS jsonb AS $BODY$ 
		INSERT INTO orders ( order_number, order_folder ) 
				SELECT *,x.order_number::text FROM jsonb_to_record ( json_data ) 
									AS x ( order_number int8 ) 
		ON CONFLICT (order_number,year) DO UPDATE SET order_number=orders.order_number
		RETURNING jsonb_build_object ('order_id', id);
	$BODY$ LANGUAGE SQL VOLATILE;
END $$;
/*
SELECT order_add('{"order_number":1234567890}');
*/

--Изменение входных данных детали перед записью в БД
DO $$ --trigger_parts()
BEGIN
	CREATE OR REPLACE FUNCTION trigger_parts () 
	RETURNS trigger AS $BODY$ 
	DECLARE
		label_offset_x real;
		label_offset_y real;
		label_offset_a real;
	BEGIN	
		label_offset_x := -16;
		label_offset_y := -15;
		label_offset_a := -2.0;
		IF is_debug ( ) THEN
				RAISE NOTICE'Start trigger parts % %',TG_WHEN,TG_OP;
		END IF;
		IF TG_WHEN = 'BEFORE' AND TG_OP = 'INSERT' THEN
			IF NEW.label_pos_x IS NULL OR NEW.label_pos_y IS NULL THEN
					NEW.label_pos_x := NEW.part_length_x/2 + label_offset_x;
					NEW.label_pos_y := NEW.part_length_y/2 + label_offset_y;
					NEW.label_angle_a := NEW.label_angle_a + label_offset_a;
			END IF;
		END IF;
		IF TG_WHEN = 'BEFORE' AND TG_OP = 'UPDATE' THEN
				NEW.change_time := now();
		END IF;
		IF is_debug ( ) THEN
				RAISE NOTICE 'End   trigger parts % %',TG_WHEN,TG_OP;
		END IF;
		RETURN NEW;
	END;
	$BODY$ LANGUAGE plpgsql VOLATILE;
END $$;

--Актуализация времени изменения задания при обновлении его статуса 
DO $$ --trigger_robot_tasks()
BEGIN
	CREATE OR REPLACE FUNCTION trigger_robot_tasks ( ) 
	RETURNS trigger AS $BODY$ 
	BEGIN
		IF is_debug ( ) THEN
				RAISE NOTICE'Start trigger robot_tasks % %',TG_WHEN,TG_OP;
		END IF;
		IF TG_WHEN = 'BEFORE' AND TG_OP = 'UPDATE' THEN
				NEW.change_time := now();
		END IF;
		IF is_debug ( ) THEN
				RAISE NOTICE 'End   trigger robot_tasks % %',TG_WHEN,TG_OP;
		END IF;
		RETURN NEW;
	END;
	$BODY$ LANGUAGE plpgsql VOLATILE;
END $$;

--Представление по статусам заданий
DO $$ --view_task_statuses
BEGIN
	--DROP VIEW IF EXISTS view_task_statuses;
	CREATE OR REPLACE VIEW view_task_statuses AS						
									SELECT   
									e.enumlabel AS task_status
									, CASE e.enumlabel
									WHEN 'not_sended' THEN 'Не отправлено'
									WHEN 'sended' THEN 'Отправлено'
									WHEN 'received' THEN 'Получено PLC'
									WHEN 'in_process' THEN 'В процессе'
									WHEN 'on_trajectory' THEN 'В процессе (робот)'
									WHEN 'new_trajectory' THEN 'Выбор новой траектории (робот)'
									WHEN 'done' THEN 'Закончено'
									ELSE 'Неизвестное состояние'
							END	AS status_alias
              FROM    pg_type t JOIN 
                      pg_enum e ON t.oid = e.enumtypid JOIN 
                      pg_catalog.pg_namespace n ON n.oid = t.typnamespace
              WHERE   t.typname = 'type_task_status';

	COMMENT ON VIEW view_task_statuses IS 'Статусы заданий';
END $$;

/*
SELECT * FROM view_task_statuses;
*/

--Представление обо всех деталях
DO $$ --view_order_parts
BEGIN
	--DROP VIEW IF EXISTS view_order_parts;
	CREATE OR REPLACE VIEW view_order_parts AS
		SELECT 
						o.order_number AS "Номер заказа"
						, p.id AS "ID детали"
						, p.part_number AS "Номер детали"
						, CASE p.part_status 
									WHEN 'ordered' THEN 'Заказана'
									WHEN 'scanning' THEN 'Сканируется'
									WHEN 'measuring' THEN 'Измеряется'
									WHEN 'pallet_in' THEN 'На входном поддоне'
									WHEN 'pallet_buf' THEN 'На буферном поддоне'
									WHEN 'pallet_out' THEN 'На выходном поддоне'
									WHEN 'gripper' THEN 'На захвате робота'
									WHEN 'in_machine' THEN 'В машине'
									WHEN 'grabbing' THEN 'Захватывается'
									WHEN 'dropping' THEN 'Сбрасывается'
									WHEN 'done' THEN 'Готова'
									ELSE 'Неизвестное состояние'
							END	AS "Состояние детали"
						, p.part_length_x AS "Высота, X"
						, p.part_length_y AS "Ширина, Y"
						, p.part_thickness_z AS "Толщина Z"
						, p.label_pos_x AS "Этикетка X"
						, p.label_pos_y AS "Этикетка Y"
						, p.label_angle_a AS "Этикетка A"
						, p.part_pos_x AS "X"
						, p.part_pos_y AS "Y"
						, p.part_pos_z AS "Z"
						, p.part_angle_a AS "Угол поворота"
						, p.part_side AS "Сторона"
						, p.lay_number AS "Слой"
						, rt.operation_content->>'program_fullpath' AS "Программа"

			FROM parts p
				JOIN orders o ON o.id=p.order_id
				JOIN robot_tasks rt ON rt.part_id=p.id
		ORDER BY o.id DESC, p.id ;
	COMMENT ON VIEW view_order_parts IS 'Детали заказов';
END $$;

--Представление по заказам
DO $$ --view_orders
BEGIN
	DROP VIEW IF EXISTS view_orders;
	CREATE OR REPLACE VIEW view_orders AS
		SELECT order_number
			FROM orders
		ORDER BY id DESC ;
	COMMENT ON VIEW view_orders IS 'Заказы';
END $$;

--Представление по статусам деталей
DO $$ --view_part_statuses
BEGIN
	--DROP VIEW IF EXISTS view_part_statuses CASCADE ;
	CREATE OR REPLACE VIEW view_part_statuses AS						
									SELECT   
									e.enumlabel AS part_status
									, CASE e.enumlabel
									WHEN 'ordered' THEN 'Заказана'
									WHEN 'scanned' THEN 'Отсканирована'
									WHEN 'measuring' THEN 'Измеряется'
									WHEN 'pallet_in' THEN 'На входном поддоне'
									WHEN 'pallet_buf' THEN 'На буферном поддоне'
									WHEN 'pallet_out' THEN 'На выходном поддоне'
									WHEN 'gripper' THEN 'На захвате робота'
									WHEN 'in_machine' THEN 'В машине'
									WHEN 'processing' THEN 'Обрабатывается'
									WHEN 'loading' THEN 'Загружается'
									WHEN 'unloading' THEN 'Выгружается'
									WHEN 'flipping' THEN 'Переворачивается'
									WHEN 'grabbing' THEN 'Захватывается'
									WHEN 'dropping' THEN 'Сбрасывается'
									WHEN 'part_slot' THEN 'На буферном поддоне'
									WHEN 'flip_table' THEN 'На переворотном столе'
									WHEN 'done' THEN 'Готова'
									ELSE 'Неизвестное состояние'
							END	AS status_alias
              FROM    pg_type t JOIN 
                      pg_enum e ON t.oid = e.enumtypid JOIN 
                      pg_catalog.pg_namespace n ON n.oid = t.typnamespace
              WHERE   t.typname = 'type_part_status';

	COMMENT ON VIEW view_part_statuses IS 'Статусы деталей';
END $$;

/*
SELECT * FROM view_part_statuses;
*/

--Представление по статусам заданий
DO $$ --view_operation_types
BEGIN
	--DROP VIEW IF EXISTS view_operation_types;
	CREATE OR REPLACE VIEW view_operation_types AS						
									SELECT   
									e.enumlabel AS operation_type
									, CASE e.enumlabel
									WHEN 'machining' THEN 'Механическая обработка'
									WHEN 'measuring_height' THEN 'Измерение слоя детали'
									WHEN 'transfer_in2slot' THEN 'Перемещение готовой детали в слот'
									WHEN 'transfer_in2out' THEN 'Перемещение готовой детали'
									WHEN 'transfer_slot2out' THEN 'Перемещение готовой детали'
									WHEN 'transfer_in2flip' THEN 'Перемещение на стол переворота'
									WHEN 'scanning' THEN 'Сканирование'
									WHEN 'go_home' THEN 'Возврат в дом'
									ELSE 'Неизвестный тип операции'
							END	AS operation_alias
              FROM    pg_type t JOIN 
                      pg_enum e ON t.oid = e.enumtypid JOIN 
                      pg_catalog.pg_namespace n ON n.oid = t.typnamespace
              WHERE   t.typname = 'type_task_type'
							
							UNION ALL VALUES(NULL,'');

	COMMENT ON VIEW view_operation_types IS 'Типы операций';
END $$;

/*
SELECT * FROM view_operation_types;
*/


--Представление по заданиям роботов
DO $$ --view_robot_tasks
BEGIN
	--DROP VIEW IF EXISTS view_task_statuses CASCADE;
	CREATE OR REPLACE VIEW view_robot_tasks AS
		SELECT 
						  vot.operation_alias AS "Тип операции"
						, vts.status_alias AS "Статус задания"
						, rt.*
			FROM robot_tasks rt
			JOIN view_operation_types vot ON vot.operation_type=rt.operation_type::text
			JOIN view_task_statuses vts ON vts.task_status=rt.task_status::text
			ORDER BY rt.change_time DESC, rt.creation_time DESC;
	COMMENT ON VIEW view_robot_tasks IS 'Детали заданий';
END $$;

/*
SELECT * FROM view_robot_tasks LIMIT 10;
*/

--Представление по деталям
DO $$ --view_last_part
BEGIN
	--DROP VIEW IF EXISTS view_last_part;
	CREATE OR REPLACE VIEW view_last_part AS
		SELECT 
						  vot.operation_alias AS "Последняя операция"
						, vps.status_alias AS "Статус детали"
						--, p.*
						, p.id AS "ID детали"
						, p.part_number AS "Номер детали"
						, p.part_length_x AS "Высота, X"
						, p.part_length_y AS "Ширина, Y"
						, p.part_thickness_z AS "Толщина Z"
						, p.label_pos_x AS "Этикетка X"
						, p.label_pos_y AS "Этикетка Y"
						, p.label_angle_a AS "Этикетка A"
						, p.part_pos_x AS "X"
						, p.part_pos_y AS "Y"
						, p.part_pos_z AS "Z"
						, p.part_angle_a AS "Угол поворота"
						, p.part_side AS "Сторона"
						, p.lay_number AS "Слой"
			FROM parts p
			JOIN view_operation_types vot ON vot.operation_type=p.last_operation::text OR (vot.operation_type IS NULL AND p.last_operation IS NULL)
			JOIN view_part_statuses vps ON vps.part_status=p.part_status::text
			ORDER BY p.change_time DESC;
	COMMENT ON VIEW view_last_part IS 'Детали заданий';
END $$;

/*
SELECT * FROM view_last_part LIMIT 10;
*/

--Представления по PLC
DO $$ --view_plcs
BEGIN
	DROP VIEW IF EXISTS view_plcs;
	CREATE OR REPLACE VIEW view_plcs AS
		SELECT id, name, ip, sector_id, operation_type, plc_status
			FROM plcs
		ORDER BY id DESC ;
	COMMENT ON VIEW view_plcs IS 'Машины (контроллеры)';
END $$;


--Считать настройку
DO $$ --setting_get()
BEGIN
	DROP FUNCTION IF EXISTS setting_get;
	CREATE OR REPLACE FUNCTION setting_get (setting_name text)
	RETURNS jsonb AS $BODY$ 
		SELECT value FROM settings WHERE name = setting_name;
	$BODY$ LANGUAGE SQL IMMUTABLE;
END $$;
/*
SELECT setting_get('pallet_length_x');
*/

--Записать настройку
DO $$ --setting_set()
BEGIN
	DROP FUNCTION IF EXISTS setting_set;
	CREATE OR REPLACE FUNCTION setting_set (setting_name text, setting_value jsonb) 
	RETURNS jsonb AS $BODY$
		UPDATE settings SET value=setting_value WHERE name = setting_name
		RETURNING value;
	$BODY$ LANGUAGE SQL VOLATILE;
END $$;

/*
SELECT setting_set('pallet_length_x',to_jsonb(800));
*/

--Недоделанная процедура калибровки сканнера
DO $$ --scanning_calib_tool()
BEGIN
	DROP FUNCTION IF EXISTS scanning_calib_tool;
	CREATE OR REPLACE FUNCTION scanning_calib_tool (id_part_1 int8, id_part_2 int8) 
	RETURNS jsonb AS $BODY$
		SELECT jsonb_build_object('B', cot((p1.part_pos_x-p2.part_pos_x)/1000), 
															'C', cot((p1.part_pos_y-p2.part_pos_y)/1000)) 
		FROM parts p1
				CROSS JOIN parts p2 
				WHERE p1.id=id_part_1 AND p2.id=id_part_2
		
		;
	$BODY$ LANGUAGE SQL VOLATILE;
END $$;

/*
SELECT scanning_calib_tool(151,152);
*/

--Проверка, является ли значение одним из допустимых значений для перечисления
DO $$ --is_enum_ok()
BEGIN
	DROP FUNCTION IF EXISTS is_enum_ok CASCADE ;
	CREATE OR REPLACE FUNCTION is_enum_ok (label text,type_name text)
	RETURNS BOOL AS $BODY$ 
			SELECT COUNT(*)=1
							FROM    pg_type t JOIN
											pg_enum e ON t.oid = e.enumtypid JOIN 
											pg_catalog.pg_namespace n ON n.oid = t.typnamespace
							WHERE   e.enumlabel = label AND t.typname = type_name;
	$BODY$ LANGUAGE SQL IMMUTABLE;
END $$;

/*
SELECT is_enum_ok('part_slot','type_part_status');
SELECT is_enum_ok('part_slot','type_part_location');
*/

--EXECUTE OPC.sql