/*
--UPDATE parts SET part_status='part_slot', lay_number=-1 WHERE part_status='pallet_out';
UPDATE parts SET part_status='ordered', part_side='A', part_slot=0, lay_number=-1
								,part_pos_x=0, part_pos_y=0, part_pos_z=0, part_angle_a=0
								,slot_pos_x=0, slot_pos_y=0, slot_pos_z=0, slot_angle_a=0
								,out_pos_x=0, out_pos_y=0, out_pos_z=0, out_pos_a=0 WHERE id>0;
DELETE FROM robot_tasks WHERE operation_type::text LIKE 'transfer%' OR operation_type::text='scanning' OR operation_type::text='go_home' OR operation_type::text='measuring_height';
UPDATE robot_tasks SET task_status='not_sended', part_number=0 , operation_number=1, gripper_id=4;
*/

/*
UPDATE "public"."robots" SET "name" = 'Gripper', "plc_id" = 2, "pallet_in_id" = 0, "pallet_out_id" = 0, "slot_count" = 0, "robot_number" = 1, "operation_list" = '[]' WHERE "id" = 2;
UPDATE "public"."robots" SET "name" = 'Feeder', "plc_id" = 1, "pallet_in_id" = 0, "pallet_out_id" = 0, "slot_count" = 1, "robot_number" = 1, "operation_list" = '["scanning", "transfer_in2out", "machining", "transfer_in2slot", "transfer_slot2out", "transfer_in2flip", "measuring_height"]' WHERE "id" = 1;
UPDATE "public"."robots" SET "name" = 'Receiver', "plc_id" = 3, "pallet_in_id" = 0, "pallet_out_id" = 0, "slot_count" = 1, "robot_number" = 1, "operation_list" = '["scanning", "transfer_in2slot", "transfer_slot2out", "measuring_height"]' WHERE "id" = 3;
*/

--Представление со всеми актуальными заданиями для роботов
DO $$ --view_next_tasks
BEGIN
  DROP VIEW IF EXISTS view_next_tasks;
	CREATE OR REPLACE VIEW view_next_tasks AS
		SELECT rtp.id, rtp.part_id, rtp.robot_id, rtp.task_status, rtp.operation_type, rtp.operation_number, rtp.operation_content
					, rtp.operation_side, rtp.robot_number, rtp.robots_count, rtp.robot_name, rtp.plc_name, rtp.ip AS plc_ip, rtp.plc_id, rtp.next_part_number, rtp.part_number, rtp.operations_count
					, to_jsonb(pr.*)-'id'-'external_id'-'label_pos_x'-'label_pos_y'-'label_angle_a'
													-'last_operation'-'robot_id' || jsonb_build_object('part_id',pr.id) AS part
					,to_jsonb(g.*)-'id'-'robot_id'-'operation_type'-'length_x'-'length_y'-'deadzone_x'-'deadzone_y'
					--|| jsonb_build_object('part_length_x0',g.length_x+2*g.deadzone_x)
					--|| jsonb_build_object('part_length_y0',g.length_y+2*g.deadzone_y)
					|| jsonb_build_object('part_length_x0', CASE WHEN pr.part_length_x>1200 and pr.part_length_x<1800 and pr.part_length_y>500 and g.operation_type::text='machining' 
																											 THEN pr.part_length_x+((1800-pr.part_length_x)/2)::int4
																											 ELSE g.length_x+2*g.deadzone_x
																									END)
					|| jsonb_build_object('part_length_y0', CASE WHEN pr.part_length_x>800 and pr.part_length_y>500 and g.operation_type::text='machining' 
																											 THEN pr.part_length_y + 250
																											 ELSE g.length_y+2*g.deadzone_y
																									END)
					|| jsonb_build_object('gripper_within_part',g.length_x<pr.part_length_x)
					AS gripper
					, CASE
								WHEN (is_enum_ok(pr.part_status::text, 'type_part_location'))::bool THEN pr.part_status::text
								WHEN pr.part_status='scanned' THEN 'pallet_in'
								ELSE 'undefined'
						END AS part_source
					, CASE rtp.operation_type 
								WHEN 'transfer_in2flip' THEN 'flip_table'
								WHEN 'transfer_in2slot' THEN 'part_slot'
								WHEN 'measuring_height' THEN 'pallet_in'
								--WHEN 'scanning' THEN 'pallet_in'
								WHEN 'machining' THEN
																		CASE
																				WHEN rtp.last_side AND rtp.operations_count>1 THEN 'flip_table'
																				WHEN pr.part_slot>0 AND rtp.operations_count=1 THEN 'part_slot'
																				ELSE 'pallet_out'
																		END
								ELSE 'pallet_out'
						END AS part_destination
			FROM (SELECT rt.*, r.name as robot_name, p.name as plc_name, p.ip ,p.id AS plc_id
									--, MIN(rt.operation_number) FILTER(WHERE rt.task_status!='done') OVER(PARTITION BY rt.part_id) AS current_operation_number
									, MIN(rt.part_number) FILTER(WHERE (task_status!='done')) OVER() AS next_part_number
									, COUNT(*) FILTER(WHERE rt.task_status!='done') OVER(PARTITION BY rt.part_id) AS operations_count
									, (COUNT(*) FILTER(WHERE rt.task_status!='done') OVER(PARTITION BY rt.part_id,rt.operation_side))=1 AS last_side
									, r.robot_number
									, p.robots_count
							FROM robot_tasks rt
							--TODO gen all names by robot operation_type_list
							JOIN robots r ON r.id=rt.robot_id
							JOIN plcs p ON p.id=r.plc_id
							JOIN parts pa ON pa.id=rt.part_id
							WHERE rt.task_status != 'done' AND (pa.part_status='pallet_in' OR pa.part_status='scanned' OR pa.part_status='flip_table' OR rt.operation_type!='machining')
										AND to_jsonb(rt.operation_type) <@ r.operation_list or r.operation_list='[]'::jsonb
							) rtp
			JOIN parts pr ON pr.id=rtp.part_id
			JOIN grippers g ON g.id=rtp.gripper_id
		--WHERE operation_number=current_operation_number
		ORDER BY rtp.part_number, rtp.operation_type::text DESC, rtp.operation_side=pr.part_side DESC, rtp.id;
	COMMENT ON VIEW view_next_tasks IS 'Следующие задачи для роботов';
END $$;

/*
SELECT * FROM view_next_tasks
*/

--Вернуть из представления всех заданий задания по данной детали и роботу, либо по данному роботу
DO $$ --get_robot_tasks
BEGIN
	DROP FUNCTION IF EXISTS get_robot_tasks;
	CREATE OR REPLACE FUNCTION get_robot_tasks (id_part int8, id_robot int8) 
	RETURNS SETOF jsonb AS $BODY$
	DECLARE
		task jsonb;
		jslot jsonb;
		err_context text;
	BEGIN
		FOR task IN (SELECT
									(jsonb_build_object (
										'not_sended2bhx',
										COALESCE(vnt.operation_content->>'program_fullpath','')!='',
										--NOT vnt.operation_content->>'program_fullpath' IN (NULL,''),
										--vnt.task_status IN ('not_sended') AND NOT (vnt.operation_content->>'program_fullpath' IS NULL),
										'robot_task_id',
										vnt.id,
										'robot_name',
										vnt.robot_name,
										'robot_number',
										vnt.robot_number,
										'robots_count',
										vnt.robots_count,
										'robot_id',
										vnt.robot_id,
										'plc_ip',
										vnt.plc_ip,
										'plc_name',
										vnt.plc_name,
										'task_status',
										vnt.task_status,
										'operation_type',
										vnt.operation_type,
										'operation_side',
										vnt.operation_side,
										'part_source',
										vnt.part_source,
										'part_destination',
										vnt.part_destination,
										'operation_number',
										vnt.operation_number,
										'free_slot', 1
										--,'robot_trajectory', ''
										--(SELECT CASE WHEN (setting_get('pushpuzzle_mode'))::bool AND (MAX(part_slot)+1)>10 
										--						 THEN 0 
										--						 ELSE (MAX(part_slot)+1) END FROM parts)
										,'w/o_machine_mode',
										(setting_get('w/o_machine_mode'))::bool
										--vnt.operation_type IN ('transfer_in2out','transfer_in2slot','transfer_slot2out')
										) 
										|| vnt.operation_content 
										|| vnt.part
										|| vnt.gripper
													) AS task
						FROM view_next_tasks vnt
							WHERE --vnt.part_source!=vnt.part_destination AND 
										((vnt.part_id=id_part --AND (vnt.part_id!=0 OR vnt.operation_type!='measuring_height')
															)
												OR (id_part=-1 AND (vnt.part->>'part_status'!='scanned' OR vnt.operation_type='measuring_height') AND vnt.part->>'part_status'!='ordered')
												--OR (vnt.part_id=0 AND vnt.operation_type='measuring_height')
												)
										AND vnt.robot_id = id_robot
							ORDER BY --vnt.robot_id = id_robot DESC, 
											 vnt.part_number ,  vnt.operation_type DESC
												,vnt.operation_side=vnt.part->>'part_side' DESC  -- transfer first
												/* plc_id=id_plc DESC,*/
						) LOOP
				
				--task:=task||jsonb_build_object('all_out',id_part=-1);
				
				IF NOT (setting_get('pushpuzzle_mode'))::bool THEN
					jslot:=jsonb_build_object('slot_pos_x', task->'part_pos_x', 'slot_pos_y', task->'part_pos_y', 
																		'slot_pos_z', task->'part_pos_z', 
																		'slot_angle_a', task->'part_angle_a');
				task:=task||jslot;
				/*
				ELSE
					jslot:=get_slot(task);
					IF jslot ? 'error' THEN
							RAISE EXCEPTION 'error "%" source "%" task "%"', jslot->>'error' , jslot->>'source' , task; 
					END IF;
					task:=task||jslot;
				*/
				END IF;
				
				--SELECT task || jsonb_build_object('robot_id',id) FROM robots WHERE plc_id=(task->'plc_id')::int8 INTO task;
				RETURN NEXT task;
		END LOOP;
		EXCEPTION WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
			err_context := SUBSTRING ( err_context FROM '%function #"%%#" line%' FOR '#' ) || SUBSTRING ( err_context FROM '%[)]#" line ___#"%' FOR '#' );
			RAISE INFO '%', SQLERRM;
			PERFORM write_error_log ( SQLERRM, '', err_context );
			RETURN NEXT jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
	END;
	$BODY$ LANGUAGE plpgsql IMMUTABLE;
	COMMENT ON FUNCTION get_robot_tasks IS 'Задания для роботов';
END $$;

/*
SELECT get_robot_tasks();
SELECT get_robot_tasks(-1,3);
SELECT get_robot_tasks(3);
SELECT get_robot_tasks(1,1);
SELECT get_robot_tasks(3,1);
SELECT get_robot_tasks(146,1);
SELECT get_robot_tasks(152,1);
SELECT get_robot_tasks(158,1);
SELECT get_robot_tasks(-1,1);
*/


--Статус PLC и запущенные задания
DO $$ --get_plcs_status 
BEGIN
DROP FUNCTION IF EXISTS get_plcs_status;
CREATE OR REPLACE FUNCTION get_plcs_status()
RETURNS SETOF jsonb
AS $BODY$
DECLARE
  plc record;
  jtask jsonb;
BEGIN
	FOR plc IN (SELECT p.ip, p.name AS plc_name, jsonb_agg(r.id) OVER(PARTITION BY p.id ORDER BY r.id) AS robot_ids, r.id AS robot_id
										, COUNT(*) OVER(PARTITION BY p.id) AS robots_count
										FROM plcs p 
										JOIN robots r ON r.plc_id=p.id
										WHERE p.active) LOOP
			SELECT get_robot_tasks(-1,plc.robot_id) LIMIT 1 INTO jtask;
			IF jtask IS NULL THEN
					RETURN NEXT jsonb_build_object(      
																				 'plc_ip', plc.ip
																				,'plc_name', plc.plc_name
																				,'robots_count', plc.robots_count
																				,'robot_ids', plc.robot_ids);
			ELSE
					RETURN NEXT jtask || jsonb_build_object('robot_ids',plc.robot_ids) || jsonb_build_object('robot_pause',TRUE);
			END IF;
	END LOOP;
END;
$BODY$ LANGUAGE plpgsql IMMUTABLE;
COMMENT ON FUNCTION get_plcs_status IS 'Статусы роботов';  
END$$;

/*
SELECT get_plcs_status();
*/

--Обновить статус задания робота
DO $$ --set_robot_task_status
BEGIN
CREATE OR REPLACE FUNCTION set_robot_task_status(json_data jsonb)
  RETURNS jsonb AS $BODY$ 
    DECLARE 
      res_part jsonb;
      res_task jsonb;
    BEGIN
			--IF (NOT json_data ? 'part_side') THEN
			--	UPDATE parts SET part_side = json_data->>'part_side';
			--END IF;
      IF is_enum_ok(json_data->>'part_status','type_part_status')
			--AND is_enum_ok(json_data->>'part_destination','type_part_location') 
																																	THEN
        UPDATE parts SET part_status=(json_data->>'part_status')::type_part_status
												,part_side=COALESCE(json_data->>'part_side',part_side)
												,last_operation=COALESCE((json_data->>'operation_type')::type_task_type,last_operation)
												,part_slot=COALESCE((json_data->'part_slot')::int2,part_slot)
												,robot_id=COALESCE((json_data->'robot_id')::int8,robot_id)
												--,part_pos_x=COALESCE((json_data->'part_pos_x')::int8,part_pos_x)
												--,part_pos_y=COALESCE((json_data->'part_pos_y')::int8,part_pos_y)
												,part_pos_z=COALESCE((json_data->'part_pos_z')::int8,part_pos_z)
												--,part_angle_a=COALESCE((json_data->'part_angle_a')::int8,part_angle_a)
												,slot_pos_x=COALESCE((json_data->'slot_pos_x')::int8,slot_pos_x)
												,slot_pos_y=COALESCE((json_data->'slot_pos_y')::int8,slot_pos_y)
												,slot_pos_z=COALESCE((json_data->'slot_pos_z')::int8,slot_pos_z)
												,slot_angle_a=COALESCE((json_data->'slot_angle_a')::int8,slot_angle_a)
												--,out_pos_x=COALESCE((json_data->'out_pos_x')::int8,out_pos_x)
												--,out_pos_y=COALESCE((json_data->'out_pos_y')::int8,out_pos_y)
												--,out_pos_z=COALESCE((json_data->'out_pos_z')::int8,out_pos_z)
												--,out_pos_a=COALESCE((json_data->'out_pos_a')::int8,out_pos_a)
												--,lay_number=COALESCE((json_data->'lay_number')::int8,lay_number)
												--,part_destination=COALESCE(json_data->>'part_destination',part_destination)
												
          WHERE id=(json_data->'part_id')::int8 --AND id!=0
          RETURNING jsonb_build_object('result_part','OK') 
          INTO res_part;
					IF (json_data->>'task_status')='done' AND FALSE THEN
							IF (json_data->>'part_destination')='pallet_out' OR (json_data->>'part_destination')='pallet_out' THEN
									UPDATE parts SET part_pos_x=out_pos_x
																	,part_pos_y=out_pos_y
																	,part_pos_z=out_pos_z
																	,part_angle_a=out_pos_a
										WHERE id=(json_data->'part_id')::int8 
										RETURNING jsonb_build_object('result_part','OK') 
										INTO res_part;
							END IF;
							IF (json_data->>'part_destination')='part_slot' THEN
									UPDATE parts SET part_pos_x=slot_pos_x
																	,part_pos_y=slot_pos_y
																	,part_pos_z=slot_pos_z
																	,part_angle_a=slot_angle_a
										WHERE id=(json_data->'part_id')::int8 
										RETURNING jsonb_build_object('result_part','OK') 
										INTO res_part;
							END IF;
					END IF;
      ELSE
        res_part = jsonb_build_object('error',format('Не найден part_status <%s>', json_data->>'part_status'));
      END IF;
      IF (SELECT json_data->>'task_status' IN (SELECT   
											e.enumlabel
							FROM    pg_type t JOIN 
											pg_enum e ON t.oid = e.enumtypid JOIN 
											pg_catalog.pg_namespace n ON n.oid = t.typnamespace
							WHERE   t.typname = 'type_task_status') ) THEN
        UPDATE robot_tasks SET task_status=(json_data->>'task_status')::type_task_status, robot_id=COALESCE((SELECT id FROM robots WHERE id=(json_data->'robot_id')::int8),robot_id)
          WHERE id=(json_data->'robot_task_id')::int8 
          RETURNING jsonb_build_object('result_task','OK')
          INTO res_task;
      ELSE
					 res_task = jsonb_build_object('error',format('Не найден task_status <%s>', json_data->>'task_status'));
      END IF;
      IF res_part->>'result_part'='OK' AND res_task->>'result_task'='OK' THEN
        RETURN jsonb_build_object('result','OK');
      ELSE
        RETURN COALESCE(res_part, jsonb_build_object('error_part',format('Не найден part_id %s ', (json_data->'part_id')::int8 ))) ||
               COALESCE(res_task, jsonb_build_object('error_task',format('Не найден robot_task_id %s', (json_data->'robot_task_id')::int8 )));
      END IF;
    END;
  $BODY$
  LANGUAGE plpgsql VOLATILE;
	COMMENT ON FUNCTION set_robot_task_status IS 'Установить статус задания робота';
END$$;

/*
SELECT set_robot_task_status('{"robot_task_id": 224, "task_status": "done", "plc_id": 1, "robot_id": 0, "part_id": 144, "part_status": "gripper", "part_side": "A", "part_slot": 1}');
*/

--Статус процесса обработки деталей
DO $$ --get_process_status 
BEGIN
DROP FUNCTION IF EXISTS get_process_status;
CREATE OR REPLACE FUNCTION get_process_status(id_robot int8)
RETURNS jsonb
AS $BODY$
DECLARE
  RESULT jsonb;
BEGIN
			SELECT jsonb_build_object('slot2out',COUNT(*)!=0) FROM parts p JOIN robot_tasks rt ON p.id=rt.part_id
					WHERE part_status='part_slot' AND rt.operation_type='transfer_slot2out' AND rt.task_status!='done' AND rt.robot_id=id_robot INTO RESULT;
			SELECT RESULT || jsonb_build_object('first_lay', COALESCE((SELECT COUNT(*)!=0 FROM parts p JOIN robot_tasks rt ON p.id=rt.part_id 
																																			WHERE p.lay_number=1 and p.part_status='part_slot'
																																			HAVING BOOL_AND(rt.operation_type!='transfer_slot2out' AND rt.task_status='done' AND rt.robot_id=id_robot))
																																		,FALSE))
																																		INTO RESULT;
			SELECT RESULT || jsonb_build_object('measure_done', (SELECT COUNT(*)>0 AND BOOL_AND( rt.task_status='done')  
																																FROM parts p JOIN robot_tasks rt ON p.id=rt.part_id AND p.part_status='scanned'
																																			WHERE rt.operation_type='measuring_height' AND rt.robot_id=id_robot)
																												OR (SELECT COUNT(*)>0 AND BOOL_AND(part_pos_z!=0)  
																																FROM parts 
																																			WHERE part_status='scanned' AND robot_id=id_robot)
																					)
																																		INTO RESULT;
			SELECT RESULT || jsonb_build_object('measure_expect', (SELECT COUNT(*)>0 
																																FROM parts p JOIN robot_tasks rt ON p.id=rt.part_id AND p.part_status='scanned'
																																			WHERE rt.operation_type='measuring_height' AND rt.task_status!='done' AND rt.robot_id=id_robot)
																					)
																																		INTO RESULT;
			SELECT RESULT || jsonb_build_object('next_part_inlay',COUNT(*)!=0) FROM parts WHERE part_status IN ('pallet_in','flip_table') --,'scanned'
																																														AND id!=0 AND robot_id=id_robot INTO RESULT;
			RETURN RESULT;
END;
$BODY$ LANGUAGE plpgsql IMMUTABLE;
COMMENT ON FUNCTION get_process_status IS 'Статус процесса обработки деталей';  
END$$;

/*
SELECT get_process_status(1);
*/



--Вернуть новое задание для робота
DO $$ --gen_robot_task 
BEGIN
DROP FUNCTION IF EXISTS gen_robot_task;
CREATE OR REPLACE FUNCTION gen_robot_task(id_part int8, id_robot int8)
RETURNS jsonb
AS $BODY$
DECLARE
  process_status jsonb;
  robot_task jsonb;
  add_res jsonb;
  jpart_ids jsonb;
  jtask_ids jsonb;
	last_lay int2;
  err_context text;
  jpacking_data jsonb;
	jpacking_output jsonb;
	check_res jsonb;
	jlay_res jsonb;
	jpack_parts jsonb;
	lay_height real;
  RESULT jsonb;
BEGIN
			UPDATE robot_tasks SET task_status='done' WHERE operation_type = 'scanning' AND task_status!='done' AND robot_id=id_robot;
			UPDATE parts SET part_status='pallet_in' WHERE id=0 AND part_status!='scanned';
			SELECT get_process_status(id_robot) INTO process_status;
			IF (process_status->'measure_done')::bool THEN
						--максимальня высота без половыны толщины
						SELECT MAX(part_pos_z) FROM parts -- MAX(part_pos_z-part_thickness_z/3.0)
								WHERE part_status='scanned' AND id!=0 AND robot_id = id_robot
								INTO lay_height;
						RAISE INFO 'lay_height %',lay_height;
						--Нижний слой
						UPDATE parts SET part_status='ordered' --SELECT id, part_pos_z FROM parts
								WHERE part_status='scanned' AND id!=0 AND part_pos_z<lay_height AND robot_id = id_robot;
						--Верхний слой
						--UPDATE parts SET part_status='pallet_in'
						--		WHERE part_status='scanned' AND robot_id = id_robot;-- AND part_pos_z>=lay_height;
						--SELECT jsonb_agg(id) FROM parts WHERE part_status='pallet_in' AND robot_id = id_robot AND id!=0 INTO jpart_ids;
						SELECT jsonb_agg(id) FROM parts WHERE part_status='scanned' AND robot_id = id_robot AND id!=0 INTO jpart_ids;
						SELECT process_lay(jpart_ids, id_robot) INTO jlay_res;
						IF jlay_res ? 'error' THEN
								RAISE EXCEPTION 'In process_lay %', jlay_res->'error';
						END IF;
						
						RESULT := process_status || jlay_res;
						RAISE INFO 'jlay_res->part_ids %',jlay_res->'part_ids';
						UPDATE parts SET part_status='ordered' WHERE part_status='pallet_in' AND robot_id = id_robot AND id!=0 AND NOT (to_jsonb(id) <@ to_jsonb(jlay_res->'part_ids'));
						IF (setting_get('pushpuzzle_mode'))::bool THEN
								SELECT pack_parts(jsonb_build_object('part_source','pallet_in','part_destination','part_slot','robot_id',id_robot)) INTO jpack_parts;
								RAISE INFO 'jpack_parts %', jpack_parts;
								IF jpack_parts ? 'error' THEN
									RAISE EXCEPTION 'In pack_parts pallet_in->part_slot "%" ', jpack_parts->'error';
								END IF;
								RESULT := RESULT || jpack_parts;
						ELSE
								UPDATE parts SET part_slot = 1 WHERE lay_number = 1 AND to_jsonb(id) <@ jpart_ids;
						END IF;
			END IF;
			
			--
			RESULT:=process_status;
			IF (process_status->'next_part_inlay')::bool OR (process_status->'slot2out')::bool OR (process_status->'measure_expect')::bool OR (process_status->'measure_done')::bool THEN
					SELECT task
							FROM get_robot_tasks(-1,id_robot) t(task) 
									WHERE task->>'operation_type'='measuring_height' OR 
														NOT (process_status->'measure_expect')::bool
					UNION ALL
					SELECT task
							FROM get_robot_tasks(0,id_robot) t(task)
					UNION ALL
					SELECT task
							FROM get_robot_tasks(id_part,id_robot) t(task)
					UNION ALL
					SELECT task
							FROM get_robot_tasks(-1,id_robot) t(task) LIMIT 1 INTO robot_task;
			ELSE
				--сканирование следующего слоя во входном поддоне 
				--Предыдущая операция - не сканирование
				IF id_part!=(0) THEN
					--RAISE INFO 'task_add id_robot %', id_robot;
					UPDATE robot_tasks SET task_status='done' WHERE operation_type = 'scanning' AND task_status!='done' AND robot_id=id_robot;
					SELECT task_add(jsonb_build_object( 'part_id', 0,
																							'robot_id', id_robot,
																							'operation_type', 'scanning',
																							'operation_number', 1,
																							'operation_content', jsonb_build_object('program_fullpath',''),
																							'operation_side', ' '
																							)
														) INTO add_res;
					IF add_res ? 'error' THEN
						RAISE EXCEPTION '%', add_res->>'error';
					END IF;
					SELECT task FROM get_robot_tasks(-1,id_robot) t(task) LIMIT 1 INTO robot_task;
				ELSE
					IF (process_status->'first_lay')::bool THEN
						jtask_ids:=jsonb_build_array();
						/*
						FOR task IN 
										(SELECT DISTINCT ON (rt.part_id) rt.part_id, rt.gripper_id	 
														FROM (SELECT p.id
																			FROM parts p JOIN robot_tasks rt ON p.id=rt.part_id
																					WHERE part_status='part_slot' AND lay_number=1
																					GROUP BY p.id
																					HAVING BOOL_AND(rt.task_status='done')) prt 
																JOIN robot_tasks rt ON prt.id=rt.part_id
																GROUP BY rt.part_id, rt.part_number, rt.operation_type, rt.gripper_id
																ORDER BY rt.part_id, rt.part_number, rt.operation_type DESC
																) 
																 LOOP
								SELECT task_add(jsonb_build_object('part_id', task.part_id,
																										'robot_id', id_robot,
																										'gripper_id', task.gripper_id,
																										--'gripper_id', (SELECT id FROM grippers WHERE robot_id=id_robot AND operation_type='transfer' LIMIT 1),
																										'operation_type', 'transfer_slot2out',
																										'operation_number', 1,
																										'operation_content', jsonb_build_object(),
																										'operation_side', ' '
																										)
																)
								INTO add_res;
								IF add_res ? 'error' THEN
									RAISE EXCEPTION '%', add_res->>'error';
								END IF;
								jtask_ids:= jtask_ids || to_jsonb(add_res->'robot_task_id'); 
						END LOOP;
						--SELECT RESULT || jsonb_build_object('add_res',add_res) INTO RESULT;
						SELECT RESULT || jsonb_build_object('jtask_ids',jtask_ids) INTO RESULT;
						*/
						--UPDATE parts SET part_status='slot2out' WHERE part_status='part_slot' AND lay_number=1;
						SELECT COALESCE((SELECT jsonb_agg(DISTINCT p.id)
																			FROM parts p JOIN robot_tasks rt ON p.id=rt.part_id
																					WHERE part_status='part_slot' AND lay_number=1 AND rt.robot_id=id_robot
																					--GROUP BY p.id
																					HAVING BOOL_AND(rt.task_status='done')) ,'[]')
																					INTO jpart_ids;
						RAISE INFO 'jpart_ids %',jpart_ids;
						IF jsonb_array_length(jpart_ids)=0 THEN
							RAISE EXCEPTION 'jsonb_array_length(jpart_ids)=0, robot_id %', id_robot; 
						END IF;
						SELECT process_lay(jpart_ids, id_robot) INTO jlay_res;
						IF jlay_res ? 'error' THEN
								RAISE EXCEPTION 'In process_lay %', jlay_res->'error';
						END IF;
						RESULT := RESULT || jlay_res;
						SELECT task FROM get_robot_tasks(-1,id_robot) t(task) LIMIT 1 INTO robot_task;
					END IF;
				END IF;
			END IF;
	IF NOT robot_task IS NULL THEN
		PERFORM pg_notify('robot_task',robot_task::text);
		RESULT := RESULT || jsonb_build_object('result','OK','robot_task',robot_task);
	END IF;
	
	
	EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'gen_robot_task',	format('part_id %s, plc_id %s',id_part, id_robot));
	RETURN RESULT;
	EXCEPTION 
			WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
			err_context := SUBSTRING ( err_context FROM '%function #"%%#" line%' FOR '#' ) || SUBSTRING ( err_context FROM '%[)]#" line ___#"%' FOR '#' );
			RAISE INFO '%', SQLERRM;
			PERFORM write_error_log ( SQLERRM, format('part_id %s, plc_id %s',id_part, id_robot), err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
COMMENT ON FUNCTION gen_robot_task IS 'Генерация нового задания по детали';  
END$$;

/*
SELECT gen_robot_task(203860003600003,1);
SELECT gen_robot_task(3,1);
SELECT gen_robot_task(7,1);
SELECT gen_robot_task(-1,1);
SELECT gen_robot_task(-1,3);
SELECT gen_robot_task(0,3);
SELECT gen_robot_task(0,1);
--SELECT gen_robot_task(4,-1);
*/

--Обновиить статус PLC (недоступен, подключен...)
DO $$ --process_plc_status 
BEGIN
DROP FUNCTION IF EXISTS process_plc_status;
CREATE OR REPLACE FUNCTION process_plc_status(json_data jsonb)
RETURNS jsonb
AS $BODY$
DECLARE
  err_context text;
  update_res text;
  RESULT jsonb;
BEGIN
		update_res:='NOK';
		UPDATE plcs SET plc_status=(json_data->>'plc_status')::type_plc_status
          WHERE id=(json_data->'plc_id')::int8 OR ip=json_data->>'plc_ip'
          RETURNING 'OK'
          INTO update_res;
		RESULT := jsonb_build_object('result', update_res);
		EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'process_plc_status',	json_data);

		RETURN RESULT;
	EXCEPTION 
			WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
			RAISE INFO '%', SQLERRM;
			PERFORM write_error_log ( SQLERRM, json_data :: TEXT, err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
COMMENT ON FUNCTION process_plc_status IS 'Обновление статуса PLC';  
END$$;

/*
SELECT process_plc_status('{"plc_status":"disconnected","plc_id":1}');
--SELECT '{"plc_status":"disconnected","plc_id":1}'::jsonb;
*/

--Сбросить входной паллет робота
DO $$ --reset_robot_pallet 
BEGIN
DROP FUNCTION IF EXISTS reset_robot_pallet;
CREATE OR REPLACE FUNCTION reset_robot_pallet(json_data jsonb)
RETURNS jsonb
AS $BODY$
DECLARE
  err_context text;
  id_robot int8;
  RESULT jsonb;
BEGIN
		IF (json_data->'new_pallet')::bool THEN
				id_robot=(json_data->'robot_id')::int8;
				UPDATE parts SET part_status='ordered'
						WHERE NOT part_status IN ('pallet_out','ordered') AND id!=0 AND robot_id = id_robot;
				UPDATE robot_tasks SET task_status='not_sended'
						WHERE NOT task_status IN ('done','not_sended') AND robot_id = id_robot;
				UPDATE robot_tasks SET task_status='done'
						WHERE operation_type IN ('scanning','go_home','measuring_height') AND NOT task_status IN ('done') AND robot_id = id_robot;
						
					--UPDATE parts SET robot_id = NULL
					--			WHERE robot_id IN (SELECT id FROM robots WHERE plc_id=(json_data->'plc_id')::int8);
				RESULT := jsonb_build_object('result', 'OK');
				EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'reset_robot_pallet',	json_data);
				RETURN RESULT;
		END IF;
	EXCEPTION 
			WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
			RAISE INFO '%', SQLERRM;
			PERFORM write_error_log ( SQLERRM, json_data :: TEXT, err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
COMMENT ON FUNCTION reset_robot_pallet IS 'Обнуление поддона';  
END$$;

/*
SELECT reset_robot_pallet('{"new_pallet":true,"plc_id":1}');
*/

--Сбросить задание робота, откатить его статус
DO $$ --reset_robot_task
BEGIN
DROP FUNCTION IF EXISTS reset_robot_task;
CREATE OR REPLACE FUNCTION reset_robot_task(id_robot int8, id_robot_task int8=-1, id_part int8=-1, status_part text='')
RETURNS jsonb
AS $BODY$
DECLARE
  err_context text;
  robot_task jsonb;
  RESULT jsonb;
BEGIN

			IF id_robot_task!=(-1) THEN
					UPDATE robot_tasks SET task_status='not_sended' WHERE id=id_robot_task;
			END IF;
			IF id_part!=(-1) AND status_part<>'' AND is_enum_ok(status_part, 'type_part_status') THEN
					UPDATE parts SET part_status=status_part::type_part_status WHERE id=id_part;
			END IF;
			UPDATE parts SET part_status='pallet_in' WHERE id = 0 AND part_status!='scanned';
			
			/*
			UPDATE robot_tasks SET task_status='done' WHERE operation_type = 'go_home' AND robot_id=id_robot;
			PERFORM task_add(jsonb_build_object('part_id', 0,
																					'robot_id', id_robot,
																					'operation_type', 'go_home',
																					'operation_number', 1,
																					'part_number', 1,
																					'operation_content', jsonb_build_object(),
																					'operation_side', ''));
			--*/
			SELECT task
						FROM get_robot_tasks(0,id_robot) t(task) 
						--WHERE task->>'operation_type'='go_home'
			UNION ALL
			SELECT task FROM get_robot_tasks(-1,id_robot) t(task) LIMIT 1 INTO robot_task;
			IF NOT robot_task IS NULL THEN
					PERFORM pg_notify('robot_task',robot_task::text);
					RESULT := jsonb_build_object('result', 'OK','robot_task',robot_task);
			END IF;
			
			EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'reset_robot_task',id_robot);
			RETURN RESULT;
	EXCEPTION 
			WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
			RAISE INFO '%', SQLERRM;
			PERFORM write_error_log ( SQLERRM, id_robot :: TEXT, err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
COMMENT ON FUNCTION reset_robot_task IS 'Возврат робота в дом';  
END$$;

/*
SELECT reset_robot_task(1);
SELECT reset_robot_task(1,0,0,'');
SELECT reset_robot_task(1,0,0,'pallet_in');

*/

--Принудительно завершить задание робота
DO $$ --force_robot_task
BEGIN
DROP FUNCTION IF EXISTS force_robot_task;
CREATE OR REPLACE FUNCTION force_robot_task(id_robot_task int8) --, status_part text=''
RETURNS jsonb
AS $BODY$
DECLARE
  err_context text;
  RESULT jsonb;
BEGIN

			UPDATE robot_tasks SET task_status='done' WHERE id=id_robot_task;
			RESULT := jsonb_build_object('result', 'OK');
			
			EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'force_robot_task',id_robot);
			RETURN RESULT;
	EXCEPTION 
			WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
			RAISE INFO '%', SQLERRM;
			PERFORM write_error_log ( SQLERRM, id_robot :: TEXT, err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
COMMENT ON FUNCTION force_robot_task IS 'Принудительное завершение задания';  
END$$;

/*
SELECT force_robot_task(1);
*/

--
DO $$ --expected_robot_trajectory
BEGIN
DROP FUNCTION IF EXISTS expected_robot_trajectory;
CREATE OR REPLACE FUNCTION expected_robot_trajectory(json_data jsonb) 
RETURNS jsonb
AS $BODY$
DECLARE
  err_context text;
  robot_trajectories jsonb;
  RESULT jsonb;
BEGIN
/*
json_data:= jsonb_build_object('previous_program' ,1, 
 'machine_status' ,'ready2load', 
 'robot_status' ,'home', 
 'part_status' ,'pallet_in', 
 'part_destination' ,'pallet_out', 
 'operation_type' ,'machining', 
 'lay_number' ,2, 
 'operation_side' ,'A', 
 'part_side','A');
*/


SELECT DISTINCT jsonb_build_object('trajectory', trajectory, 'expected_tags', CASE WHEN BOOL_OR(ready_trajectory) THEN '"<trajectory is raedy>"' ELSE jsonb_agg(tag_expected->0) FILTER (WHERE NOT tag_expected->0 IS NULL) END) FROM (
								
SELECT DISTINCT rp.trajectory
								, (COUNT(*) FILTER(WHERE tag_value!=value_tag AND tag_name=name_tag) OVER w)=0 AS ready_trajectory
								, COUNT(*) FILTER(WHERE tag_value!=value_tag AND tag_name=name_tag) OVER w AS tag_count_expect
								, COALESCE(jsonb_agg(jsonb_build_object(tag_name,tag_value)) FILTER(WHERE tag_value!=value_tag AND tag_name=name_tag) OVER w,'[]') AS tag_expected
								, bool_or(tag_name='operation_type' AND tag_value=value_tag) FILTER(WHERE tag_name=name_tag) OVER w AS operation_match
		FROM robot_programs rp, jsonb_each(json_data) jd(name_tag,value_tag)
 WINDOW w AS (PARTITION BY trajectory, entry_point) 
 ORDER BY tag_count_expect
 ) tce
 WHERE tag_count_expect<2 AND operation_match
 GROUP BY trajectory
		INTO robot_trajectories;
 
			RESULT := jsonb_build_object('result', 'OK','plc_ip',json_data->'plc_ip','robot_trajectories',robot_trajectories);
			
			EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'expected_robot_trajectory',json_data::text);
			RETURN RESULT;
	EXCEPTION 
			WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
			RAISE INFO '%', SQLERRM;
			PERFORM write_error_log ( SQLERRM, json_data :: TEXT, err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
COMMENT ON FUNCTION expected_robot_trajectory IS 'Ожидаемая траектория робота';  
END$$;

/*

SELECT expected_robot_trajectory('{"previous_program":1, "machine_status":"ready2load",  "robot_status":"home",  "part_status":"pallet_in",  "part_destination":"pallet_out",  "operation_type":"machining",  "lay_number":2,  "operation_side":"A",  "part_side":"A"}');

SELECT expected_robot_trajectory('{"query": "expected_robot_trajectory", "previous_program": 7, "machine_status": "ready2unload", "robot_status": "manual_mode", "part_status": "pallet_in", "part_destination": "part_slot", "operation_type": "machining", "lay_number": 1, "operation_side": "A", "part_side": "A", "plc_ip": "192.168.29.22"}');
*/


DO $$ --process_xyacodes 
BEGIN
DROP FUNCTION IF EXISTS process_xyacodes;
CREATE OR REPLACE FUNCTION process_xyacodes(json_data jsonb)
RETURNS jsonb
AS $BODY$
DECLARE
  err_context text;
  XYA_codes jsonb;
  --jparts jsonb;
	--jgrippers jsonb;
	--jgripping_data jsonb;
	jpart_ids jsonb;
	--jgripping_order jsonb;
	jtask jsonb;
	RESULT jsonb;
	scaner_FOVx real;
	scaner_FOVy real;
	scaner_d0 real;
	scaner_BOXx real;
	scaner_BOXy real;
	scaner_alpha0_x real;
	scaner_alpha0_y real;
	scaner_scale_x real;
	scaner_scale_y real;
	jscale jsonb;
	--scan_pos_x real;
	--scan_pos_y real;
	--robot_pos record;
	id_robot int8;
	--id_plc int8;
	robot_pos_z real;
	jpack_parts jsonb;
	--part record;
	--jtaskadd_res jsonb;
	jlay_res jsonb;
BEGIN
	SELECT jsonb_path_query_array(json_data->'XYA_codes','$[*] ? (@.type()=="object")') INTO XYA_codes;
	id_robot:=(json_data->'robot_id')::int8;
	IF (jsonb_array_length(XYA_codes)>0) THEN
		--id_plc:=(json_data->'plc_id')::int8;
		scaner_BOXx:= setting_get('scaner_BOXx')::real;
		scaner_BOXy:= setting_get('scaner_BOXy')::real;
		scaner_d0:= setting_get('scaner_d0')::real;
		
	/*
		SELECT scan_robot_pos_x AS X, scan_robot_pos_y AS Y, scan_robot_pos_z AS Z, scan_robot_angle_a AS A
				FROM jsonb_to_record ( json_data ) AS x ( 
																																scan_robot_pos_x real,
																																scan_robot_pos_y real,
																																scan_robot_pos_z real,
																																scan_robot_angle_a real)
		INTO robot_pos;
	*/	
		robot_pos_z:=(XYA_codes->0->'scan_robot_pos_z')::real;
		IF robot_pos_z<1500 THEN
			scaner_alpha0_x:= 34;
			scaner_alpha0_y:= 19.537;
		ELSE
			scaner_alpha0_x:= 33.29;
			scaner_alpha0_y:= 19.082;
		END IF;
		
		
		
		scaner_FOVx:= 2*((robot_pos_z + scaner_d0) * tan(radians(scaner_alpha0_x/2.0)));
		scaner_FOVy:= 2*((robot_pos_z + scaner_d0) * tan(radians(scaner_alpha0_y/2.0)));
		--SELECT 2*((2000 + 8) * tan(radians(33.29/2.0)))/1920.0,2*((2000 + 8) * tan(radians(19.082/2.0)))/1080.0;
		--SELECT 2*((1200 + 8) * tan(radians(34/2.0))),2*((1200 + 8) * tan(radians(19.537/2.0)));
		--SELECT 2*((1000 + 8) * tan(radians(34/2.0))),2*((1000 + 8) * tan(radians(19.537/2.0)));
		--SELECT 2*((1000 + 8) * tan(radians(34/2.0)))/1920,2*((1000 + 8) * tan(radians(19.537/2.0)))/1080;
		--scaner_scale_x:=0,3302528298673238;scaner_scale_y:=	0,3213597881804298;
		--SELECT 1427::int2-1920/2.0
		--select 2*((1523) * tan(radians(33.6/2.0)))/1920*(306) ,    2*((1523 ) * tan(radians(19.337/2.0)))/1080*(303) ;

		scaner_scale_x:=scaner_FOVx/scaner_BOXx;
		scaner_scale_y:=scaner_FOVy/scaner_BOXy;
		jscale:=jsonb_build_object('scaner_scale_x',scaner_scale_x,'scaner_scale_y',scaner_scale_y);
		--scan_pos_x=-scaner_FOVx/2.0;
		--scan_pos_y=-scaner_FOVy/2.0;
		
		RAISE INFO 'xya XYA_codes %', XYA_codes;
		
		WITH lay AS (
					UPDATE parts SET part_pos_x = scan_robot_pos_x + (p.x * p.scan_cos_a - p.y * p.scan_sin_a) - (p.dx * p.cos_theta - p.dy * p.sin_theta)
													,part_pos_y = scan_robot_pos_y + (p.x * p.scan_sin_a + p.y * p.scan_cos_a) - (p.dx * p.sin_theta + p.dy * p.cos_theta)
													,part_angle_a = p.angle_a
													,part_status = 'scanned' --CASE WHEN part_status::text IN ('pallet_out') THEN 'pallet_out'::type_part_status ELSE 'scanned'::type_part_status END
													,part_side = 'A'
													,part_slot = 0
													,robot_id = id_robot
													--,operation_type=(SELECT operation_type FROM plcs WHERE id = id_plc)
													,lay_number=(json_data->'scan_lay_number')::int4
							FROM 		(SELECT DISTINCT ON (p.part_id) p.part_id
															 , (p.scan_label_pos_x-scaner_BOXx/2.0)*p.scaner_scale_x x
															 --, (p.scan_label_pos_y-scaner_BOXy/2.0)*p.scaner_scale_y y
															 , (scaner_BOXy/2.0-p.scan_label_pos_y)*p.scaner_scale_y y
															 , (pa.label_pos_x - pa.part_length_x/2.0) dx
															 , (pa.label_pos_y - pa.part_length_y/2.0) dy
															 , cos(radians(p.scan_label_angle_a-pa.label_angle_a)) cos_theta
															 , sin(radians(p.scan_label_angle_a-pa.label_angle_a)) sin_theta
															 , cos(radians(p.scan_robot_angle_a)) scan_cos_a
															 , sin(radians(p.scan_robot_angle_a)) scan_sin_a
															 , p.scan_label_angle_a-pa.label_angle_a angle_a
															 , p.scan_robot_pos_x
															 , p.scan_robot_pos_y
																FROM jsonb_array_elements(XYA_codes) a(xya_code)
																	 , jsonb_to_record ( xya_code || jscale ) AS p ( 
																																part_id int8,
																																scan_label_pos_x int2,
																																scan_label_pos_y int2,
																																scan_label_angle_a int2,
																																scan_robot_pos_x real,
																																scan_robot_pos_y real,
																																scan_robot_pos_z real,
																																scan_robot_angle_a real,
																																scaner_scale_x real,
																																scaner_scale_y real)
																JOIN parts pa ON pa.id=p.part_id
																																) AS p
																																
							WHERE id=p.part_id
							RETURNING id)
				SELECT jsonb_agg(id) FROM lay --(SELECT id FROM lay) lay_parts
					INTO jpart_ids;
				RAISE INFO 'xya jpart_ids %',jpart_ids;
				IF jpart_ids IS NULL OR jsonb_array_length(jpart_ids)=0 THEN
						RAISE EXCEPTION 'jsonb_array_length(jpart_ids)=0 XYA_codes %', XYA_codes::text;
				END IF;
				--Clean robot_tasks
				DELETE FROM robot_tasks WHERE to_jsonb(part_id) <@ jpart_ids AND operation_type::text LIKE 'transfer%';
				DELETE FROM robot_tasks WHERE to_jsonb(part_id) <@ jpart_ids AND operation_type::text = 'measuring_height';
				
				UPDATE robot_tasks SET part_number=0 WHERE to_jsonb(part_id) <@ jpart_ids;
				UPDATE robot_tasks SET task_status='not_sended' WHERE to_jsonb(part_id) <@ jpart_ids;

				
				--IF SELECT COUNT(*)=jsonb_array_length(jpart_ids) FROM parts WHERE AND part_status='pallet_in' to_jsonb(part_id) <@ jpart_ids THEN
				--		UPDATE parts SET part_status='pallet_in' WHERE to_jsonb(part_id) <@ jpart_ids;
				--END IF;					
				SELECT process_lay(jpart_ids, id_robot) INTO jlay_res;
				IF jlay_res ? 'error' THEN
						RAISE EXCEPTION 'In process_lay %', jlay_res->'error';
				END IF;
				RESULT := jlay_res; --jsonb_build_object('result', 'OK','robot_task', jtask);
				--Draw layer
				PERFORM get_part_layers(json_data || jsonb_build_object('part_status','pallet_in'
													,'pallet_length_x', setting_get('pallet_length_x')
													,'pallet_length_y', setting_get('pallet_length_y')
													,'lay_number',json_data->'scan_lay_number'
													,'order_id',(SELECT order_id FROM parts WHERE to_jsonb(id) = jpart_ids->0)
																																));
				IF (setting_get('pushpuzzle_mode'))::bool THEN
						SELECT pack_parts(jsonb_build_object('part_source','pallet_in','part_destination','part_slot','robot_id',id_robot)) INTO jpack_parts;
						RAISE INFO 'jpack_parts %', jpack_parts;
						IF jpack_parts ? 'error' THEN
							RAISE EXCEPTION 'In pack_parts pallet_in->part_slot "%" ', jpack_parts->'error';
						END IF;
						RESULT := jlay_res || jpack_parts;
				ELSE
						UPDATE parts SET part_slot = 1 WHERE lay_number = 1 AND to_jsonb(id) <@ jpart_ids;
				END IF;
				--UPDATE parts SET part_status='pallet_in'
				--								,part_side='A'
				--								,robot_id=id_robot
				--		 WHERE to_jsonb(part_id) <@ jpart_ids;
				--RESULT := jsonb_build_object('result', 'OK', 'part_ids', jpart_ids, 'gripping_data', jgripping_data, 'gripping_order', jgripping_order);
	ELSE
				--SELECT get_robot_tasks(-1,id_robot) INTO jtask;
				RAISE INFO 'part_slot->pallet_out id_robot %', id_robot;
				RESULT := jsonb_build_object('result', 'OK','robot_task', jtask);
				IF (setting_get('pushpuzzle_mode'))::bool THEN
						SELECT pack_parts(jsonb_build_object('part_source','part_slot','part_destination','pallet_out','robot_id',id_robot)) INTO jpack_parts;
						RAISE INFO 'jpack_parts %', jpack_parts;
						IF jpack_parts ? 'error' THEN
							RAISE EXCEPTION 'In pack_parts part_slot->pallet_out "%" ', jpack_parts->'error';
						END IF;
						RESULT := RESULT || jpack_parts;
				END IF;
	END IF;
			--IF ((json_data->'gen_robot_task')::bool) AND RESULT ? 'robot_task' THEN
			--	--RESULT := RESULT || jsonb_build_object('robot_task', jtask);
			--	PERFORM pg_notify('robot_task',RESULT->>'robot_task');
			--END IF;
			EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'process_xyacodes',	json_data);


			RETURN RESULT;
		EXCEPTION 
			WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
				err_context := SUBSTRING ( err_context FROM '%function #"%%#" line%' FOR '#' ) || SUBSTRING ( err_context FROM '%[)]#" line ___#"%' FOR '#' );
			RAISE INFO '%', SQLERRM;
			--EXECUTE format ($x$ INSERT INTO error_log ( error_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, SQLERRM, 'process_xyacodes',	json_data);
			--PERFORM send2opc('"Error in process_xyacodes: "' || to_jsonb(SQLERRM) ,6003);
			PERFORM write_error_log ( SQLERRM, json_data :: TEXT, err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
COMMENT ON FUNCTION process_xyacodes IS 'Генерация нового задания по QR кодам';  
END$$;

/*
SELECT process_xyacodes('{"gen_robot_task": true, "XYA_codes": [{"part_id": 157,  "scan_label_pos_x": 1521, "scan_label_pos_y": 513, "scan_label_angle_a": 90, "scan_robot_pos_x": -200.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 2000.0, "scan_robot_angle_a": -180.0}, {"part_id": 158, "scan_label_pos_x": 530, "scan_label_pos_y": 582, "scan_label_angle_a": 270, "scan_robot_pos_x": -200.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 2000.0, "scan_robot_angle_a": -180.0}, {"part_id": 146, "scan_label_pos_x": 1036, "scan_label_pos_y": 497, "scan_label_angle_a": 89, "scan_robot_pos_x": -200.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 2000.0, "scan_robot_angle_a": -180.0}], "scan_lay_number": 1, "robot_id": 1, "plc_id": 1}');

SELECT process_xyacodes('{"gen_robot_task": false, "XYA_codes": [{"part_id": 145, "scan_label_pos_x": 1630, "scan_label_pos_y": 866, "scan_label_angle_a": 358, "scan_robot_pos_x": -200.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 2000.0, "scan_robot_angle_a": -180.0}, {"part_id": 146, "scan_label_pos_x": 577, "scan_label_pos_y": 212, "scan_label_angle_a": 179, "scan_robot_pos_x": -200.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 2000.0, "scan_robot_angle_a": -180.0}, {"part_id": 147, "scan_label_pos_x": 1594, "scan_label_pos_y": 200, "scan_label_angle_a": 357, "scan_robot_pos_x": -200.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 2000.0, "scan_robot_angle_a": -180.0}, {"part_id": 151, "scan_label_pos_x": 618, "scan_label_pos_y": 905, "scan_label_angle_a": 180, "scan_robot_pos_x": -200.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 2000.0, "scan_robot_angle_a": -180.0}], "scan_lay_number": 1, "plc_id": 1, "robot_id": 1}');

*/

DO $$ --process_gripping
BEGIN
DROP FUNCTION IF EXISTS process_gripping;
CREATE OR REPLACE FUNCTION process_gripping(arg jsonb)
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
import shapely.affinity
import math
try:
	arg=json.loads(simplejson.dumps(args[0], use_decimal=True))
	DEBUG = plpy.execute('SELECT is_debug()')[0]['is_debug']
	assert isinstance(arg, dict),'Проверь формат JSON'
	assert 'parts' in arg,'Нужен массив parts'
	assert len(arg['parts'])>0,'Нужен не пустой массив parts'
	assert 'part_id' in arg['parts'][0],'Нужен параметр part_id в элементах массива'

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

	class Part(Rect):
			def __init__(self, part_id, centre_X, centre_Y, pos_Z, length_X, length_Y, thickness_Z, angle, grippers, operation_type, lay_number, part_source):
					super().__init__(centre_X, centre_Y, length_X, length_Y, thickness_Z, angle)
					self.active = True
					self.pos_z = pos_Z
					self.part_id = part_id
					self.grippers = []
					self.operation_type = operation_type
					self.lay_number = lay_number
					self.part_source = part_source
					gripper_angle=angle
					#if length_X<length_Y:
					#		gripper_angle=((gripper_angle+90) % 360)
					for g in grippers:
							gripper = Gripper(centre_X,centre_Y,g['length_x'],g['length_y'],gripper_angle, g['gripper_sign_x'], g['gripper_sign_y'],g['deadzone_x'],g['deadzone_y'],g['gripper_id'],g['gripper_type'])
							self.grippers.append(gripper)

	class Gripper(Rect):
			def __init__(self, centre_X, centre_Y, length_X, length_Y, angle, sign_X, sign_Y, deadzone_X, deadzone_Y, gripper_id, gripper_type):
					super().__init__(centre_X, centre_Y, length_X, length_Y, 0, angle)
					self.sign_X = int(sign_X)*2-1
					self.sign_Y = int(sign_Y)*2-1
					#self.sign_X = int(sign_X)*(-2)+1
					#self.sign_Y = int(sign_Y)*(-2)+1
					self.deadzone_X = deadzone_X
					self.deadzone_Y = deadzone_Y
					self.gripper_id = gripper_id
					self.gripper_type = gripper_type
					# Minimum centre-gropping part (centre part and centre gripper are the same). 
					# In  c a s e  part smaller gropping by edge of gripper
					self.part_length_X0 = length_X+2*deadzone_X
					self.part_length_Y0 = length_Y+2*deadzone_Y
					self.within_part = False


	def is_gripping(part, gripper, parts, pallet):
			if DEBUG: plpy.info(f"====is_gripping {part.part_id} begin====")
			if DEBUG: plpy.info(f"part_length_X0 {gripper.part_length_X0} part_length_Y0 {gripper.part_length_Y0}")
			if DEBUG: plpy.info(f"sign_X {gripper.sign_X} sign_Y {gripper.sign_Y}")
			if DEBUG: plpy.info(f"Центр детали {part.part_id} {part.centre_X},{part.centre_Y} размеры {part.length_X}X{part.length_Y}")
			gripper_offset_x=0        
			gripper_offset_y=0       
			if part.length_X<gripper.part_length_X0:
					gripper_offset_x=gripper.sign_X*(gripper.part_length_X0-part.length_X)/2.0
			if part.length_Y<gripper.part_length_Y0:
					gripper_offset_y=gripper.sign_Y*(gripper.part_length_Y0-part.length_Y)/2.0
			if DEBUG: plpy.info(f"Смещения gripper_offset_x {gripper_offset_x} gripper_offset_y {gripper_offset_y}")
			if DEBUG: plpy.info(f"Центр захвата       {gripper.centre_X},{gripper.centre_Y} размеры {gripper.length_X}X{gripper.length_Y} периметр {gripper.get_contour()}")    
			gripper.translate(gripper_offset_x,gripper_offset_y)    
			if DEBUG: plpy.info(f"Центр захвата trans {gripper.centre_X},{gripper.centre_Y} размеры {gripper.length_X}X{gripper.length_Y} периметр {gripper.get_contour()}")    
			grip_ok = True
			for p in parts:
					if parts.index(part)!=parts.index(p) and p.active:
							intersect=gripper.intersection(p)
							if DEBUG: plpy.info(f"Пересечение {intersect}")
							grip_ok = grip_ok and not intersect
			if pallet:
					grip_ok = grip_ok and gripper.intersection(pallet)
			if DEBUG: plpy.info(f"Деталь {part.part_id} доступна для захвата {grip_ok}")
			if grip_ok:
				gripper.within_part = not gripper.intersection(part)
			return grip_ok

	def process_gripping(gripping_data):
			parts=[]
			for p in gripping_data['parts']:
					part=Part(p['part_id'],p['part_pos_x'],p['part_pos_y'],p['part_pos_z'],p['part_length_x'],p['part_length_y'],p['part_thickness_z'],p['part_angle_a'],gripping_data['grippers'],p['operation_type'],p['lay_number'],p['part_source'])
					parts.append(part)
			gripping_order=[]
			lay_ok = True
			tolerance=gripping_data['intersection_tolerance']
			for part in parts:
				for p in parts:
						if parts.index(part)!=parts.index(p):
								#intersection tolerance limit
								dp=Part(0,p.centre_X,p.centre_Y,p.pos_z,p.length_X-tolerance,p.length_Y-tolerance,p.thickness_Z,p.angle,[],'',0,'')
								lay_ok=lay_ok and not part.intersection(dp)
			if lay_ok:
				for i in range(len(parts)+1):
						for part in parts:
								for gripper in part.grippers:
										if part.active and (i>0 or gripper==part.grippers[0]):
												if 'pallet_length_x' in gripping_data and 'pallet_length_y' in gripping_data:
														length_X=gripping_data['pallet_length_x']
														length_Y=gripping_data['pallet_length_y']
														plt=Part(0,length_X/2.0,length_Y/2.0,0,length_X,length_Y,0,0,[],'',0,'')
														plt_ok = not part.intersection(plt) or True
												else:
														plt = None
														plt_ok = True
												if plt_ok and is_gripping(part, gripper, parts, plt):
														part.active = False
														if DEBUG: plpy.info(f"part {part.part_id}: plt_ok and is_gripping True")
														gripping_order.append(dict(part_id=part.part_id,operation_type=part.operation_type,lay_number=part.lay_number,part_source=part.part_source,gripper_id=gripper.gripper_id,gripper_type=gripper.gripper_type,gripper_within_part=gripper.within_part))
				res=gripping_order
				if not res:
					res=dict(measuring_height=True,error='no detail can be gripped',source='process_gripping')
				#if len(parts)!=len(res):
				#	res=dict(measuring_height=True,error='not all details can be gripped',source='process_gripping')
			else:
				res=dict(measuring_height=True,error='intersection of parts (several layers scanned)',source='process_gripping')
			return res
			
	res = process_gripping(arg)		
	return res
except Exception as e:
	traceback_info=traceback.format_exc().splitlines()[1].split(', ')
	source=traceback_info[1]+' '+'_'.join(traceback_info[2].split('_')[slice(4,-1)])
	err_arg = [traceback_info[1]+': '+str(e),simplejson.dumps(args,ensure_ascii=False,use_decimal=True)]
	res = plpy.execute(plpy.prepare(f'SELECT write_error_log($1,$2)', ['text','text']), err_arg)
	return dict(source=source,error=str(e),args=arg)
$BODY$
	LANGUAGE plpython3u
	COST 100;
COMMENT ON FUNCTION process_gripping IS 'Обработка операций по перемещению ячеек';	
END$$;

/*
SELECT debug_off();
SELECT debug_on();
SELECT process_gripping('[{"plc_id": 1, "part_id": 3, "grippers": [{"length_x": 640, "length_y": 165, "deadzone_x": 50, "deadzone_y": 50, "gripper_id": 3, "gripper_type": "transfer", "gripper_sign_x": false, "gripper_sign_y": false}, {"length_x": 640, "length_y": 165, "deadzone_x": 50, "deadzone_y": 50, "gripper_id": 2, "gripper_type": "transfer", "gripper_sign_x": true, "gripper_sign_y": false}], "lay_number": 1, "part_pos_x": -85.06956, "part_pos_y": 6.1060786, "part_angle_a": 22, "part_length_x": 350, "part_length_y": 350, "operation_type": "transfer_in2out", "part_thickness_z": 16}, {"plc_id": 1, "part_id": 4, "grippers": [{"length_x": 640, "length_y": 165, "deadzone_x": 50, "deadzone_y": 50, "gripper_id": 3, "gripper_type": "transfer", "gripper_sign_x": false, "gripper_sign_y": false}, {"length_x": 640, "length_y": 165, "deadzone_x": 50, "deadzone_y": 50, "gripper_id": 2, "gripper_type": "transfer", "gripper_sign_x": true, "gripper_sign_y": false}], "lay_number": 1, "part_pos_x": 272.2226, "part_pos_y": 74.23706, "part_angle_a": 18, "part_length_x": 350, "part_length_y": 350, "operation_type": "transfer_in2out", "part_thickness_z": 16}]');

SELECT process_gripping('[{"plc_id": 1, "part_id": 157, "grippers": [{"length_x": 640, "length_y": 165, "deadzone_x": 180, "deadzone_y": 50, "gripper_id": 4, "gripper_type": "machining", "gripper_sign_x": true, "gripper_sign_y": false}, {"length_x": 640, "length_y": 165, "deadzone_x": 50, "deadzone_y": 50, "gripper_id": 3, "gripper_type": "transfer", "gripper_sign_x": false, "gripper_sign_y": false}, {"length_x": 640, "length_y": 165, "deadzone_x": 50, "deadzone_y": 50, "gripper_id": 2, "gripper_type": "transfer", "gripper_sign_x": true, "gripper_sign_y": false}], "lay_number": 1, "part_pos_x": 231.24937, "part_pos_y": 132.3018, "part_angle_a": 219, "part_length_x": 568, "part_length_y": 250, "operation_type": "machining", "part_thickness_z": 16}, {"plc_id": 1, "part_id": 148, "grippers": [{"length_x": 640, "length_y": 165, "deadzone_x": 180, "deadzone_y": 50, "gripper_id": 4, "gripper_type": "machining", "gripper_sign_x": true, "gripper_sign_y": false}, {"length_x": 640, "length_y": 165, "deadzone_x": 50, "deadzone_y": 50, "gripper_id": 3, "gripper_type": "transfer", "gripper_sign_x": false, "gripper_sign_y": false}, {"length_x": 640, "length_y": 165, "deadzone_x": 50, "deadzone_y": 50, "gripper_id": 2, "gripper_type": "transfer", "gripper_sign_x": true, "gripper_sign_y": false}], "lay_number": 1, "part_pos_x": -207.37862, "part_pos_y": -208.39067, "part_angle_a": 40, "part_length_x": 590, "part_length_y": 390, "operation_type": "machining", "part_thickness_z": 16}]');


*/


DO $$ --process_label  (Conveyor)
BEGIN
CREATE OR REPLACE FUNCTION process_label(json_data jsonb)
  RETURNS jsonb AS $BODY$
DECLARE
  err_context text;
  XYA_codes jsonb;
  jparts jsonb;
	jgrippers jsonb;
	jgripping_data jsonb;
	jpart_ids jsonb;
	jgripping_order jsonb;
	jtask jsonb;
	RESULT jsonb;
	scaner_FOVx real;
	scaner_FOVy real;
	scaner_d0 real;
	scaner_BOXx real;
	scaner_BOXy real;
	scaner_alpha0_x real;
	scaner_alpha0_y real;
	scaner_scale_x real;
	scaner_scale_y real;
	jscale jsonb;
	robot_pos_z real;
	ref_pos jsonb;

BEGIN
	SELECT jsonb_path_query(json_data->'XYA_codes','$[*] ? (@.part_id==0)') INTO ref_pos;
	SELECT jsonb_path_query_array(json_data->'XYA_codes','$[*] ? (@.type()=="object")') INTO XYA_codes;
	SELECT jsonb_path_query_array(XYA_codes,'$[*] ? (@.part_id!=0)') INTO XYA_codes;
	RESULT := jsonb_build_object('result', 'NOK');
	IF (jsonb_array_length(XYA_codes)=1) THEN
		scaner_BOXx:= setting_get('scaner_BOXx')::real;
		scaner_BOXy:= setting_get('scaner_BOXy')::real;
		scaner_d0:= setting_get('scaner_d0')::real;
		robot_pos_z:= setting_get('robot_pos_z')::real;
		
		--robot_pos_z:=(json_data->'XYA_codes'->0->'scan_robot_pos_z')::real;
		IF robot_pos_z<1500 THEN
			scaner_alpha0_x:= 34;
			scaner_alpha0_y:= 19.537;
		ELSE
			IF robot_pos_z<1600 THEN
				scaner_alpha0_x:= 33.6;
				scaner_alpha0_y:= 19.23;
			ELSE
				scaner_alpha0_x:= 33.29;
				scaner_alpha0_y:= 19.082;
			END IF;
		END IF;
		
		
		
		scaner_FOVx:= 2*((robot_pos_z + scaner_d0) * tan(radians(scaner_alpha0_x/2.0)));
		scaner_FOVy:= 2*((robot_pos_z + scaner_d0) * tan(radians(scaner_alpha0_y/2.0)));
		scaner_scale_x:=scaner_FOVx/scaner_BOXx;
		scaner_scale_y:=scaner_FOVy/scaner_BOXy;
		jscale:=jsonb_build_object('scaner_scale_x',scaner_scale_x,'scaner_scale_y',scaner_scale_y);
		
		WITH lay AS (
					UPDATE parts SET label_pos_x = p.scan_robot_pos_x-setting_get('ref_delta_x')::real + (p.x * cos(radians(p.a)) - p.y * sin(radians(p.a)))
													,label_pos_y = p.scan_robot_pos_y-setting_get('ref_delta_y')::real + (p.x * sin(radians(p.a)) + p.y * cos(radians(p.a)))
													,label_angle_a = p.label_angle_a
													--,part_pos_x=p.x
													--,part_pos_y=p.y
													--,robot_id=p.robot_id
													--,operation_type=(SELECT operation_type FROM plcs WHERE id = id_plc)
													--,lay_number=(json_data->'scan_lay_number')::int2
							FROM 		(SELECT 	 p.part_id
															 --, p.robot_id
															 --, setting_get('label_offset_x')::real dx
															 --, setting_get('label_offset_y')::real dy
															 , -(p.scan_label_pos_x-(ref_pos->'scan_label_pos_x')::int4)*p.scaner_scale_x - setting_get('label_offset_x')::real x
															 , (p.scan_label_pos_y-(ref_pos->'scan_label_pos_y')::int4)*p.scaner_scale_y - setting_get('label_offset_y')::real y
															 , (p.scan_robot_angle_a+setting_get('ref_delta_a')::real+(ref_pos->'scan_label_angle_a')::real - p.scan_label_angle_a)::int4 % 360 label_angle_a
															 , p.scan_robot_angle_a a
															 , p.scan_robot_pos_x
															 , p.scan_robot_pos_y
																FROM jsonb_array_elements(XYA_codes) a(xya_code)
																	 , jsonb_to_record ( xya_code || jscale ) AS p ( 
																																part_id int8,
																																robot_id int8,
																																scan_label_pos_x int2,
																																scan_label_pos_y int2,
																																scan_label_angle_a int2,
																																scan_robot_pos_x real,
																																scan_robot_pos_y real,
																																scan_robot_angle_a real,
																																scaner_scale_x real,
																																scaner_scale_y real)
																JOIN parts pa ON pa.id=p.part_id
																																) AS p
																																
							WHERE id=p.part_id
							RETURNING id)
				SELECT COALESCE(jsonb_agg(id),'[]') FROM (SELECT id FROM lay) lay_parts
					INTO jparts;
				
			RESULT := jsonb_build_object('result', 'OK', 'part_ids', jparts);
			EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'process_label',	json_data);

	END IF;
			RETURN RESULT;
		EXCEPTION 
			WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
				err_context := SUBSTRING ( err_context FROM '%function #"%%#" line%' FOR '#' ) || SUBSTRING ( err_context FROM '%[)]#" line ___#"%' FOR '#' );
			RAISE INFO '%', SQLERRM;
			--EXECUTE format ($x$ INSERT INTO error_log ( error_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, SQLERRM, 'process_xyacodes',	json_data);
			--PERFORM send2opc('"Error in process_xyacodes: "' || to_jsonb(SQLERRM) ,6003);
			PERFORM write_error_log ( SQLERRM, json_data :: TEXT, err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
COMMENT ON FUNCTION process_label IS 'Запись координат этикетки в базу';  
END$$;
				
/*
SELECT process_label('{"XYA_codes": [{"part_id": 159, "robot_id": 2, "scan_label_pos_x": 407, "scan_label_pos_y": 717, "scan_label_angle_a": 270, "scan_robot_pos_x": 185.0, "scan_robot_pos_y": 380.0, "scan_robot_pos_z": 1515.0, "scan_robot_angle_a": 270.0}, {"part_id": 0, "robot_id": 2, "scan_label_pos_x": 49, "scan_label_pos_y": 58, "scan_label_angle_a": 1, "scan_robot_pos_x": 185.0, "scan_robot_pos_y": 380.0, "scan_robot_pos_z": 1515.0, "scan_robot_angle_a": 270.0}], "plc_id": 2}');

SELECT process_label('{"XYA_codes": [{"part_id": 150, "robot_id": 2, "scan_label_pos_x": 506, "scan_label_pos_y": 687, "scan_label_angle_a": 89, "scan_robot_pos_x": 185.0, "scan_robot_pos_y": 380.0, "scan_robot_pos_z": 1515.0, "scan_robot_angle_a": 270.0}, {"part_id": 0, "robot_id": 2, "scan_label_pos_x": 50, "scan_label_pos_y": 58, "scan_label_angle_a": 2, "scan_robot_pos_x": 185.0, "scan_robot_pos_y": 380.0, "scan_robot_pos_z": 1515.0, "scan_robot_angle_a": 270.0}], "plc_id": 2}');

SELECT process_label('{"XYA_codes": [{"part_id": 181, "robot_id": 2, "scan_label_pos_x": 359, "scan_label_pos_y": 651, "scan_label_angle_a": 89, "scan_robot_pos_x": 185.0, "scan_robot_pos_y": 380.0, "scan_robot_pos_z": 1515.0, "scan_robot_angle_a": 270.0}, {"part_id": 0, "robot_id": 2, "scan_label_pos_x": 50, "scan_label_pos_y": 58, "scan_label_angle_a": 1, "scan_robot_pos_x": 185.0, "scan_robot_pos_y": 380.0, "scan_robot_pos_z": 1515.0, "scan_robot_angle_a": 270.0}], "plc_id": 2}');


SELECT process_label('{"XYA_codes": [{"part_id": 182, "robot_id": 2, "scan_label_pos_x": 407, "scan_label_pos_y": 717, "scan_label_angle_a": 270, "scan_robot_pos_x": 185.0, "scan_robot_pos_y": 380.0, "scan_robot_pos_z": 1515.0, "scan_robot_angle_a": 270.0}, {"part_id": 0, "robot_id": 2, "scan_label_pos_x": 49, "scan_label_pos_y": 58, "scan_label_angle_a": 1, "scan_robot_pos_x": 185.0, "scan_robot_pos_y": 380.0, "scan_robot_pos_z": 1515.0, "scan_robot_angle_a": 270.0}], "plc_id": 2}');

SELECT process_label('{"XYA_codes": [{"part_id": 219, "robot_id": 2, "scan_label_pos_x": 322, "scan_label_pos_y": 514, "scan_label_angle_a": 270, "scan_robot_pos_x": 185.0, "scan_robot_pos_y": 380.0, "scan_robot_pos_z": 0.0, "scan_robot_angle_a": 270.0}, {"part_id": 0, "robot_id": 2, "scan_label_pos_x": 56, "scan_label_pos_y": 53, "scan_label_angle_a": 2, "scan_robot_pos_x": 185.0, "scan_robot_pos_y": 380.0, "scan_robot_pos_z": 0.0, "scan_robot_angle_a": 270.0}], "plc_id": 2}');


-- select 2*((1523) * tan(radians(33.6/2.0)))/1920*(266) ,    2*((1523 ) * tan(radians(19.337/2.0)))/1080*(461) ;

SELECT process_label('{"XYA_codes": [{"part_id": 220, "robot_id": 2, "scan_label_pos_x": 362, "scan_label_pos_y": 356, "scan_label_angle_a": 89, "scan_robot_pos_x": 185.0, "scan_robot_pos_y": 380.0, "scan_robot_pos_z": 0.0, "scan_robot_angle_a": 270.0}, {"part_id": 0, "robot_id": 2, "scan_label_pos_x": 56, "scan_label_pos_y": 53, "scan_label_angle_a": 1, "scan_robot_pos_x": 185.0, "scan_robot_pos_y": 380.0, "scan_robot_pos_z": 0.0, "scan_robot_angle_a": 270.0}], "plc_id": 2}');

-- select 2*((1523) * tan(radians(33.6/2.0)))/1920*(306) ,    2*((1523 ) * tan(radians(19.337/2.0)))/1080*(303) ;


127/146 - (20)  -20

221/145 - (76)  -24

ref_delta_x	5
ref_delta_y	8
ref_delta_a	0
label_offset_x	12
label_offset_y	16


SELECT process_label('{"XYA_codes": [{"part_id": 195, "robot_id": 2, "scan_label_pos_x": 420, "scan_label_pos_y": 515, "scan_label_angle_a": 270, "scan_robot_pos_x": 185.0, "scan_robot_pos_y": 380.0, "scan_robot_pos_z": 0.0, "scan_robot_angle_a": 268.0}, {"part_id": 0, "robot_id": 2, "scan_label_pos_x": 56, "scan_label_pos_y": 53, "scan_label_angle_a": 1, "scan_robot_pos_x": 185.0, "scan_robot_pos_y": 380.0, "scan_robot_pos_z": 0.0, "scan_robot_angle_a": 268.0}], "plc_id": 2}');

SELECT process_label('{"XYA_codes": [{"part_id": 196, "robot_id": 2, "scan_label_pos_x": 420, "scan_label_pos_y": 513, "scan_label_angle_a": 270, "scan_robot_pos_x": 185.0, "scan_robot_pos_y": 380.0, "scan_robot_pos_z": 0.0, "scan_robot_angle_a": 268.0}, {"part_id": 0, "robot_id": 2, "scan_label_pos_x": 56, "scan_label_pos_y": 48, "scan_label_angle_a": 2, "scan_robot_pos_x": 185.0, "scan_robot_pos_y": 380.0, "scan_robot_pos_z": 0.0, "scan_robot_angle_a": 268.0}], "plc_id": 2}');
*/


DO $$ --send2opc
BEGIN
DROP FUNCTION IF EXISTS send2opc;
CREATE OR REPLACE FUNCTION send2opc(data jsonb, port int4)
  RETURNS text TRANSFORM FOR TYPE jsonb AS $BODY$
# TRANSFORM FOR TYPE jsonb
from multiprocessing.connection import Connection, answer_challenge, deliver_challenge
import socket, struct
import traceback
import time
import simplejson
def ClientWithTimeout(address, authkey, timeout):
  with socket.socket(socket.AF_INET) as s:
    s.setblocking(True)
    s.connect(address)
    seconds = int(timeout)
    microseconds = int((timeout - seconds) * 1e6)
    timeval = struct.pack("@LL", seconds, microseconds)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVTIMEO, timeval)
    c = Connection(s.detach())
  answer_challenge(c, authkey)
  deliver_challenge(c, authkey)
  return c
try:
	data=simplejson.loads(simplejson.dumps(args[0], use_decimal=True))
	DEBUG = plpy.execute('SELECT is_debug()')[0]['is_debug']
	status='not sended'
	address = ('localhost', port)
	conn = ClientWithTimeout(address, timeout=3, authkey=b'expopsw')
	conn.send(data)
	start = time.time()
	msg = '{}'
	#if DEBUG:plpy.info(f'start at "{start}"')
	while (time.time()-start)<3.2:
		if conn.poll():
			#if DEBUG:plpy.info(f'conn.poll() on "{time.time()-start}"')
			msg = conn.recv()
			if msg['result']=='OK':
				status='sended'
			#	if DEBUG:plpy.info(f'status: "{status}"')
			#else:
			#	if DEBUG:plpy.info(f'Неверный ответ OPC: "{msg}"')
			start=0
			conn.close()
	if start!=0:
		if DEBUG:plpy.info('OPC недоступен')
	return msg
except Exception as e:
	if DEBUG:plpy.info(f'Сообщение не отправлено. {e}')
	return status
$BODY$
  LANGUAGE plpython3u IMMUTABLE;
	COMMENT ON FUNCTION send2opc IS 'Отправка сообщения в OPC';
END$$;


/*
SELECT send2opc('{"robot_task_id":4, "zone_from":3, "zone_to":2, "required":1, "cover":[1], "thickness":[1]}',6000);
SELECT send2opc('{"error":"error_message"}',6001);
SELECT send2opc('{"error":"ТЕСТ ошибок OPC производства","source":"кириллица"}',6003);
SELECT send2opc('"ТЕСТ ошибок OPC производства, кириллица"',6003);
*/


DO $$ --process_lay
BEGIN
DROP FUNCTION IF EXISTS process_lay;
CREATE OR REPLACE FUNCTION process_lay(jpart_ids jsonb/*, id_plc int8*/, id_robot int8)
RETURNS jsonb
AS $BODY$
DECLARE
  err_context text;
	jgrippers jsonb;
	jparts jsonb;
	jgripping_data jsonb;
	--jpart_ids jsonb;
	jgripping_order jsonb;
	jtask jsonb;
	part record;
	RESULT jsonb;
BEGIN
					SELECT jsonb_agg(to_jsonb(g.*)-'id'-'robot_id'-'operation_type'
																								|| jsonb_build_object('gripper_id',g.id)
																								|| jsonb_build_object('gripper_type',g.operation_type)
																								ORDER BY operation_type='machining' DESC
																								) grippers
									FROM grippers g 
									WHERE g.robot_id=id_robot
												AND operation_type!='scanning'
					INTO jgrippers;		
					
					RAISE INFO 'jgrippers %',jgrippers;
				IF jgrippers IS NULL THEN
					RAISE EXCEPTION 'jgrippers IS NULL, robot_id %', id_robot; 
				END IF;
				IF jsonb_array_length(jpart_ids)=0 THEN
					RAISE EXCEPTION 'jsonb_array_length(jpart_ids)=0, robot_id %', id_robot; 
				END IF;
				RAISE INFO 'jpart_ids % id_robot %',jpart_ids, id_robot;
				--TODO если нет обработки - то трансфер
				SELECT jsonb_agg(jsonb_build_object(--'grippers', jgrippers --COALESCE(jgrippers->(t.task->>'operation_type'), jgrippers->'transfer')
																					  'part_id', pa.id
																					, 'part_pos_x', pa.part_pos_x
																					, 'part_pos_y', pa.part_pos_y
																					, 'part_pos_z', pa.part_pos_z
																					, 'part_length_x', pa.part_length_x
																					, 'part_length_y', pa.part_length_y
																					, 'part_thickness_z', pa.part_thickness_z
																					, 'part_angle_a', pa.part_angle_a
																					, 'part_source',  COALESCE(t.task->>'part_status',pa.part_status::text)
																					--, 'part_destination', pa.part_destination
																					, 'lay_number', pa.lay_number
																					--, 'plc_id', id_plc
																					, 'robot_id', id_robot
																					, 'operation_type', COALESCE(t.task->>'operation_type','transfer_in2out')
																						)
												)
											FROM (SELECT part_id::int8, (SELECT get_robot_tasks(part_id::int8,id_robot) LIMIT 1) task
																	FROM jsonb_array_elements(jpart_ids) p(part_id)) t
											JOIN parts pa ON pa.id=t.part_id
					INTO jparts;
				IF jparts IS NULL THEN
					RAISE EXCEPTION 'jgripping_data IS NULL, jpart_ids %, jgrippers %', jpart_ids, jgrippers::text; 
				END IF;
				jgripping_data := jsonb_build_object('intersection_tolerance', setting_get('intersection_tolerance'),'parts', jparts,'grippers', jgrippers);
				RAISE INFO 'SELECT process_gripping(''%'');',jgripping_data;
				SELECT process_gripping(jgripping_data) INTO jgripping_order;
				RAISE INFO 'jgripping_order %',jgripping_order;
				IF jgripping_order IS NULL THEN
					RAISE EXCEPTION 'jgripping_order IS NULL, jgripping_data %', jgripping_data::text; 
				END IF;
				IF jgripping_order ? 'error' THEN
						IF jgripping_order ? 'measuring_height' AND (setting_get('measuring_mode'))::bool THEN
								RAISE INFO 'measuring_height';
								FOR part IN (
															--SELECT x.* FROM jsonb_array_elements(jpart_ids) WITH ORDINALITY x(part_id,idx) 													
															SELECT x.*,p.part_status,p.part_pos_z
																	FROM jsonb_array_elements(jpart_ids) WITH ORDINALITY x(part_id,idx)
																	JOIN parts p ON p.id=x.part_id::int8
															) LOOP --|| '[0]'::jsonb
										IF part.part_status='scanned' AND part.part_pos_z=0 THEN
												SELECT task_add(jsonb_build_object( 'part_id', part.part_id,
																														'robot_id', id_robot,
																														'operation_type', 'measuring_height',
																														'operation_number', 1,
																														'part_number', part.idx, --CASE part.part_id::int8 WHEN 0 THEN 1 ELSE 0 END,
																														'operation_content', jsonb_build_object(),
																														'operation_side', 'A'
																														)
																				)
												INTO jtask;
										ELSE
												UPDATE parts SET part_status='ordered' WHERE id=part.part_id::int8;
												--UPDATE parts SET part_status='pallet_in' WHERE id=part.part_id::int8 AND part_pos_z!=0;
										END IF;
								END LOOP;
						ELSE
								RAISE EXCEPTION 'error "%" source "%"', jgripping_order->>'error' , jgripping_order->>'source';
						END IF;
				END IF;
				--Для пересекающихся деталей сгенерирровать задания на измерение
				
				--Если детали пе пересекаются, то записать порядок разборки слоя
				IF NOT(jgripping_order ? 'measuring_height') THEN
						RAISE INFO 'SELECT set_parts_order(''%'',%)',jgripping_order,id_robot;
						SELECT set_parts_order(jgripping_order,id_robot) INTO jtask;
						IF jtask ? 'error' THEN
							RAISE EXCEPTION 'error "%" source "%"', jtask->>'error' , jtask->>'source'; 
						END IF;
						RESULT := jsonb_build_object('result', 'OK', 'part_ids', jpart_ids, 'gripping_data', jgripping_data, 'gripping_order', jgripping_order, 'robot_task', jtask);
				END IF;
				RESULT := jsonb_build_object('result', 'OK', 'part_ids', jpart_ids, 'gripping_data', jgripping_data, 'gripping_order', jgripping_order);

			EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'process_lay',	jsonb_build_object('part_ids', jpart_ids, 'robot_id', id_robot));


			RETURN RESULT;
		EXCEPTION 
			WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
				err_context := SUBSTRING ( err_context FROM '%function #"%%#" line%' FOR '#' ) || SUBSTRING ( err_context FROM '%[)]#" line ___#"%' FOR '#' );
			RAISE INFO '%', SQLERRM;
			--EXECUTE format ($x$ INSERT INTO error_log ( error_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, SQLERRM, 'process_lay',	jsonb_build_object('robot_id', jparts, 'robot_id', id_robot));
			--PERFORM send2opc('"Error in process_lay: "' || to_jsonb(SQLERRM) ,6003);
			PERFORM write_error_log ( SQLERRM, jsonb_build_object('jpart_ids', jpart_ids, 'robot_id', id_robot) :: TEXT, err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
COMMENT ON FUNCTION process_lay IS 'Обработка слоя';  
END$$;

/*
SELECT process_lay('[151,152]');
*/

DO $$ --set_parts_order
BEGIN
DROP FUNCTION IF EXISTS set_parts_order;
CREATE OR REPLACE FUNCTION set_parts_order(json_data jsonb, id_robot int8)
  RETURNS jsonb AS $BODY$
    DECLARE
			err_context text;
			jtask jsonb;
			jtaskadd_res jsonb;
			rtask record;
			part_id int8;
			lay record;
			RESULT jsonb;
			type_operation text;
    BEGIN

			FOR lay IN (SELECT x.*
											FROM jsonb_array_elements(json_data) WITH ORDINALITY o(part,idx),
													 jsonb_to_record(part||jsonb_build_object('idx',idx)) AS x ( 
																																idx int2,
																																part_id int8,
																																gripper_id int8,
																																operation_type type_task_type,
																																part_source text,
																																lay_number int2,
																																--part_destination type_part_location,
																																gripper_type type_task_type
																																)
									) LOOP
						--UPDATE parts p SET part_number=lay.idx, gripper_id=lay.gripper_id WHERE p.part_id = lay.part_id;
						UPDATE robot_tasks rt SET gripper_id=lay.gripper_id
								WHERE rt.part_id = lay.part_id AND rt.operation_type = lay.gripper_type;			
						UPDATE robot_tasks rt SET part_number=lay.idx
								WHERE rt.part_id = lay.part_id;			
						--UPDATE parts p SET part_destination=lay.part_destination WHERE p.part_id = lay.part_id;
						--part_destination='pallet_out'
						--UPDATE parts p SET part_destination='flip_table' WHERE p.part_id = lay.part_id AND lay.operation_type!=lay.gripper_type;
						
						IF NOT lay.part_source IN ('scanned','pallet_in','part_slot') THEN
								--RAISE EXCEPTION 'Unknown part_source - "%"', lay.part_source;
								lay.part_source = 'pallet_in';
						END IF;
						IF lay.part_source='scanned' THEN
								UPDATE parts p SET part_status='pallet_in' WHERE p.id = lay.part_id;
								lay.part_source='pallet_in';
						END IF;
						type_operation := '';
						IF lay.part_source='pallet_in' THEN
								IF (SELECT COUNT(*)!=0
														FROM robot_tasks rt
														JOIN robots r ON r.id = id_robot
														WHERE rt.part_id=lay.part_id 
															AND rt.task_status!='done' 
															--AND rt.operation_type!='transfer_slot2out' 
															AND rt.operation_type!='measuring_height' 
															AND rt.operation_type!='transfer_in2flip'
															AND to_jsonb(lay.operation_type::text) <@ r.operation_list
																											) THEN
										RAISE INFO 'Exists undone tasks for % operation % gripper %', lay.part_id,lay.operation_type::text,lay.gripper_type::text;
										IF lay.operation_type::text!=lay.gripper_type::text THEN
												--IF (SELECT to_jsonb(type_operation) <@ operation_list FROM robots WHERE id = id_robot) THEN
														type_operation:='transfer_in2flip';
												--ELSE
												--		type_operation:='transfer_in2slot';
												--END IF;
										END IF;
								ELSE
										RAISE INFO 'No undone tasks for %', lay.part_id;
										IF (SELECT to_jsonb(lay.operation_type::text) <@ operation_list FROM robots WHERE id = id_robot) THEN
												IF lay.lay_number=1 AND NOT (setting_get('pushpuzzle_mode'))::bool THEN
														RAISE INFO 'First lay part %', lay.part_id;
														type_operation:='transfer_in2slot';
												ELSE
														RAISE INFO 'There is a subscription to the operation for % and not first lay', lay.part_id;
														type_operation:='transfer_in2out';
												END IF;
										ELSE
												RAISE INFO 'No subscription to operation for %', lay.part_id;
												type_operation:='transfer_in2slot';
										END IF;
								END IF;
						ELSE
								IF lay.part_source='part_slot' THEN
										type_operation:='transfer_slot2out';
								END IF;
						END IF;
						
						IF type_operation != '' THEN
								SELECT task_add(jsonb_build_object( 'part_id', lay.part_id,
																										'robot_id', id_robot,
																										'gripper_id', lay.gripper_id,
																										'operation_type', type_operation,
																										'operation_number', 1,
																										'part_number', lay.idx,
																										'operation_content', jsonb_build_object(),
																										'operation_side', ' '
																										)
																)
								INTO jtaskadd_res;										
								RAISE INFO 'lay.part_id % jtaskadd_res %', lay.part_id, jtaskadd_res;
						END IF;
						
			END LOOP;
			/*
			PERFORM task_add(jsonb_build_object('part_id', p.id,
																					'robot_id', id_robot,
																					--'gripper_id', (SELECT id FROM grippers WHERE robot_id=id_robot AND operation_type='transfer' LIMIT 1),
																					'operation_type', 'transfer_in2out',
																					'operation_number', 1,
																					'operation_content', jsonb_build_object(),
																					'operation_side', ' '
																					)
											) --SELECT p.id
					FROM parts p
							LEFT JOIN robot_tasks rt ON p.id=rt.part_id
							WHERE part_status='pallet_in' AND p.id!=0
							GROUP BY p.id
							HAVING BOOL_AND(rt.task_status='done') OR BOOL_AND(rt.task_status IS NULL);
			*/				
					--INTO jtaskadd_res;
			--RAISE INFO 'jtaskadd_res %', jtaskadd_res;
			SELECT get_robot_tasks((json_data->0->'part_id')::int8,(json_data->0->'robot_id')::int8) INTO jtask;
			RESULT := jtask;
			EXECUTE format ($x$ INSERT INTO info_log (info_text, SOURCE, DATA) VALUES ('%s', '%s', '%s'); $x$, RESULT, 'set_parts_order',	json_data);

			RETURN RESULT;
		EXCEPTION 
			WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
				err_context := SUBSTRING ( err_context FROM '%function #"%%#" line%' FOR '#' ) || SUBSTRING ( err_context FROM '%[)]#" line ___#"%' FOR '#' );
			RAISE INFO '%',	SQLERRM;
			PERFORM write_error_log ( SQLERRM, json_data :: TEXT, err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context || '. ' || (jsonb_build_object('rtask',rtask)::text),
																	'source', 'set_gripping_order');
    END;
  $BODY$
  LANGUAGE plpgsql VOLATILE;
	COMMENT ON FUNCTION set_parts_order IS 'Обновить место назначения деталей и порядок захвата';
END$$;
				
/*
SELECT set_parts_order('[{"part_id": 209758003100001, "gripper_id": 4, "lay_number": 3, "part_source": "pallet_in", "gripper_type": "machining", "operation_type": "machining", "gripper_within_part": false}, {"part_id": 209758003000002, "gripper_id": 4, "lay_number": 3, "part_source": "pallet_in", "gripper_type": "machining", "operation_type": "machining", "gripper_within_part": false}, {"part_id": 209758001109001, "gripper_id": 4, "lay_number": 3, "part_source": "pallet_in", "gripper_type": "machining", "operation_type": "machining", "gripper_within_part": false}, {"part_id": 209758003100002, "gripper_id": 4, "lay_number": 3, "part_source": "pallet_in", "gripper_type": "machining", "operation_type": "machining", "gripper_within_part": false}]',1);
*/


