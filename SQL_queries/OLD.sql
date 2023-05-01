
/*
DO $$ --parts_add()
BEGIN	DROP FUNCTION
	IF EXISTS parts_add;
	CREATE OR REPLACE FUNCTION parts_add ( json_data jsonb ) 
	RETURNS jsonb AS $BODY$ 
	DECLARE
		err_context TEXT;
		part record;
		jpart_ids jsonb;
	BEGIN
			FOR part IN (
												SELECT *,	p.ID,	p.NAME AS local_name 
												FROM
													jsonb_array_elements ( json_data ) AS A ( pr ),
													jsonb_to_record ( pr ) AS x (
														cover int4,
														thickness int4,
														NAME VARCHAR ( 150 ),
														ALIAS VARCHAR ( 25 ),
														STORAGE VARCHAR ( 10 ),
														external_id VARCHAR ( 38 ),
														standard_count int4,
														rack_position int4,
														top_protect bool,
														sector_id int4
				)
				LEFT JOIN parts p ON p.external_id = x.external_id 
			)
			LOOP
			
				INSERT INTO parts ( external_id, part_number, order_id, part_status, part_length_X, part_length_Y, part_thickness_Z, label_pos_X, label_pos_Y, label_angle_a, part_angle_A )
			VALUES
				(
					part.external_id,
					part.part_number,
					part.order_id,
					part.part_status,
					part.part_length_X,
					part.part_length_Y,
					part.part_thickness_Z,
					part.label_pos_X,
					part.label_pos_Y,
					part.label_angle_a,
					part.part_angle_A
				) RETURNING COALESCE (jpart_ids, '[]' ) || jsonb_build_object ( 'id', ID, 'external_id', external_id ) INTO jpart_ids;

			UPDATE parts 
			SET cover = part.cover,
			thickness = part.thickness,
			ALIAS = COALESCE ( part.ALIAS, '' ),
			STORAGE = COALESCE ( part.STORAGE, '' ),
			external_id = part.external_id,
			standard_count = COALESCE ( part.standard_count, 24 ),
			rack_position = COALESCE ( part.rack_position, 0 ),
			top_protect = part.top_protect,
			sector_id = part.sector_id
			WHERE
				ID = part.ID RETURNING COALESCE ( jpart_ids, '[]' ) || jsonb_build_object ( 'id', ID, 'external_id', external_id ) INTO jpart_ids;
			
		END IF;
		
	END LOOP;
RETURN jsonb_build_object ( 'result', 'OK', 'parts', COALESCE ( jpart_ids, '[]' ) );
EXCEPTION 
WHEN OTHERS THEN
	GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
RAISE INFO '%',
SQLERRM;
PERFORM write_error_log ( SQLERRM, json_data :: TEXT, err_context );
RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );

END;
$BODY$ LANGUAGE plpgsql VOLATILE;

END $$;
*/
/*
SELECT parts_add('');
*/


DO $$ --get_slot 
BEGIN
DROP FUNCTION IF EXISTS get_slot;
CREATE OR REPLACE FUNCTION get_slot(task jsonb)
RETURNS jsonb
AS $BODY$
DECLARE
		err_context text;
		jslot jsonb;
		jpacking_output jsonb;
BEGIN
				--calc slot_pos
				IF (((task->'part_length_x')::real*(task->'part_length_y')::real)<((setting_get('min_slot_square'))::real))
							AND ((task->'part_length_x')::real<((setting_get('min_slot_lenth'))::real))
							AND ((task->'part_length_y')::real<((setting_get('min_slot_lenth'))::real))
							AND (task->>'operation_type'!='scanning')  THEN
					--slot 1
					SELECT process_packing(jsonb_build_object('parts', jsonb_build_array(jsonb_build_object(
																													'part_id', task->'part_id',
																													'part_length_x', (SELECT GREATEST((task->'part_length_x')::real, length_x+deadzone_x) 
																																						FROM grippers
																																						WHERE operation_type='transfer'
																																						LIMIT 1),
																													'part_length_y', (SELECT GREATEST((task->'part_length_y')::real, length_y+deadzone_y) 
																																						FROM grippers
																																						WHERE operation_type='transfer'
																																						LIMIT 1),
																													'part_thickness_z', task->'part_thickness_z')),
																													--TODO учитывать угол при вычитании координат или передалать всё на координаты угла
																													'previous_lay', COALESCE((SELECT jsonb_agg(jsonb_build_object(
																																													'part_id', id,
																																													'part_length_x', part_length_x,
																																													'part_length_y', part_length_y,
																																													'part_thickness_z', part_thickness_z,
																																													'part_pos_x', slot_pos_x,
																																													'part_pos_y', slot_pos_y,
																																													'part_pos_z', slot_pos_z,
																																													'part_angle_a', slot_angle_a
																																													))
																																													FROM parts p 
																																													WHERE p.part_status::text='part_slot'AND  part_slot=1),'[]') ,
																													'pallet_length_x', setting_get('slot1_length_x'),
																													'pallet_length_y', setting_get('slot1_length_y'),
																													'part_indent', setting_get('part_indent'),
																													'next_lay', FALSE,
																													'all_out', (task->'all_out')::bool
																													)) INTO jpacking_output;
					IF jpacking_output ? 'error' THEN
						RAISE EXCEPTION 'error "%" source "%" args "%"', jpacking_output->>'error' , jpacking_output->>'source' , jpacking_output->>'args'; 
					END IF;
					
					IF jsonb_array_length(jpacking_output->'current_lay')>0 THEN
						jslot:=jsonb_build_object('free_slot',1,
																			'slot_pos_x', jpacking_output->'current_lay'->0->'part_pos_x',
																			'slot_pos_y', jpacking_output->'current_lay'->0->'part_pos_y',
																			'slot_pos_z', jpacking_output->'current_lay'->0->'part_pos_z',
																			'slot_angle_a', jpacking_output->'current_lay'->0->'part_angle_a');
						--UPDATE parts SET  slot_pos_x=(jslot->'slot_pos_x')::real
						--								, slot_pos_y=(jslot->'slot_pos_y')::real
						--								, slot_pos_z=(jslot->'slot_pos_z')::real
						--								, slot_angle_a=(jslot->'slot_angle_a')::real
						--		WHERE id = (task->'part_id')::int8;
					END IF;
				END IF;
				
				IF jslot IS NULL THEN
					--slot 2-4
					jslot:=jsonb_build_object('free_slot', (SELECT CASE WHEN MAX(part_slot)>=setting_get('slot_count')::int4
																															THEN 0 
																															ELSE GREATEST(1,MAX(part_slot))+1 
																															END FROM parts)
																		,'slot_pos_x', 0, 'slot_pos_y', 0, 'slot_pos_z', 0, 'slot_angle_a', 0);
					--UPDATE parts SET slot_pos_x=0, slot_pos_y=0, slot_pos_z=0, slot_angle_a=0
					--		WHERE id = (task->'part_id')::int8;
				END IF;
				
				RETURN jslot;
				
		EXCEPTION WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
			err_context := SUBSTRING (err_context FROM '%function #"%%#" line%' FOR '#') || SUBSTRING ( err_context FROM '%[)]#" line ___#"%' FOR '#');
			RAISE INFO '%', SQLERRM;
			PERFORM write_error_log ( SQLERRM, '', err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
			
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
COMMENT ON FUNCTION get_slot IS 'Вычислить и записать в БД информацию о временном хранениии детали';  
END$$;

/*
SELECT get_slot('{"plc_id": 1, "plc_ip": "192.168.29.22", "part_id": 151, "order_id": 145, "plc_name": "BHX", "robot_id": 1, "part_side": "A", "part_slot": 0, "lay_number": 2, "part_pos_x": 346.50894, "part_pos_y": 67.23472, "part_pos_z": 0, "robot_name": "Feeder", "part_number": 142710010, "part_status": "pallet_in", "part_destination": "part_slot", "task_status": "not_sended", "part_angle_a": 131, "part_length_x": 568, "part_length_y": 382, "robot_task_id": 231, "gripper_sign_x": true, "gripper_sign_y": false, "not_sended2bhx": true, "operation_side": "A", "operation_type": "machining", "part_length_x0": 1000, "part_length_y0": 345, "operation_number": 1, "part_thickness_z": 16, "program_fullpath": "14271-10"}');
*/



/*
UPDATE parts SET label_angle_a=88 WHERE id>=142;
UPDATE parts SET --part_length_x=part_length_y, part_length_y=part_length_x, 
								label_pos_x=part_length_x/2.0-16, label_pos_y=part_length_y/2.0-15,label_angle_a=label_angle_a  WHERE id>=142;
UPDATE parts SET label_pos_x=label_pos_x+0, label_pos_y=label_pos_y-5 WHERE id>=142;

SELECT process_xyacodes('{"gen_robot_task": false, "robot_id": 1, "XYA_codes": [{"part_id": 151,  "scan_label_pos_x": 785, "scan_label_pos_y": 507, "scan_label_angle_a": 86, "scan_robot_pos_x": 0.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 2000.0, "scan_robot_angle_a": -180.0}, {"part_id": 146, "scan_label_pos_x": 1469, "scan_label_pos_y": 541, "scan_label_angle_a": 263, "scan_robot_pos_x": 0.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 2000.0, "scan_robot_angle_a": -180.0}], "scan_lay_number": 1, "plc_id": 1}');
SELECT gen_robot_task(0,1);
UPDATE robot_tasks SET task_status='done' WHERE part_id=146;
UPDATE parts SET part_status='part_slot' WHERE id=146;
SELECT gen_robot_task(146,1);
UPDATE robot_tasks SET task_status='done' WHERE part_id=151;
UPDATE parts SET part_status='part_slot' WHERE id=151;
SELECT gen_robot_task(151,1);

SELECT process_xyacodes('{"gen_robot_task": false, "robot_id": 1, "XYA_codes": [{"part_id": 145, "scan_label_pos_x": 805, "scan_label_pos_y": 563, "scan_label_angle_a": 266, "scan_robot_pos_x": 0.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 2000.0, "scan_robot_angle_a": -180.0}, {"part_id": 152, "scan_label_pos_x": 1431, "scan_label_pos_y": 453, "scan_label_angle_a": 89, "scan_robot_pos_x": 0.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 2000.0, "scan_robot_angle_a": -180.0}], "scan_lay_number": 2, "plc_id": 1}');

SELECT gen_robot_task(0,1);
UPDATE robot_tasks SET task_status='done' WHERE part_id=145;
UPDATE parts SET part_status='pallet_out' WHERE id=145;
SELECT gen_robot_task(145,1);
UPDATE robot_tasks SET task_status='done' WHERE part_id=152;
UPDATE parts SET part_status='pallet_out' WHERE id=152;
SELECT gen_robot_task(152,1);
SELECT gen_robot_task(0,1);

UPDATE parts SET part_status='pallet_out' WHERE id=146;
SELECT gen_robot_task(146,1);
UPDATE parts SET part_status='pallet_out' WHERE id=151;
SELECT gen_robot_task(151,1);


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
	jparts jsonb;
	jtask jsonb;
	jslotpart jsonb;
	id_robot int8;
	count_slot int4;
	upd_slot_part int4;
	slot_part int4;
	thickness_part_z int4;
	RESULT jsonb;
BEGIN
		json_data:=json_data-'part_slot'-'lay_number'-'pallet_length_x'-'pallet_length_y';
		
		
		--TODO проверять, если несколько толщин
		SELECT jsonb_agg(
											 jsonb_build_object('part_id', id
																				 ,'part_length_x', part_length_x
																				 ,'part_length_y', part_length_y
																				 ,'part_thickness_z', part_thickness_z)), part_thickness_z
					FROM parts
					WHERE json_data->'part_ids' @> to_jsonb(id)
					GROUP BY part_thickness_z
					INTO jparts, thickness_part_z;
					
		SELECT get_part_layers(jsonb_build_object('part_status', 'pallet_in','robot_id', id_robot))
				INTO jparts;

		id_robot:=(json_data->'robot_id')::int8;
		RAISE INFO 'jparts %', jparts;
		IF json_data->>'part_status'='part_slot' THEN
				SELECT slot_count FROM robots WHERE id=id_robot INTO count_slot;
				RAISE INFO 'count_slot %', count_slot;
				FOR slot_part IN 1..count_slot LOOP
						RAISE INFO 'slot_part %', slot_part;
						upd_slot_part:=slot_part;
						SELECT get_part_layers(jsonb_build_object('part_status', 'part_slot'
																															,'robot_id', id_robot
																															,'part_slot', slot_part))
								INTO jprevious_lay;
						jprevious_lay:=jprevious_lay->'part_layers';
						RAISE INFO 'jprevious_lay %', jprevious_lay;
						SELECT jsonb_build_object( 'parts', jparts
																			,'pallet_length_x', setting_get('slot_length_x')
																			,'pallet_length_y', setting_get('slot_length_y')
																			,'part_indent', setting_get('part_indent')
																			,'next_lay', ((COALESCE((jprevious_lay->0->'part_pos_z')::int4+
																														 (jprevious_lay->0->'part_thickness_z')::int4,0)+
																														 thickness_part_z) < setting_get('slot_thickness_z')::int4)
																			,'previous_lay',jprevious_lay)
							
							INTO jpacking_data;
						RAISE INFO 'jpacking_data %', jpacking_data;
						SELECT process_packing(jpacking_data) INTO jpacking_output;
						RAISE INFO 'jpacking_output %', jpacking_output;
						IF NOT (jpacking_output ? 'error') THEN
								EXIT;
						END IF;
				END LOOP;
		END IF;
		IF json_data->>'part_status'='pallet_out' THEN
						jprevious_lay:=get_part_layers(jsonb_build_object( 'part_status', 'pallet_out'
																															,'robot_id', id_robot));
						RAISE INFO 'jprevious_lay %', jprevious_lay;
						SELECT jsonb_build_object( 'parts', jparts
																			,'pallet_length_x', setting_get('pallet_length_x')
																			,'pallet_length_y', setting_get('pallet_length_y')
																			,'part_indent', setting_get('part_indent')
																			,'next_lay', TRUE
																			,'previous_lay',jprevious_lay)
							
							INTO jpacking_data;
						RAISE INFO 'jpacking_data %', jpacking_data;
						SELECT process_packing(jpacking_data) INTO jpacking_output;																															
						RAISE INFO 'jpacking_output %', jpacking_output;
		END IF;
						IF NOT (jpacking_output ? 'error') THEN
								RAISE INFO 'upd_slot_part %', upd_slot_part;
								UPDATE parts SET out_pos_x = p.part_pos_x
																	,out_pos_y = p.part_pos_y
																	,out_pos_a = p.part_angle_a
																	,part_slot = upd_slot_part
																	--,part_status = 'slot2out'
																	,lay_number = p.lay_number
													FROM (SELECT * FROM jsonb_array_elements(jpacking_output->'current_lay') a(parts)
																						, jsonb_to_record (parts) AS x ( 
																																						part_id int8,
																																						lay_number int2,
																																						part_pos_x real,
																																						part_pos_y real,
																																						part_angle_a real)
																	WHERE json_data->'part_ids' @> to_jsonb(part_id)
																	LIMIT 1																						
																) AS p
														WHERE id=p.part_id
													RETURNING jsonb_build_object('result','OK'
																											,'filling',jpacking_output->'filling'
																											,'part_id',id
																											,'part_pos_x',slot_pos_x
																											,'part_pos_y',slot_pos_y
																											,'part_angle_a',slot_angle_a)
													INTO jslotpart;
											RAISE INFO 'jslotpart %', jslotpart;
								--TODO проверка (захвата из слота?) и укладки
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
								SELECT gen_robot_task((jslotpart->'part_id')::int8,id_robot) INTO jtask;
								RESULT := jsonb_build_object('result','OK','robot_task',jtask);
								EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'pack_parts',	json_data);
								RETURN RESULT;
						ELSE
								RETURN jsonb_build_object('result','NOK','error', 'NO PLACE FOR TRANSFER');						
						END IF;
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


DO $$ --get_part_layers 
BEGIN
DROP FUNCTION IF EXISTS get_part_layers(text,int8,int4,int4,int4,int4);
CREATE OR REPLACE FUNCTION get_part_layers(status_part text, id_robot int8, number_lay int4=0,pallet_length_x int4=NULL,pallet_length_y int4=NULL, slot_part int4=0)
RETURNS jsonb
AS $BODY$
DECLARE
	err_context text;
	part_layers jsonb;
	RESULT jsonb;
BEGIN
		SELECT jsonb_AGG(
									 jsonb_build_object( 'part_id',id
																			,'part_length_x',part_length_x
																			,'part_length_y',part_length_y
																			,'part_thickness_z', part_thickness_z
																			,'part_pos_x',part_pos_x + COALESCE(pallet_length_x,setting_get('pallet_length_x')::int4)
																			,'part_pos_y',part_pos_y + COALESCE(pallet_length_y,setting_get('pallet_length_y')::int4)
																			,'part_pos_z',part_pos_z
																			,'part_angle_a',part_angle_a
																			,'lay_number',lay_number
																			))
																													FROM parts 
																			WHERE part_status=status_part::type_part_status and robot_id=id_robot 
																						and (lay_number=number_lay OR number_lay=0)
																						and (part_slot=slot_part OR slot_part=0)
		INTO part_layers;

		RESULT := jsonb_build_object('result','OK','part_layers',COALESCE(part_layers,'[]'));
		EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'get_part_layers',	'');
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
SELECT get_part_layers('pallet_out',1,0);
*/

DO $$ --packing_test 
BEGIN
DROP FUNCTION IF EXISTS packing_test;
CREATE OR REPLACE FUNCTION packing_test(id_robot int8)
RETURNS jsonb
AS $BODY$
DECLARE
	err_context text;
	next_lay_number int2;
	jpacking_data jsonb;
	jnew_lay_data jsonb;
	jpacking_output jsonb;
	RESULT jsonb;
BEGIN
		PERFORM setting_set('min_lay_filling',to_jsonb(0));
		PERFORM setting_set('min_lay_parts',to_jsonb(1));
		WHILE (SELECT COUNT(*)>0 FROM parts WHERE part_status='part_slot' AND robot_id=id_robot) LOOP
				SELECT check_new_lay(id_robot) INTO jnew_lay_data;
				IF jnew_lay_data ? 'error' THEN
					RAISE EXCEPTION 'error "%" source "%"', jnew_lay_data->>'error' , jnew_lay_data->>'source'; 
				END IF;
				SELECT COALESCE(MAX(lay_number),0)+1 FROM parts WHERE part_status='pallet_out' AND robot_id=id_robot INTO next_lay_number;
				UPDATE parts SET part_status = 'pallet_out',
												 lay_number = next_lay_number
							WHERE part_status = 'slot2out' AND robot_id=id_robot;
		END LOOP;
		PERFORM setting_set('min_lay_filling',to_jsonb(75));
		PERFORM setting_set('min_lay_parts',to_jsonb(3));

		RESULT := jsonb_build_object('result','OK','filling',jpacking_output->'filling');
		EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'packing_test',	'');
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
COMMENT ON FUNCTION packing_test IS 'Проверка алгоритма укладки слоёв';	
END$$;

/*
SELECT packing_test(1);
*/



DO $$ --view_next_tasks
BEGIN
  DROP VIEW IF EXISTS view_next_tasks;
	CREATE OR REPLACE VIEW view_next_tasks AS
		SELECT rtp.id, rtp.part_id, rtp.robot_id, CASE WHEN pr.part_status='slot2out' THEN 'not_sended' ELSE rtp.task_status END AS task_status
																						, CASE WHEN pr.part_status='slot2out' THEN 'transfer_slot2out' ELSE rtp.operation_type END AS operation_type
					, rtp.operation_number, rtp.operation_content
					, rtp.operation_side , rtp.robot_name, rtp.plc_name, rtp.ip AS plc_ip, rtp.plc_id, rtp.next_part_number, rtp.part_number, rtp.operations_count
					, to_jsonb(pr.*)-'id'-'external_id'-'label_pos_x'-'label_pos_y'-'label_angle_a'
													-'operation_type' - 'robot_id' || jsonb_build_object('part_id',pr.id) AS part
					,to_jsonb(g.*)-'id'-'robot_id'-'operation_type'-'length_x'-'length_y'-'deadzone_x'-'deadzone_y'
					|| jsonb_build_object('part_length_x0',g.length_x+2*g.deadzone_x)
					|| jsonb_build_object('part_length_y0',g.length_y+2*g.deadzone_y)
					/* || jsonb_build_object('gripper_id',pr.id) */ 
					AS gripper
					--, g.length_x+2*g.deadzone_x AS part_length_x0, g.length_y+2*g.deadzone_y AS part_length_y0
					--, g.gripper_sign_x
					--, g.gripper_sign_y
			FROM (SELECT
									--, p.sector_id, p.machine_type
									rt.*, r.name as robot_name, p.name as plc_name, p.ip, p.id AS plc_id, MIN(rt.operation_number) FILTER(WHERE rt.task_status!='done' OR (pa.part_status='slot2out' AND rt.operation_type!='transfer_in2flip')) OVER(PARTITION BY rt.part_id) AS current_operation_number
									, MIN(rt.part_number) FILTER(WHERE (task_status!='done' OR (pa.part_status='slot2out' AND rt.operation_type!='transfer_in2flip')) AND (
													(rt.operation_type='scanning')
											 OR (rt.operation_type='machining' AND pa.part_status='pallet_in')
											 OR (rt.operation_type='machining' AND pa.part_status='slot2out')
											 OR (rt.operation_type='machining' AND pa.part_status='flip_table')
											 OR (rt.operation_type='transfer_in2flip' AND pa.part_status='pallet_in')
											 OR (rt.operation_type='transfer_in2out' AND pa.part_status='pallet_in')
											 OR (rt.operation_type='transfer_in2slot' AND pa.part_status='pallet_in')
											 OR (rt.operation_type='transfer_slot2out' AND pa.part_status='part_slot'
														AND (SELECT COUNT(*)=0 FROM parts WHERE part_status!='done' AND part_status!='ordered' 
																																		AND part_status!='part_slot'
																																		--AND part_status!='slot2out' 
																																		AND id!=rt.part_id 
																																		AND lay_number=pa.lay_number)
																																								)
																								)) OVER(PARTITION BY rt.operation_type) AS next_part_number
									, COUNT(*) FILTER(WHERE rt.task_status!='done' AND NOT (rt.operation_type::text LIKE 'transfer%')) OVER(PARTITION BY rt.part_id) AS operations_count
									--, CASE WHEN rt.task_status='done' AND pa.part_status='part_slot' 
									--						AND rt.operation_type = pa.operation_type 
									--						AND (SELECT COUNT(*)=0 FROM parts WHERE part_status!='done' AND part_status!='ordered' 
									--																										AND part_status!='part_slot' AND id!=rt.part_id)
									--																																 THEN 'transfer_slot2out'
									--			 WHEN rt.task_status='done' AND rt.part_status='pallet_in' THEN 'transfer_in2out'
									--	ELSE rt.operation_type
									--	END AS op_type
									
									--,(SELECT COUNT(*)=0 FROM parts WHERE part_status!='done' AND part_status!='ordered' AND id!=rt.part_id) AS other_done
							FROM robot_tasks rt
							JOIN robots r ON r.id=rt.robot_id
							JOIN plcs p ON p.id=r.plc_id
							JOIN parts pa ON pa.id=rt.part_id
							WHERE (rt.task_status != 'done' AND pa.part_status='pallet_in') 
										OR pa.part_status='part_slot' 
										OR pa.part_status='slot2out' 
										OR pa.part_status='flip_table'
							) rtp
			JOIN parts pr ON pr.id=rtp.part_id
			JOIN grippers g ON g.id=rtp.gripper_id
		WHERE operation_number=current_operation_number
		ORDER BY rtp.id;
	COMMENT ON VIEW view_next_tasks IS 'Следующие задачи для роботов';
END $$;

/*
SELECT * FROM view_next_tasks
*/

DO $$ --get_robot_tasks
BEGIN
	DROP FUNCTION IF EXISTS get_robot_tasks;
	CREATE OR REPLACE FUNCTION get_robot_tasks (id_part int8 = -1, id_plc int8 = -1, number_lay int4 = -1) 
	RETURNS SETOF jsonb AS $BODY$
	DECLARE
		task jsonb;
		jslot jsonb;
		err_context text;
	BEGIN
		FOR task IN (SELECT
									(jsonb_build_object (
										'not_sended2bhx',
										vnt.task_status IN ('not_sended'),
										'robot_task_id',
										vnt.id,
										'robot_name',
										vnt.robot_name,
										'plc_id',
										vnt.plc_id,
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
										'operation_number',
										vnt.operation_number,
										'part_destination',
										CASE WHEN vnt.operations_count>1
												 THEN 'flip_table'
												 WHEN (vnt.operations_count=1 AND (part->'part_length_x')::real>1400 AND (part->'part_length_y')::real>400) 
																OR (vnt.part->>'part_status')='slot2out'
												 THEN 'pallet_out'
												 ELSE 'part_slot'
													END,
										'free_slot',
										(SELECT CASE WHEN (MAX(part_slot)+1)>10 
																 THEN 0 
																 ELSE (MAX(part_slot)+1) END FROM parts)
										--,'w/o_machine_mode',
										--vnt.operation_type IN ('transfer_in2out','transfer_in2slot','transfer_slot2out')
										) 
										|| vnt.operation_content 
										|| vnt.part
										|| vnt.gripper
													) AS task
						FROM view_next_tasks vnt
							WHERE ((part_id=id_part OR (id_part=-1 OR id_part=0
																--AND vnt.task_status!='not_sended'
							)) AND
											 ((vnt.part->'lay_number')::int2=number_lay OR number_lay=-1)
											AND (plc_id=id_plc OR (id_plc=-1 AND vnt.task_status!='not_sended')))
										 /*vnt.operation_type='transfer_in2out' AND */ 
										 AND next_part_number=part_number
										 AND ( (vnt.part->>'part_status')!='part_slot' 
														OR vnt.operation_type='machining'
														OR (id_part=0 AND
																(vnt.part->'lay_number')::int2!=1)
														OR (id_part=0 AND
															(SELECT COUNT(*)=0
																	FROM parts p
																	WHERE part_status!='done' 
																				AND part_status!='ordered' 
																				AND part_status!='part_slot') ) )
										 AND (vnt.part->>'part_status') IN ('pallet_in','part_slot','slot2out','flip_table')
							ORDER BY (vnt.part->>'part_status')='flip_table' DESC, (vnt.part->>'part_status')='slot2out' DESC, (vnt.part->>'part_status')='pallet_in' DESC, vnt.part->>'part_slot' DESC
						) LOOP
				
				--task:=task||jsonb_build_object('all_out',id_part=-1);
				IF (setting_get('layout_mode'))::int=0 THEN
					jslot:=jsonb_build_object('slot_pos_x', task->'part_pos_x', 'slot_pos_y', task->'part_pos_y', 'slot_pos_z', task->'part_pos_z', 'slot_angle_a', task->'part_angle_a');
					IF (task->'lay_number')::int=1 THEN
						task:=task||jsonb_build_object('part_destination','part_slot','free_slot',1);
					ELSE
						task:=task||jsonb_build_object('part_destination','pallet_out');
					END IF;
				ELSE
					jslot:=get_slot(task);
					IF jslot ? 'error' THEN
							RAISE EXCEPTION 'error "%" source "%" task "%"', jslot->>'error' , jslot->>'source' , task; 
					END IF;
				END IF;
				task:=task||jslot;
				SELECT task || jsonb_build_object('robot_id',id) FROM robots WHERE plc_id=(task->'plc_id')::int8 INTO task;
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
SELECT get_robot_tasks(0,1,1);
SELECT get_robot_tasks(3);
SELECT get_robot_tasks(1,1);
SELECT get_robot_tasks(3,1);
SELECT get_robot_tasks(146,1);
SELECT get_robot_tasks(150,1);
SELECT get_robot_tasks(158,1);
SELECT get_robot_tasks(-1,1,1);
SELECT get_robot_tasks(-1,1);
*/


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
$BODY$
	LANGUAGE plpython3u
	COST 100;
COMMENT ON FUNCTION process_packing IS 'Упаковка слоя';	
END$$;

/*
SELECT debug_off();
SELECT debug_on();
SELECT process_packing('{"parts": [{"part_id": 145, "part_slot": 0, "part_length_x": 600, "part_length_y": 400, "part_thickness_z": 16}, {"part_id": 146, "part_slot": 0, "part_length_x": 600, "part_length_y": 400, "part_thickness_z": 16}, {"part_id": 159, "part_slot": 0, "part_length_x": 548, "part_length_y": 240, "part_thickness_z": 16}, {"part_id": 181, "part_slot": 0, "part_length_x": 350, "part_length_y": 350, "part_thickness_z": 16}, {"part_id": 182, "part_slot": 0, "part_length_x": 350, "part_length_y": 350, "part_thickness_z": 16}, {"part_id": 183, "part_slot": 0, "part_length_x": 350, "part_length_y": 350, "part_thickness_z": 16}, {"part_id": 184, "part_slot": 0, "part_length_x": 350, "part_length_y": 350, "part_thickness_z": 16}, {"part_id": 142, "part_slot": 0, "part_length_x": 600, "part_length_y": 568, "part_thickness_z": 16}, {"part_id": 6, "part_slot": 0, "part_length_x": 450, "part_length_y": 300, "part_thickness_z": 16}, {"part_id": 5, "part_slot": 0, "part_length_x": 300, "part_length_y": 450, "part_thickness_z": 16}], "part_indent": 10, "previous_lay": [], "pallet_length_x": 800, "pallet_length_y": 1200}');

*/


/*DO $$ --check_rack 
BEGIN
DROP FUNCTION IF EXISTS check_rack;
CREATE OR REPLACE FUNCTION check_rack()
RETURNS jsonb
AS $BODY$
DECLARE
	err_context text;
	jpacking_data jsonb;
	jpacking_output jsonb;
	RESULT jsonb;
BEGIN
	IF (SELECT COUNT(*)>=(setting_get('min_lay_parts')::int2) FROM parts WHERE part_status='part_slot') THEN
			SELECT jsonb_build_object('parts',(SELECT jsonb_AGG(
									 jsonb_build_object('part_id',id,
																			'part_length_x',part_length_x,
																			'part_length_y',part_length_y,
																			'part_thickness_z', part_thickness_z,
																			'part_slot', part_slot)) FROM (SELECT * FROM parts WHERE part_status='part_slot') p)
													,'previous_lay', (SELECT jsonb_AGG(
									 jsonb_build_object('part_id',id,
																			'part_length_x',part_length_x,
																			'part_length_y',part_length_y,
																			'part_thickness_z', part_thickness_z)) FROM (SELECT * FROM parts WHERE part_status='pallet_out') p)
													,'pallet_length_x', setting_get('pallet_length_x')
													,'pallet_length_y', setting_get('pallet_length_y')
													,'part_indent', setting_get('part_indent')
																)
													INTO jpacking_data;
			SELECT process_packing(jpacking_data) INTO jpacking_output;
			IF ((SELECT COUNT(*)>=(setting_get('slot_count'))::int4 FROM parts WHERE part_status='part_slot')
									OR (jpacking_output->'filling')::int2>(setting_get('min_lay_filling'))::int4) THEN
					UPDATE parts SET part_pos_x = p.part_pos_x
													,part_pos_y = p.part_pos_y
													,part_angle_a = p.part_angle_a
													,part_status = 'slot2out'
													,lay_number = -1 
													--(SELECT MAX(lay_number) FROM parts WHERE )
									FROM (SELECT * FROM jsonb_array_elements(jpacking_output->'parts') a(parts)
																		, jsonb_to_record (parts) AS x ( 
																																		part_id int8,
																																		part_pos_x real,
																																		part_pos_y real,
																																		part_angle_a real)
																																		) AS p
									WHERE id=p.part_id;
			END IF; 
	END IF; 
		RESULT := jsonb_build_object('result','OK','filling',jpacking_output->'filling');
		EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'check_rack',	'');
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
COMMENT ON FUNCTION check_rack IS 'Проверка стеллажа, можно ли выложить слой';	
END$$;
*/

/*
SELECT check_rack();
*/

DO $$ --set_gripping_order
BEGIN
CREATE OR REPLACE FUNCTION set_gripping_order(json_data jsonb)
  RETURNS jsonb AS $BODY$ 
    DECLARE 
			err_context text;
			jtask jsonb;
			rtask record;
			part_id int8;
			lay record;
			RESULT jsonb;
    BEGIN

			FOR lay IN (SELECT	(part->'part_id')::int8 AS part_id
												, (part->'gripper_id')::int8 AS gripper_id
												, (part->>'operation_type')::type_task_type AS operation_type
												, (part->>'gripper_type')::type_task_type AS gripper_type 
												, (part->>'lay_number')::int4 AS lay_number 
												, idx
											FROM jsonb_array_elements(json_data) WITH ORDINALITY o(part,idx)) LOOP
											
								UPDATE robot_tasks rt SET part_number=lay.idx
									WHERE rt.part_id = lay.part_id AND rt.part_number=0 ;
									--AND task_status!='done' AND operation_type=lay.operation_type;
						IF (SELECT COUNT(*)!=0 FROM robot_tasks rt 
									WHERE rt.part_id = lay.part_id AND task_status!='done'
												AND operation_type=lay.operation_type) AND lay.operation_type!='transfer_in2out' THEN
												
												
								IF lay.operation_type!=lay.gripper_type 
										AND (SELECT COUNT(*)=0 FROM robot_tasks rt 
																WHERE rt.part_id = lay.part_id AND task_status!='done'
																			AND operation_type='transfer_in2flip') THEN
								--	UPDATE robot_tasks rt SET part_number=lay.idx, gripper_id=lay.gripper_id
								--		WHERE rt.part_id = lay.part_id AND operation_type=lay.operation_type;
								--ELSE
		--								ORDER BY operation_type=lay.operation_type DESC LIMIT 1
								--	RETURNING rt.* INTO rtask;
									--IF NOT rtask IS NULL THEN
									--	IF NOT rtask.part_id IS NULL THEN
											SELECT * FROM robot_tasks rt
												WHERE rt.part_id = lay.part_id AND operation_type=lay.operation_type
												ORDER BY operation_type=lay.operation_type DESC, task_status!='done' DESC, operation_number LIMIT 1
											INTO rtask;
											UPDATE robot_tasks rt SET operation_number=operation_number+1
												WHERE rt.part_id = lay.part_id AND operation_number>=rtask.operation_number;
											rtask.operation_type:='transfer_in2flip';
											rtask.gripper_id:=lay.gripper_id;
											rtask.part_number:=lay.idx;
											INSERT INTO robot_tasks(part_id, robot_id, operation_type, operation_number, operation_content, operation_side, task_status, part_number, gripper_id) VALUES(rtask.part_id, rtask.robot_id, rtask.operation_type, rtask.operation_number, rtask.operation_content, rtask.operation_side, 'not_sended', rtask.part_number, rtask.gripper_id);
									--	END IF;
									--END IF;
								END IF;
								
						ELSE
								IF (SELECT COUNT(*)=0 FROM robot_tasks rt 
																WHERE rt.part_id = lay.part_id AND task_status!='done'
																			AND operation_type IN ('transfer_in2out','transfer_in2slot')) THEN
									SELECT * FROM robot_tasks rt 
										WHERE rt.part_id = lay.part_id --AND operation_type=lay.operation_type
										ORDER BY operation_type=lay.operation_type DESC, operation_number LIMIT 1
									INTO rtask;
										UPDATE robot_tasks rt SET operation_number=operation_number+1
											WHERE rt.part_id = lay.part_id AND operation_number>rtask.operation_number;
											IF lay.lay_number=1 AND lay.operation_type!='transfer_in2out' THEN
												rtask.operation_type:='transfer_in2slot';
											ELSE
												rtask.operation_type:='transfer_in2out';
											END IF;
										rtask.operation_number:=rtask.operation_number+1;
										rtask.part_number:=lay.idx;
										INSERT INTO robot_tasks(part_id, robot_id, operation_type, operation_number, operation_content, operation_side, task_status, part_number, gripper_id) VALUES(rtask.part_id, rtask.robot_id, rtask.operation_type, rtask.operation_number, rtask.operation_content, rtask.operation_side, 'not_sended', rtask.part_number, lay.gripper_id);
								END IF;
						END IF;
						
								IF lay.lay_number=1 
										AND (SELECT COUNT(*)=0 FROM robot_tasks rt 
																WHERE rt.part_id = lay.part_id AND task_status!='done'
																			AND operation_type IN ('transfer_in2out')) THEN --,'transfer_in2slot'
									SELECT * FROM robot_tasks rt 
										WHERE rt.part_id = lay.part_id --AND operation_type=lay.operation_type
										ORDER BY operation_type=lay.operation_type DESC, task_status!='done' DESC, operation_number LIMIT 1
									INTO rtask;
									--IF NOT rtask IS NULL THEN
										UPDATE robot_tasks rt SET operation_number=rt.operation_number+1
											WHERE rt.part_id = lay.part_id AND rt.operation_number>rtask.operation_number;
										rtask.operation_type:='transfer_slot2out';
										rtask.gripper_id:=lay.gripper_id;
										rtask.operation_number:=rtask.operation_number+1;
										rtask.part_number:=lay.idx;
										INSERT INTO robot_tasks(part_id, robot_id, operation_type, operation_number, operation_content, operation_side, task_status, part_number, gripper_id) VALUES(rtask.part_id, rtask.robot_id, rtask.operation_type, rtask.operation_number, rtask.operation_content, rtask.operation_side, 'not_sended', rtask.part_number, lay.gripper_id);
									--END IF;
								END IF;

			END LOOP;



			--UPDATE robot_tasks rt SET part_number=idx, gripper_id=(part->'gripper_id')::int8
			--	FROM jsonb_array_elements(json_data) WITH ORDINALITY o(part,idx)
			--	WHERE rt.part_id = (part->'part_id')::int8 AND operation_type=(part->>'operation_type')::type_task_type;
				
			SELECT get_robot_tasks((json_data->0->'part_id')::int8,(json_data->0->'plc_id')::int8) INTO jtask;
			--IF NOT task IS NULL THEN
			--	PERFORM pg_notify('robot_task',task::text);
			--END IF;

			RESULT := jtask;
			EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'set_gripping_order',	json_data);

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
	COMMENT ON FUNCTION set_gripping_order IS 'Установить статус задания робота';
END$$;
				
/*
SELECT set_gripping_order('[{"plc_id": 1, "part_id": 4, "gripper_id": 4, "operation_type": "machining"}, {"plc_id": 1, "part_id": 3, "gripper_id": 4, "operation_type": "machining"}]');

SELECT set_gripping_order('[{"plc_id": 1, "part_id": 3, "gripper_id": 3, "lay_number": 1, "gripper_type": "transfer", "operation_type": "transfer_in2out"}, {"plc_id": 1, "part_id": 4, "gripper_id": 3, "lay_number": 1, "gripper_type": "transfer", "operation_type": "transfer_in2out"}]');
*/

				
/*			
DO $$ --set_robot_task_status
BEGIN
	DROP FUNCTION IF EXISTS set_robot_task_status( int8, type_task_status );
	CREATE OR REPLACE FUNCTION set_robot_task_status (robot_task_id int8, status_task type_task_status) 
		RETURNS jsonb AS $BODY$ 
		DECLARE 
			res jsonb;
		BEGIN
			UPDATE robot_tasks SET task_status=status_task WHERE id=robot_task_id RETURNING jsonb_build_object('result','OK') INTO res;
			RETURN COALESCE(res,jsonb_build_object('error',format('Не найден robot_task_id %s', robot_task_id)));
		END;
	$BODY$ LANGUAGE plpgsql VOLATILE;
	COMMENT ON FUNCTION set_robot_task_status ( int8, type_task_status ) IS 'Установить статус задания робота';
	
END $$;			
*/
			
/*
SELECT set_robot_task_status(0,'unknown');
*/

/*			
DO $$ --set_robot_task_status
BEGIN
DROP FUNCTION IF EXISTS set_robot_task_status;
CREATE OR REPLACE FUNCTION set_robot_task_status(arg jsonb)
  RETURNS jsonb TRANSFORM FOR TYPE jsonb
AS $BODY$
# TRANSFORM FOR TYPE jsonb
import json
import traceback
import simplejson
try:
	arg=json.loads(simplejson.dumps(args[0], use_decimal=True))
	DEBUG = True
	DEBUG = plpy.execute('SELECT is_debug()')[0]['is_debug']
	assert isinstance(arg, dict),'Проверь формат JSON'
	assert 'robot_task_id' in arg,'Нужен параметр robot_task_id'
	assert 'task_status' in arg, 'Нужен параметр task_status'
	query=(f'UPDATE robot_tasks SET task_status=\'{arg["task_status"]}\' WHERE id={arg["robot_task_id"]} RETURNING id')
	if DEBUG: plpy.info(query)	
	res=plpy.execute(query)
	assert res, f'Ошибка в запросе "{query}" '
	return dict(result='OK')
except Exception as e:
	source=traceback.format_exc().splitlines()[1].split(', ')
	source=source[1]+' '+'_'.join(source[2].split('_')[slice(4,-1)])+': '+str(e)
	err_arg = [source,json.dumps(args,ensure_ascii=False,default=int)]
	res = plpy.execute(plpy.prepare(f'SELECT write_error_log($1,$2)', ['text','text']), err_arg)
	return dict(source=err_arg[0],error=str(e),args=args)
$BODY$
  LANGUAGE plpython3u VOLATILE
  COST 100;
COMMENT ON FUNCTION set_robot_task_status IS 'Установить статус задания робота';
END$$;
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
	--scan_pos_x real;
	--scan_pos_y real;
	--robot_pos record;
	robot_pos_z real;
BEGIN
	SELECT jsonb_path_query_array(json_data->'XYA_codes','$[*] ? (@.type()=="object")') INTO XYA_codes;
	IF (jsonb_array_length(XYA_codes)>0) THEN
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
		robot_pos_z:=(json_data->'XYA_codes'->0->'scan_robot_pos_z')::real;
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
		--SELECT 2*((1000 + 8) * tan(radians(34/2.0)))/1920,2*((1000 + 8) * tan(radians(19.537/2.0)))/1080;
		--scaner_scale_x:=0,3302528298673238;scaner_scale_y:=	0,3213597881804298;
		--SELECT 1427::int2-1920/2.0
		--select 2*((1523) * tan(radians(33.6/2.0)))/1920*(306) ,    2*((1523 ) * tan(radians(19.337/2.0)))/1080*(303) ;

		scaner_scale_x:=scaner_FOVx/scaner_BOXx;
		scaner_scale_y:=scaner_FOVy/scaner_BOXy;
		jscale:=jsonb_build_object('scaner_scale_x',scaner_scale_x,'scaner_scale_y',scaner_scale_y);
		--scan_pos_x=-scaner_FOVx/2.0;
		--scan_pos_y=-scaner_FOVy/2.0;
		
		WITH lay AS (
					UPDATE parts SET part_pos_x = scan_robot_pos_x + (p.x * p.scan_cos_a - p.y * p.scan_sin_a) - (p.dx * p.cos_theta - p.dy * p.sin_theta)
													,part_pos_y = scan_robot_pos_y + (p.x * p.scan_sin_a + p.y * p.scan_cos_a) - (p.dx * p.sin_theta + p.dy * p.cos_theta)
													,part_angle_a = p.angle_a
													,part_status='pallet_in'
													,robot_id=COALESCE(p.robot_id,1)
													,operation_type=(SELECT operation_type FROM plcs WHERE id = (json_data->'plc_id')::int8)
													,lay_number=(json_data->'scan_lay_number')::int2
							FROM 		(SELECT 	 x.part_id
															 , x.robot_id
															 , (x.scan_label_pos_x-scaner_BOXx/2.0)*x.scaner_scale_x x
															 , (scaner_BOXy/2.0-x.scan_label_pos_y)*x.scaner_scale_y y
															 , (pa.label_pos_x - pa.part_length_x/2.0) dx
															 , (pa.label_pos_y - pa.part_length_y/2.0) dy
															 , cos(radians(x.scan_label_angle_a-pa.label_angle_a)) cos_theta
															 , sin(radians(x.scan_label_angle_a-pa.label_angle_a)) sin_theta
															 , cos(radians(x.scan_robot_angle_a)) scan_cos_a
															 , sin(radians(x.scan_robot_angle_a)) scan_sin_a
															 , x.scan_label_angle_a-pa.label_angle_a angle_a
															 , x.scan_robot_pos_x
															 , x.scan_robot_pos_y
																FROM jsonb_array_elements(XYA_codes) a(xya_code)
																	 , jsonb_to_record ( xya_code || jscale ) AS x ( 
																																part_id int8,
																																robot_id int8,
																																scan_label_pos_x int2,
																																scan_label_pos_y int2,
																																scan_label_angle_a int2,
																																scan_robot_pos_x real,
																																scan_robot_pos_y real,
																																scan_robot_pos_z real,
																																scan_robot_angle_a real,
																																scaner_scale_x real,
																																scaner_scale_y real)
																JOIN parts pa ON pa.id=x.part_id
																																) AS p
																																
							WHERE id=p.part_id
							RETURNING id)
				SELECT COALESCE(jsonb_agg(id),'[]') FROM (SELECT id FROM lay) lay_parts
					INTO jparts;
				--Clean robot_tasks
				UPDATE robot_tasks SET part_number=0 WHERE to_jsonb(part_id) <@ jparts;
				DELETE FROM robot_tasks WHERE to_jsonb(part_id) <@ jparts AND (operation_type::text LIKE 'transfer%');
				--SELECT jsonb_object_agg(operation_type, grippers) FROM ( SELECT operation_type,
					SELECT jsonb_agg(to_jsonb(g.*)-'id'-'robot_id'-'operation_type'
																								|| jsonb_build_object('gripper_id',g.id)
																								|| jsonb_build_object('gripper_type',g.operation_type)
																								ORDER BY operation_type='machining' DESC
																								) grippers
									FROM grippers g 
										JOIN robots r ON r.id=g.robot_id
									WHERE r.plc_id = (json_data->'plc_id')::int8 --TODO robot_id
												AND operation_type!='scanning'
									--GROUP BY operation_type
									--ORDER BY operation_type='machining' DESC
									--) gs
					INTO jgrippers;		
					RAISE INFO 'jgrippers %',jgrippers;
				IF jgrippers IS NULL THEN
					RAISE EXCEPTION 'jgrippers IS NULL, plc_id %', json_data->'plc_id'; 
				END IF;
				SELECT jsonb_agg(jsonb_build_object('grippers', jgrippers --COALESCE(jgrippers->(t.task->>'operation_type'), jgrippers->'transfer')
																					, 'part_id', pa.id
																					, 'part_pos_x', pa.part_pos_x
																					, 'part_pos_y', pa.part_pos_y
																					, 'part_length_x', pa.part_length_x
																					, 'part_length_y', pa.part_length_y
																					, 'part_thickness_z', pa.part_thickness_z
																					, 'part_angle_a', pa.part_angle_a
																					, 'lay_number', pa.lay_number
																					, 'plc_id', (json_data->'plc_id')::int8
																					, 'operation_type', COALESCE(t.task->>'operation_type','transfer_in2slot')
																						)
												)
											FROM (SELECT part_id::int8, (SELECT get_robot_tasks(part_id::int8,(json_data->'plc_id')::int8)) task
																	FROM jsonb_array_elements(jparts) p(part_id)) t
											JOIN parts pa ON pa.id=t.part_id
					INTO jgripping_data;
					RAISE INFO 'jgripping_data %',jgripping_data;
				IF jgripping_data IS NULL THEN
					RAISE EXCEPTION 'jgripping_data IS NULL, jgrippers %', jgrippers::text; 
				END IF;
				SELECT process_gripping(jgripping_data) INTO jgripping_order;
					RAISE INFO 'jgripping_order %',jgripping_order;
				IF jgripping_order ? 'error' THEN
					--RAISE EXCEPTION 'error "%" source "%" gripping_data "%"', jgripping_order->>'error' , jgripping_order->>'source' , jgripping_data::text;
					RAISE EXCEPTION 'error "%" source "%"', jgripping_order->>'error' , jgripping_order->>'source'; 
				END IF;
				IF jgripping_order IS NULL THEN
					RAISE EXCEPTION 'jgripping_order IS NULL, jgripping_data %', jgripping_data::text; 
				END IF;
				SELECT set_gripping_order(jgripping_order) INTO jtask;
				IF jtask ? 'error' THEN
					--RAISE EXCEPTION 'error "%" source "%" gripping_data "%"', jtask->>'error' , jtask->>'source', jgripping_order::text; 
					RAISE EXCEPTION 'error "%" source "%"', jtask->>'error' , jtask->>'source'; 
				END IF;
				
				RESULT := jsonb_build_object('result', 'OK', 'part_ids', jparts, 'gripping_data', jgripping_data, 'gripping_order', jgripping_order);
			ELSE
				SELECT get_robot_tasks(-1,(json_data->'plc_id')::int8) INTO jtask;
				RESULT := jsonb_build_object('result', 'OK','robot_task', jtask);
			END IF;
			IF ((json_data->'gen_robot_task')::bool) AND (NOT (jtask IS NULL)) THEN
				--RESULT := RESULT || jsonb_build_object('robot_task', jtask);
				PERFORM pg_notify('robot_task',jtask::text);
			END IF;
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

SELECT process_xyacodes('{"gen_robot_task": true, "XYA_codes": ["Failure"], "scan_lay_number": 1, "scan_robot_pos_x": 0.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 1000.0, "scan_robot_angle_a": -180.0, "plc_id": 1}');

*/

/*
--qr_scanned upper_layer

DO $$ --process_measuring
BEGIN
DROP FUNCTION IF EXISTS process_measuring;
CREATE OR REPLACE FUNCTION process_measuring(json_data jsonb)
RETURNS jsonb
AS $BODY$
DECLARE
  err_context text;
  jpart_ids jsonb;
  task jsonb;
  tasks_arr jsonb;
BEGIN
	WITH p AS ( 
				UPDATE parts SET part_pos_x = pxya.x - label_pos_x + part_length_x/2.0,
												 part_pos_y = pxya.y - label_pos_y + part_length_y/2.0,
												 part_angle_a = pxya.a + label_angle_a,
												 part_status='pallet_in'
						FROM 		(SELECT * FROM jsonb_array_elements(XYA_codes) a(xya_code), jsonb_to_record ( xya_code ) AS x ( 
																															part_id int8,
																															x int2,
																															y int2,
																															a int2) ) AS pxya
						WHERE id=pxya.part_id
						RETURNING id)
			SELECT jsonb_build_object ('part_ids', jsonb_agg(p.id)) 
				FROM p
				INTO jpart_ids;
		PERFORM write_info_log(COALESCE ( jpart_ids, '{}' )::text,json_data::text);
		
		FOR task IN (SELECT get_robot_tasks(-1,id_robot) LOOP
			PERFORM pg_notify('robot_task',task::text);
			tasks_arr:=COALESCE(tasks_arr,'[]')||task;
		END LOOP;
		RETURN jsonb_build_object('result','OK','tasks',tasks_arr, 'part_ids',jpart_ids);
		EXCEPTION 
			WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
			RAISE INFO '%',
			SQLERRM;
			PERFORM write_error_log ( SQLERRM, json_data :: TEXT, err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
COMMENT ON FUNCTION process_measuring IS 'Генерация нового задания по QR коду';  
END$$;
*/
/*
SELECT process_measuring('{"XYA_codes":[{"part_id":1,"x":100,"y":100,"a":10}]}');
*/

/*
DO $$ --process_qrcode 
BEGIN
DROP FUNCTION IF EXISTS process_qrcode;
CREATE OR REPLACE FUNCTION process_qrcode(qrcode text, id_plc int8)
RETURNS jsonb AS $BODY$
DECLARE
  err_context text;
  task jsonb;
  tasks_arr jsonb;
  RESULT jsonb;
BEGIN
		FOR task IN (SELECT get_robot_tasks(qrcode::int8,id_robot)) LOOP
			PERFORM pg_notify('robot_task',task::text);
			tasks_arr:=COALESCE(tasks_arr,'[]')||task;
		END LOOP;
		RESULT := jsonb_build_object('result','OK','tasks',tasks_arr);
		EXECUTE format ($x$ INSERT INTO info_log ( info_text, SOURCE, DATA ) VALUES ( '%s', '%s', '%s' ); $x$, RESULT, 'process_qrcode',	json_data);

		RETURN RESULT;
	EXCEPTION 
			WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
			RAISE INFO '%',
			SQLERRM;
			PERFORM write_error_log ( SQLERRM, json_data :: TEXT, err_context );
			RETURN jsonb_build_object ( 'error', SQLERRM || '. ' || err_context );
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
COMMENT ON FUNCTION process_qrcode IS 'Генерация нового задания по QR коду';  
END$$;
*/
/*
SELECT process_qrcode('              1',1);
SELECT process_qrcode('              2',1);
*/
