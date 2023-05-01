UPDATE settings SET value = 'true' WHERE name = 'pushpuzzle_mode';
--UPDATE settings SET value = '800'  WHERE name = 'pallet_length_x';
--UPDATE settings SET value = '2800' WHERE name = 'pallet_length_y';
--UPDATE settings SET value = '1500' WHERE name = 'slot_length_x';
--UPDATE settings SET value = '2800' WHERE name = 'slot_length_y';

UPDATE settings SET value = '1200' WHERE name = 'pallet_in_length_x';
UPDATE settings SET value = '800' WHERE name = 'pallet_in_length_y';
UPDATE settings SET value = '1200' WHERE name = 'pallet_out_length_x';
UPDATE settings SET value = '800' WHERE name = 'pallet_out_length_y';
UPDATE settings SET value = '2000' WHERE name = 'part_slot_length_x';
UPDATE settings SET value = '1500' WHERE name = 'part_slot_length_y';


--SELECT pack_parts(jsonb_build_object('part_source','pallet_in','part_destination','part_slot','robot_id',1));
--SELECT get_part_layers(jsonb_build_object('part_status', 'part_slot','robot_id', 1,'part_slot', 0));
--SELECT get_part_layers(jsonb_build_object('part_status', 'part_slot','robot_id', 1,'part_slot', 1));

--SELECT debug_on();
SELECT process_xyacodes('{"query": "process_xyacodes", "XYA_codes": ["Failure", "Failure", {"part_id": 203860001100007, "scan_label_pos_x": 1069, "scan_label_pos_y": 256, "scan_label_angle_a": 178, "scan_robot_pos_x": -860.0, "scan_robot_pos_y": 150.0, "scan_robot_pos_z": 1000.0, "scan_robot_angle_a": -180.0}, "Failure", {"part_id": 203860001100010, "scan_label_pos_x": 1775, "scan_label_pos_y": 200, "scan_label_angle_a": 173, "scan_robot_pos_x": 280.0, "scan_robot_pos_y": 150.0, "scan_robot_pos_z": 1000.0, "scan_robot_angle_a": -180.0}, {"part_id": 203860001100008, "scan_label_pos_x": 586, "scan_label_pos_y": 225, "scan_label_angle_a": 179, "scan_robot_pos_x": 860.0, "scan_robot_pos_y": 150.0, "scan_robot_pos_z": 1000.0, "scan_robot_angle_a": -180.0}, "Failure", "Failure"], "scan_lay_number": 1, "plc_id": 0, "robot_id": 1}');

SELECT get_part_layers('{"part_status":"pallet_in", "robot_id":1}');



UPDATE robot_tasks rt SET task_status = 'done'
		FROM parts p WHERE p.part_status = 'pallet_in' and operation_type!='transfer_in2slot' and task_status='not_sended' and rt.part_id=p.id;
UPDATE parts p SET part_status = 'part_slot', part_pos_x = slot_pos_x, part_pos_y = slot_pos_y, part_pos_z = slot_pos_z, part_angle_a = slot_angle_a 
		FROM robot_tasks rt WHERE p.part_status = 'pallet_in' and rt.operation_type='transfer_in2slot' and rt.part_id=p.id and rt.task_status='not_sended';
UPDATE parts SET part_status = 'part_slot', part_pos_x = slot_pos_x, part_pos_y = slot_pos_y, part_pos_z = slot_pos_z, part_angle_a = slot_angle_a 
		WHERE part_status = 'pallet_in' AND part_slot > 0;
UPDATE parts SET part_status = 'pallet_out'
		WHERE part_status = 'pallet_in' AND part_slot = 0;
UPDATE robot_tasks SET task_status = 'done' WHERE operation_type='transfer_in2slot' and task_status='not_sended';

SELECT get_part_layers('{"part_status":"part_slot", "robot_id":1,"part_slot":1}');


SELECT process_xyacodes('{"query": "process_xyacodes", "XYA_codes": ["Failure"], "scan_lay_number": 2, "plc_id": 0, "robot_id": 1}');

UPDATE parts p SET part_status = 'pallet_out'
FROM robot_tasks rt WHERE p.part_status = 'part_slot' and rt.operation_type='transfer_slot2out' and rt.part_id=p.id and rt.task_status='not_sended';
UPDATE robot_tasks SET task_status = 'done' WHERE operation_type='transfer_slot2out' and task_status='not_sended';


SELECT get_part_layers('{"part_status":"pallet_out", "robot_id":1}');



SELECT process_xyacodes('{"query": "process_xyacodes", "XYA_codes": ["Failure", "Failure", {"part_id": 203860000509001, "scan_label_pos_x": 1069, "scan_label_pos_y": 256, "scan_label_angle_a": 178, "scan_robot_pos_x": -860.0, "scan_robot_pos_y": 150.0, "scan_robot_pos_z": 1000.0, "scan_robot_angle_a": -180.0}, "Failure", "Failure"], "scan_lay_number": 80, "plc_id": 0, "robot_id": 1}');

SELECT get_part_layers('{"part_status":"pallet_in", "robot_id":1}');


UPDATE robot_tasks rt SET task_status = 'done'
		FROM parts p WHERE p.part_status = 'pallet_in' and operation_type!='transfer_in2slot' and task_status='not_sended' and rt.part_id=p.id;
UPDATE parts p SET part_status = 'part_slot', part_pos_x = slot_pos_x, part_pos_y = slot_pos_y, part_pos_z = slot_pos_z, part_angle_a = slot_angle_a 
		FROM robot_tasks rt WHERE p.part_status = 'pallet_in' and rt.operation_type='transfer_in2slot' and rt.part_id=p.id and rt.task_status='not_sended';
UPDATE parts SET part_status = 'part_slot', part_pos_x = slot_pos_x, part_pos_y = slot_pos_y, part_pos_z = slot_pos_z, part_angle_a = slot_angle_a 
		WHERE part_status = 'pallet_in' AND part_slot > 0;
UPDATE parts SET part_status = 'pallet_out'
		WHERE part_status = 'pallet_in' AND part_slot = 0;
UPDATE robot_tasks SET task_status = 'done' WHERE operation_type='transfer_in2slot' and task_status='not_sended';

SELECT get_part_layers('{"part_status":"part_slot", "robot_id":1,"part_slot":1}');


SELECT process_xyacodes('{"query": "process_xyacodes", "XYA_codes": ["Failure"], "scan_lay_number": 2, "plc_id": 0, "robot_id": 1}');

UPDATE parts SET part_status = 'pallet_out'
		WHERE part_status = 'part_slot';

SELECT get_part_layers('{"part_status":"pallet_out", "robot_id":1}');


SELECT process_xyacodes('{"query": "process_xyacodes", "XYA_codes": ["Failure", "Failure", {"part_id": 203860002409001, "scan_label_pos_x": 1069, "scan_label_pos_y": 256, "scan_label_angle_a": 178, "scan_robot_pos_x": -860.0, "scan_robot_pos_y": 150.0, "scan_robot_pos_z": 1000.0, "scan_robot_angle_a": -180.0}, "Failure", "Failure"], "scan_lay_number": 1, "plc_id": 0, "robot_id": 1}');

SELECT get_part_layers('{"part_status":"pallet_in", "robot_id":1}');

UPDATE robot_tasks rt SET task_status = 'done'
		FROM parts p WHERE p.part_status = 'pallet_in' and operation_type!='transfer_in2slot' and task_status='not_sended' and rt.part_id=p.id;
UPDATE parts p SET part_status = 'part_slot', part_pos_x = slot_pos_x, part_pos_y = slot_pos_y, part_pos_z = slot_pos_z, part_angle_a = slot_angle_a 
		FROM robot_tasks rt WHERE p.part_status = 'pallet_in' and rt.operation_type='transfer_in2slot' and rt.part_id=p.id and rt.task_status='not_sended';
UPDATE parts SET part_status = 'part_slot', part_pos_x = slot_pos_x, part_pos_y = slot_pos_y, part_pos_z = slot_pos_z, part_angle_a = slot_angle_a 
		WHERE part_status = 'pallet_in' AND part_slot > 0;
UPDATE parts SET part_status = 'pallet_out'
		WHERE part_status = 'pallet_in' AND part_slot = 0;
UPDATE robot_tasks SET task_status = 'done' WHERE operation_type='transfer_in2slot' and task_status='not_sended';

SELECT get_part_layers('{"part_status":"part_slot", "robot_id":1,"part_slot":1}');

SELECT process_xyacodes('{"query": "process_xyacodes", "XYA_codes": ["Failure"], "scan_lay_number": 2, "plc_id": 0, "robot_id": 1}');

UPDATE parts SET part_status = 'pallet_out'
		WHERE part_status = 'part_slot';



SELECT get_part_layers('{"part_status":"pallet_out", "robot_id":1}');


UPDATE settings SET value = 'false' WHERE name = 'pushpuzzle_mode';
