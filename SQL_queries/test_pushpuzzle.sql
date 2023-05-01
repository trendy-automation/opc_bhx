UPDATE parts SET part_status='ordered', part_side='A', part_slot=0, lay_number=-1
								,part_pos_x=0, part_pos_y=0, part_pos_z=0, part_angle_a=0
								,slot_pos_x=0, slot_pos_y=0, slot_pos_z=0, slot_angle_a=0
								,out_pos_x=0, out_pos_y=0, out_pos_z=0, out_pos_a=0 WHERE id>0;
DELETE FROM robot_tasks WHERE operation_type::text LIKE 'transfer%' OR operation_type::text='scanning' OR operation_type::text='go_home' OR operation_type::text='measuring_height';
UPDATE robot_tasks SET task_status='not_sended', part_number=0 , operation_number=1, gripper_id=4;
UPDATE settings SET value = 'true' WHERE name = 'pushpuzzle_mode';
UPDATE settings SET value = '1200' WHERE name = 'pallet_length_x';
UPDATE settings SET value = '2800' WHERE name = 'pallet_length_y';
UPDATE settings SET value = '2800' WHERE name = 'slot_length_x';
UPDATE settings SET value = '864' WHERE name = 'slot_length_y';

SELECT process_xyacodes('{"query": "process_xyacodes", "XYA_codes": [{"part_id": 209758003000002, "scan_label_pos_x": 958, "scan_label_pos_y": 919, "scan_label_angle_a": 267, "scan_robot_pos_x": 0.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 1600.0, "scan_robot_angle_a": 0.0}, {"part_id": 209758003000001, "scan_label_pos_x": 480, "scan_label_pos_y": 923, "scan_label_angle_a": 280, "scan_robot_pos_x": 0.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 1600.0, "scan_robot_angle_a": 0.0}], "scan_lay_number": 1, "plc_id": 0, "robot_id": 3}');

UPDATE parts p SET part_status = 'part_slot', part_pos_x = slot_pos_x, part_pos_y = slot_pos_y, part_pos_z = slot_pos_z, part_angle_a = slot_angle_a 
FROM robot_tasks rt WHERE p.part_status = 'pallet_in' and rt.operation_type='transfer_in2slot' and rt.part_id=p.id and rt.task_status='not_sended';
UPDATE robot_tasks SET task_status = 'done' WHERE operation_type='transfer_in2slot' and task_status='not_sended';

SELECT get_part_layers('{"part_status":"part_slot", "robot_id":3,"part_slot":1}');

--SELECT debug_on();
SELECT process_xyacodes('{"query": "process_xyacodes", "XYA_codes": [{"part_id": 209758001800001, "scan_label_pos_x": 499, "scan_label_pos_y": 766, "scan_label_angle_a": 91, "scan_robot_pos_x": 0.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 1600.0, "scan_robot_angle_a": 0.0}, {"part_id": 209758001800002, "scan_label_pos_x": 938, "scan_label_pos_y": 858, "scan_label_angle_a": 269, "scan_robot_pos_x": 0.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 1600.0, "scan_robot_angle_a": 0.0}], "scan_lay_number": 2, "plc_id": 0, "robot_id": 3}');

UPDATE parts p SET part_status = 'part_slot', part_pos_x = slot_pos_x, part_pos_y = slot_pos_y, part_pos_z = slot_pos_z, part_angle_a = slot_angle_a 
FROM robot_tasks rt WHERE p.part_status = 'pallet_in' and rt.operation_type='transfer_in2slot' and rt.part_id=p.id and rt.task_status='not_sended';
UPDATE robot_tasks SET task_status = 'done' WHERE operation_type='transfer_in2slot' and task_status='not_sended';

SELECT get_part_layers('{"part_status":"part_slot", "robot_id":3,"part_slot":1}');

SELECT process_xyacodes('{"query": "process_xyacodes", "XYA_codes": [{"part_id": 209758001600002, "scan_label_pos_x": 906, "scan_label_pos_y": 849, "scan_label_angle_a": 261, "scan_robot_pos_x": 0.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 1600.0, "scan_robot_angle_a": 0.0}, {"part_id": 209758001600001, "scan_label_pos_x": 492, "scan_label_pos_y": 874, "scan_label_angle_a": 269, "scan_robot_pos_x": 0.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 1600.0, "scan_robot_angle_a": 0.0}], "scan_lay_number": 3, "plc_id": 0, "robot_id": 3}');

UPDATE parts p SET part_status = 'part_slot', part_pos_x = slot_pos_x, part_pos_y = slot_pos_y, part_pos_z = slot_pos_z, part_angle_a = slot_angle_a 
FROM robot_tasks rt WHERE p.part_status = 'pallet_in' and rt.operation_type='transfer_in2slot' and rt.part_id=p.id and rt.task_status='not_sended';
UPDATE robot_tasks SET task_status = 'done' WHERE operation_type='transfer_in2slot' and task_status='not_sended';

SELECT get_part_layers('{"part_status":"part_slot", "robot_id":3,"part_slot":1}');

SELECT process_xyacodes('{"query": "process_xyacodes", "XYA_codes": [{"part_id": 209758001109001, "scan_label_pos_x": 810, "scan_label_pos_y": 622, "scan_label_angle_a": 86, "scan_robot_pos_x": 0.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 1600.0, "scan_robot_angle_a": 0.0}], "scan_lay_number": 4, "plc_id": 0, "robot_id": 3}');

UPDATE parts p SET part_status = 'part_slot', part_pos_x = slot_pos_x, part_pos_y = slot_pos_y, part_pos_z = slot_pos_z, part_angle_a = slot_angle_a 
FROM robot_tasks rt WHERE p.part_status = 'pallet_in' and rt.operation_type='transfer_in2slot' and rt.part_id=p.id and rt.task_status='not_sended';
UPDATE robot_tasks SET task_status = 'done' WHERE operation_type='transfer_in2slot' and task_status='not_sended';

SELECT get_part_layers('{"part_status":"part_slot", "robot_id":3,"part_slot":1}');

SELECT process_xyacodes('{"query": "process_xyacodes", "XYA_codes": [{"part_id": 209758000309001, "scan_label_pos_x": 695, "scan_label_pos_y": 669, "scan_label_angle_a": 359, "scan_robot_pos_x": 0.0, "scan_robot_pos_y": 0.0, "scan_robot_pos_z": 1600.0, "scan_robot_angle_a": 0.0}], "scan_lay_number": 5, "plc_id": 0, "robot_id": 3}');

UPDATE parts p SET part_status = 'part_slot', part_pos_x = slot_pos_x, part_pos_y = slot_pos_y, part_pos_z = slot_pos_z, part_angle_a = slot_angle_a 
FROM robot_tasks rt WHERE p.part_status = 'pallet_in' and rt.operation_type='transfer_in2slot' and rt.part_id=p.id and rt.task_status='not_sended';
UPDATE robot_tasks SET task_status = 'done' WHERE operation_type='transfer_in2slot' and task_status='not_sended';

SELECT get_part_layers('{"part_status":"part_slot", "robot_id":3,"part_slot":1}');

SELECT process_xyacodes('{"query": "process_xyacodes", "XYA_codes": ["Failure"], "scan_lay_number": 6, "plc_id": 0, "robot_id": 3}');

UPDATE parts p SET part_status = 'pallet_out' 
FROM robot_tasks rt WHERE p.part_status = 'part_slot' and rt.operation_type='transfer_slot2out' and rt.part_id=p.id and rt.task_status='not_sended';
UPDATE robot_tasks SET task_status = 'done' WHERE operation_type='transfer_slot2out' and task_status='not_sended';

SELECT get_part_layers('{"part_status":"pallet_out", "robot_id":3}');

--SELECT process_packing('{"parts": [{"part_id": 209758000309001, "lay_number": 2, "part_pos_x": -111, "part_pos_y": -108, "part_pos_z": 16, "part_angle_a": 0, "part_length_x": 1471, "part_length_y": 548, "part_thickness_z": 16}], "next_lay": true, "part_indent": 50, "previous_lay": [], "pallet_length_x": 1200, "pallet_length_y": 2700}');
--SELECT process_packing('{"parts": [{"part_id": 209758003000002, "lay_number": 1, "part_pos_x": 13.140524, "part_pos_y": -206.45258, "part_pos_z": 0, "part_angle_a": 267, "part_length_x": 400, "part_length_y": 184, "part_thickness_z": 16}, {"part_id": 209758003000001, "lay_number": 1, "part_pos_x": -222.82007, "part_pos_y": -204.84366, "part_pos_z": 0, "part_angle_a": 280, "part_length_x": 400, "part_length_y": 184, "part_thickness_z": 16}], "next_lay": true, "part_indent": 50, "previous_lay": [], "pallet_length_x": 2800, "pallet_length_y": 864}');

--SELECT pack_parts('{"part_source":"part_slot","part_destination":"pallet_out","robot_id":3}');

SELECT process_xyacodes('{"query": "process_xyacodes", "XYA_codes": ["Failure"], "scan_lay_number": 6, "plc_id": 0, "robot_id": 3}');

UPDATE parts p SET part_status = 'pallet_out'
FROM robot_tasks rt WHERE p.part_status = 'part_slot' and rt.operation_type='transfer_slot2out' and rt.part_id=p.id and rt.task_status='not_sended';
UPDATE robot_tasks SET task_status = 'done' WHERE operation_type='transfer_slot2out' and task_status='not_sended';

SELECT get_part_layers('{"part_status":"pallet_out", "robot_id":3}');

UPDATE settings SET value = 'false' WHERE name = 'pushpuzzle_mode';

