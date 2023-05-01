function reset_image() {
    $('#image').attr('src', null);
    $('#status_label')[0].textContent = "Загрузка...";

    const select_layer = document.getElementById("layer")
    const select_robot = document.getElementById("robot")
    const select_status = document.getElementById("status")
    const select_part_slot = document.getElementById("part_slot")

    get_image(select_layer, select_robot, select_status, select_part_slot)
}


function get_image(select_layer, select_robot, select_status, select_part_slot) {

    const layer = parseInt(select_layer.value)
    const robot = parseInt(select_robot.value)
    const status = select_status.value
    const part_slot = parseInt(select_part_slot.value)

    $.ajax({
            url: `/get_lay_image/${robot}/${layer}/${status}/${part_slot}`,
            xhrFields: {
                responseType: 'blob'
            },
            success: function (data) {
                if (data != new Blob()) {
                    console.log(data);
                    let reader = new FileReader();
                    reader.readAsDataURL(data); // конвертирует Blob в base64 и вызывает onload

                    reader.onload = function () {
                        $('#image').attr('src', reader.result) // url с данными
                        $('#status_label')[0].textContent = ''
                    };
                }
            }
        }
    );
}


function update_image(select_layer, select_robot, select_status, select_part_slot) {

    const layer = parseInt(select_layer.value)
    const robot = parseInt(select_robot.value)
    const status = select_status.value
    const part_slot = parseInt(select_part_slot.value)


    $.ajax({
            url: `/update_lay_image/${robot}/${layer}/${status}/${part_slot}`,
            xhrFields: {
                responseType: 'blob'
            },
            success: function (data) {
                if (data != new Blob()) {
                    console.log(data);
                    let reader = new FileReader();
                    reader.readAsDataURL(data); // конвертирует Blob в base64 и вызывает onload

                    reader.onload = function () {
                        $('#image').attr('src', reader.result) // url с данными
                        $('#status_label')[0].textContent = ''
                    };
                }
            }
        }
    );
}


const select_layer = document.getElementById("layer")
select_layer.addEventListener("change", reset_image)
const select_robot = document.getElementById("robot")
select_robot.addEventListener("change", reset_image)
const select_status = document.getElementById("status")
select_status.addEventListener("change", reset_image)
const select_part_slot = document.getElementById("part_slot")
select_part_slot.addEventListener("change", reset_image)

get_image(select_layer, select_robot, select_status, select_part_slot)
setInterval(update_image, 15 * 1000, select_layer, select_robot, select_status, select_part_slot)
