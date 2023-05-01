function get_image_and_info() {
    console.log('before')
    $.ajax({
            url: `/get_current_status_image`,
            xhrFields: {
                responseType: 'blob'
            },
            success: function (data) {
                console.log(data);
                if (data == new Blob() ) {
                    $('#status_label')[0].textContent = "Очередь пуста";
                } else {
                    const url = window.URL || window.webkitURL;
                    const src = url.createObjectURL(data);
                    $('#status_label')[0].textContent = "";
                    $('#image').attr('src', src);
                }
            }
        }
    );
    console.log('after one')

    $.ajax({
            url: `/get_current_data`,
            success: function (data) {
                console.log(data);
                $('#lay_number')[0].textContent = data.lay_number;
                $('#part_status')[0].textContent = data.part_status;
                $('#robot_id')[0].textContent = data.robot_id;
                $('#pallet_length_x')[0].textContent = data.pallet_length_x;
                $('#pallet_length_y')[0].textContent = data.pallet_length_y;
            }
        }
    );

    console.log('after')

}

get_image_and_info()
setInterval(get_image_and_info, 1 * 1000)
