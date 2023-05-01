function removeAllChildNodes(parent) {
    while (parent.firstChild) {
        parent.removeChild(parent.firstChild);
    }
}

function get_image_and_info() {
    $.ajax({
            url: `/get_current_lay_image`,
            xhrFields: {
                responseType: 'blob'
            },
            success: function (data) {
                console.log(data);
                if (data == new Blob()) {
                    $('#status_label')[0].textContent = "Очередь пуста";
                } else {
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

    $.ajax({
            url: `/get_current_data`,
            success: function (data) {
                console.log(data);
                removeAllChildNodes($('#table_header')[0])
                removeAllChildNodes($('#table_content')[0])

                for (const [key, value] of Object.entries(data)) {
                    console.log(key, value);

                    const td_header = document.createElement('td')
                    td_header.textContent = key
                    $('#table_header')[0].appendChild(td_header)

                    const td_content = document.createElement('td')
                    td_content.textContent = value
                    $('#table_content')[0].appendChild(td_content)
                }
            }
        }
    );
}

function update_image_and_info() {
    $.ajax({
            url: `/update_current_lay_image`,
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

    $.ajax({
            url: `/get_current_data`,
            success: function (data) {
                console.log(data);
                removeAllChildNodes($('#table_header')[0])
                removeAllChildNodes($('#table_content')[0])

                for (const [key, value] of Object.entries(data)) {

                    const td_header = document.createElement('td')
                    td_header.textContent = key
                    $('#table_header')[0].appendChild(td_header)

                    const td_content = document.createElement('td')
                    td_content.textContent = value
                    $('#table_content')[0].appendChild(td_content)
                }
            }
        }
    );

}

get_image_and_info()
setInterval(update_image_and_info, 1 * 1000)
