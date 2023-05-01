function changeSelect(selectObj) {
    // const id = selectObj.value;
    const id = selectObj.id;
    const status = selectObj.value;
    const data = JSON.stringify({"part_status": status})
    console.log(id)
    console.log(status)
    $.ajax({
            type: "PATCH",
            data: data,
            url: `/parts/${id}`,
            success: function (data) {
                console.log("DONE");
                $("#message").text(`Запись с id ${id} изменена, текущий статус: ${status}`)
                $("#message").css({'color': 'green', 'font-size': 'xx-large', 'text-align': 'center'})
            },
            error: function (data) {
                console.log(data)
                $("#message").text(`Ошибка! Не удалось изменить статус записи с id ${id}`)
                $("#message").css({'color': 'red', 'font-size': 'xx-large', 'text-align': 'center'})
            }
        }
    );
}

function get_data() {
    let date = new Date(Date.now());

    $.ajax({
            type: "GET",
            url: "/update_last_parts",
            success: function (data) {
                $('#parts_table').html(data.result);
                $('#update_time').text(`Последнее обновление ${date.toLocaleString('ru-Ru')}`);
                console.log("DONE");
            },
            error: function (data) {
                console.log(data)
                $("#update_time").text(`Не удалось получить данные`)
                $("#update_time").css({'color': 'red', 'font-size': 'xx-large', 'text-align': 'center'})
            }
        }
    );
}

setInterval(get_data, 15 * 1000)