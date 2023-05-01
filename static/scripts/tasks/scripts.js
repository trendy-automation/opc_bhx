// Обновление

function changeSelect(selectObj) {
    const id = selectObj.value;
    const status = selectObj.options[selectObj.selectedIndex].label;
    const data = JSON.stringify({"task_status": status})

    $.ajax({
            type: "PATCH",
            data: data,
            url: `/tasks/${id}`,
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


function get_data(){
    let date = new Date(Date.now());

    $.ajax({
            type: "GET",
            url: "/tasks_upd",
            success: function (data) {
                $('#tasks_table').html(data.result);
                $('#update_time').text(`Последнее обновление ${date.toLocaleString('ru-Ru')}`);
                console.log("DONE");
            }
        }
    );
}

setInterval(get_data, 15 * 1000)