let timeoutUpd = 1000;

function checkCheck() {
    $.ajax({
        url: "/logs_upd",
        success: function (data) {
            $('#logs').html(data.result);
        },
    });
}

setInterval(checkCheck, timeoutUpd);


function clear_logs() {
    console.log("Clear logs");
    $.ajax({
        url: `/clear_logs`,
        success: function () {
            console.log("LOG CLEAR!")
        },
    });
    event.preventDefault();
}
