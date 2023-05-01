/*
$('.push_start').each(() => {
    this.click(() => {
        let ip = this.data('plc');
        console.log(ip);
        //$.ajax({
        //    url: `/push_start/${ip}`,
        //});
    })
})
*/

/*
window.addEventListener("load",function(){
    $('.btn_div').on('click', '#push_start', function (event) {
        var t_href = event.target;
        let ip = t_href.dataset.plc;
        console.log(t_href)
        console.log(ip);
        $.ajax({
            url: `/push_start/${ip}`,
        });
        event.preventDefault();
    });
    $('.btn_div').on('click', '#push_reset', function (event) {
        var t_href = event.target;
        let ip = t_href.dataset.plc;
        console.log(t_href);
        console.log(ip);
        $.ajax({
            url: `/push_reset/${ip}`,
        });
        event.preventDefault();
    });
    $('.btn_div').on('click', '#robot_alarm_reset', function (event) {
        var t_href = event.target;
        let ip = t_href.dataset.plc;
        console.log(t_href);
        console.log(ip);
        $.ajax({
            url: `/robot_alarm_reset/${ip}`,
        });
        event.preventDefault();
    });
});
*/

let [...push_start] = document.querySelectorAll(".push_start")
push_start.forEach((elem) => {
    elem.addEventListener('click', (target) => {
        //console.log(target)
        let ip = target.target.dataset.plc;
        console.log(target)
        console.log(ip);
        $.ajax({
            url: `/push_start/${ip}`,
        });
        event.preventDefault();
    })
})

let [...push_reset] = document.querySelectorAll(".push_reset")
push_reset.forEach((elem) => {
    elem.addEventListener('click', (target) => {
        let ip = target.target.dataset.plc;
        //console.log(target)
        //console.log(ip);
        $.ajax({
            url: `/push_reset/${ip}`,
        });
        event.preventDefault();
    })
})

let [...robot_alarm_reset] = document.querySelectorAll(".robot_alarm_reset")
robot_alarm_reset.forEach((elem) => {
    elem.addEventListener('click', (target) => {
        let ip = target.target.dataset.plc;
        //console.log(target)
        //console.log(ip);
        $.ajax({
            url: `/robot_alarm_reset/${ip}`,
        });
        event.preventDefault();
    })
})

let [...robot_pause] = document.querySelectorAll(".robot_pause")
robot_pause.forEach((elem) => {
    elem.addEventListener('change', (target) => {
        let checked = target.target.checked;
        console.log(checked);
        let ip = target.target.dataset.plc;
        //console.log(target)
        console.log(ip);
        $.ajax({
            url: `/robot_pause/${ip}/${checked}`,
        });
        event.preventDefault();
    })
})

