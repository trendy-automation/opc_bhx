let countTask = 1;
let tasks = [];


// function getFormDataTask($form) {
//     let unindexed_array = $form.serializeArray();
//
//     $.map(unindexed_array, function (n, i) {
//         if (n['name'] in tasks) {
//             tasks[n['name']].push(n['value']);
//         } else {
//             tasks[n['name']] = [n['value']];
//         }
//     });
// }

function getFormData($form) {
    var unindexed_array = $form.serializeArray();
    var indexed_array = {};

    $.map(unindexed_array, function (n, i) {
        indexed_array[n['name']] = n['value'];
    });

    return indexed_array;
}

function addTask(data) {
    let cur_tasks = document.querySelector("#cur_task");
    let p = document.createElement("p");
    p.textContent = JSON.stringify(data);
    cur_tasks.appendChild(p);
    // console.log(tasks);
}

function deltask() {
    tasks.pop();
    let cur_tasks = document.querySelector("#cur_task");
    if(cur_tasks.lastElementChild)  cur_tasks.removeChild(cur_tasks.lastElementChild);
    // console.log(tasks);
}

function newtask() {
    let $form = $("#task_list");
    let data = getFormData($form);
    tasks.push(data);
    addTask(data);
    // console.log(tasks);
}


function addDetail() {
    let $form = $("#main_info");
    let main_data = getFormData($form);

    // let detail = {"info": tasks, "main": data};

    // let detail = Object.assign(data, tasks);

    let robot_tasks = {"robot_tasks": tasks}
    let data = Object.assign(main_data, robot_tasks);

    let cur_tasks = document.querySelector("#cur_task");
    cur_tasks.innerHTML = "";

    let [...inputs] = document.querySelectorAll("input");
    // console.log(inputs);
    inputs.forEach((input) => {
        input.value = "";
    });

    $.ajax({
            type: "POST",
            data: JSON.stringify(data),
            dataType: 'json',
            url: "/details_add",
            success: function (data) {
                console.log(data);
            }
        }
    );

}

