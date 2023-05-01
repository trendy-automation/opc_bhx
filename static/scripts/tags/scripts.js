// Обновление
let timeoutUpd = 1000;

function checkCheck() {

    $.ajax({
        url: "/tags_upd",
        success: function (data) {
            // console.log(data);
            for (plc in data.result) {
                    for (tag in data.result[plc][1]) {
                        let tag_for_search = tag.replace('/', '_');
                        let id = `${plc}_1_${tag_for_search}`;
                        let change_element = document.getElementById(id);
                        try {
                            change_element.textContent = data.result[plc][1][tag]['tag_value'];
                        } catch (err) {
                            console.log(err);
                            console.log(plc);
                            console.log(1);
                            console.log(tag);
                            console.log(data.result[plc][1][tag]['tag_value']);
                        }
                    }
            }
            console.log("Update!");
        },
    });
}

setInterval(checkCheck, timeoutUpd);

// запись
function getData(target, plc_name) {
    let selector = `#${target}`
    let tr = $(selector);
    let [...td] = tr.find("td");
    let newTd = td.filter(function (elem) {
        if (!elem.hasAttribute("data-content")) return true
        else return false;
    }).map(function (elem) {
        return elem.textContent.trim();
    });

    let newValSelector = `#${target}NewVal`
    let elem = $(newValSelector);
    let newValue;
    if (elem.attr("type") == "checkbox") newValue = elem.prop('checked');
    else newValue = elem.val();

    let jsonData = {
        "name": newTd[0],
        "type": newTd[1],
        "DB": newTd[2],
        "bit": newTd[3],
        "byte": newTd[4],
        "newValue": newValue
    };

    $.ajax({
            type: "POST",
            data: JSON.stringify(jsonData),
            dataType: 'json',
            url: `/tags_write/${plc_name}`,
        }
    );
    console.log(`Write: ${plc_name}:${JSON.stringify(jsonData)}`)
}

