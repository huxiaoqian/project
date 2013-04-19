(function ($) {
    var interval = null;
    var total_stage = 4;
    function request_callback(data) {
	var status = data["status"];
	if(status == 'finished') {
	    clearInterval(interval);
	    $("#loading_data").text("计算完成!");
	    $("#pr_process").css('width', "100%")
	    $("#pr_bar").removeClass("active");
	    $("#pr_bar").removeClass("progress-striped");
	    // print_data(data);
	    var data = data["data"];
	    CreateTable(data, 20, 9);
	}
	else if(status == 'data_not_prepared') {
	    $("#loading_data").text("数据装载中...");
	}
	else if(status == 'results_not_prepared') {
	    $("#loading_data").text("结果装载中...");
	}
	else {
	    var stage = status.replace(/[^\d]/g,'');
	    var present = stage * 100 / total_stage;
	    var width = parseInt(present) + '%';
	    $("#loading_data").text("计算阶段 " + stage + " / " + total_stage);
	    $("#pr_process").css('width', width);
	}
    }
    
    function CreateTable(data, rowCount, cellCount) { 
	var table = '<table class="table table-bordered">';
	var thead = '<thead><tr><th>排名</th><th>博主ID</th><th>博主昵称</th><th>博主地域</th><th>粉丝数</th><th>关注数</th><th>同比</th><th>敏感状态</th><th>全选<input type="checkbox"></th></tr></thead>';
	var tbody = '<tbody>';
	for(var i=0;i<rowCount;i++) {
            var tr = '<tr>';
            for(var j=0;j<cellCount;j++) {
		if(j == 8) {
		    var td = '<td><input type="checkbox"></td>';
		}
		else if(j == 7) {
		    var td = '<td><i class="icon-ok"></i></td>';
		}
		else if(j == 6) {
		   var td = '<td><i class="icon-arrow-up"></i></td>';
		}
		else if(j == 0) {
		    var td = '<td><span class="label label-important">'+data[i][j]+'</span></td>';
		}
		else{
		    var td = '<td>'+data[i][j]+'</td>';
		}
		tr += td;
            }
	    tr += '</tr>';
	    tbody += tr;
	}
	tbody += '</tbody>';
	table += thead + tbody;
	table += '</table>'
	$("#rank_table").html(table);
    }

    function print_data(data) {
	for(var i=0;i<20;i++) {
	    name = data[i][0];
	    pr = data[i][1];
	    console.log(name);
	}
    }

    function status_request() {
	$.post("/identify/area/", {'action': 'check', 'field': field, 'keywords': keywords}, request_callback, "json");
    }

    // function submit_request(event) {
    // 	console.log('submit request ok');
    // 	var form = $(this);
    // 	event.preventDefault();
    // 	$.post("/test/", form.serializeArray(), request_callback, "json");
    // 	return false;
    // }

    // $("#pr").submit(submit_request);

    interval = setInterval(status_request, 3000);

})(jQuery);