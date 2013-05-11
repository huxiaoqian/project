(function ($) {
    function request_callback(data) {
	var status = data['status'];
	var data = data['data'];
	if (status == 'current finished') {
	    $("#loading_current_data").text("计算完成!");
	    $("#pr_process").css('width', "100%")
	    $("#pr_bar").removeClass("active");
	    $("#pr_bar").removeClass("progress-striped");
	    create_current_table(data, 20);
	}
	else if (status == 'previous finished') {
	    if (data.length) {
		$("#loading_previous_data").text("计算完成!");
		create_previous_table(data, 20);
	    }
	    else {
		$("#loading_previous_data").text("上期结果不存在!");
	    }
	}
	else
	    return
    }
    
    function create_current_table(data, rowCount) {
	var cellCount = 9;
	var table = '<table class="table table-bordered">';
	var thead = '<thead><tr><th>排名</th><th>博主ID</th><th>博主昵称</th><th>博主地域</th><th>粉丝数</th><th>关注数</th><th>同比</th><th>敏感状态</th><th>全选<input type="checkbox"></th></tr></thead>';
	var tbody = '<tbody>';
	for(var i = 0;i < rowCount;i++) {
            var tr = '<tr>';
            for(var j = 0;j < cellCount;j++) {
		if (j == 8) {
		    // checkbox
		    var td = '<td><input type="checkbox"></td>';
		}
		else if (j == 7) {
		    // identify status
		    if (data[i][j])
			var td = '<td><i class="icon-ok"></i></td>';
		    else
			var td = '<td><i class="icon-remove"></i></td>';
		}
		else if(j == 6) {
		    // comparsion
		    if (data[i][j] > 0)
			var td = '<td><i class="icon-arrow-up"></i></td>';
		    else if (data[i][j] < 0)
			var td = '<td><i class="icon-arrow-down"></i></td>';
		    else
			var td = '<td><i class="icon-minus"></i></td>';
		}
		else if(j == 0) {
		    // rank status
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

    function create_previous_table(data, rowCount) {
	var cellCount = 7;
	var table = '<table class="table table-bordered">';
	var thead = '<thead><tr><th>排名</th><th>博主ID</th><th>博主昵称</th><th>博主地域</th><th>粉丝数</th><th>关注数</th><th>敏感状态</th></tr></thead>';
	var tbody = '<tbody>';
	for(var i = 0;i < rowCount;i++) {
            var tr = '<tr>';
            for(var j = 0;j < cellCount;j++) {
		if (j == 6) {
		    // identify status
		    if (data[i][j])
			var td = '<td><i class="icon-ok"></i></td>';
		    else
			var td = '<td><i class="icon-remove"></i></td>';
		}
		else if(j == 0) {
		    // rank status
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
	$("#previous_rank_table").html(table);
    }

    function identify_request() {
	// previous results
	$.post("/identify/whole/", {'action': 'previous_rank'}, request_callback, "json");
	// current results
	$.post("/identify/whole/", {'action': 'rank'}, request_callback, "json");
    }

    identify_request();

})(jQuery);