(function ($) {
    function request_callback(data) {
	var status = data['status'];
	var data = data['data'];
	if (status == 'current finished') {
	    $("#current_process_bar").css('width', "100%")
	    $("#current_process").removeClass("active");
	    $("#current_process").removeClass("progress-striped");
	    if (data.length) {
		$("#loading_current_data").text("计算完成!");
		if (data.length < page_num)
		    page_num = data.length
		create_current_table(data, page_num);
	    }
	    else {
		$("#loading_current_data").text("本期计算结果为空!");
	    }
	    
	}
	else if (status == 'previous finished') {
	    // current results
	    $.post("/identify/area/", {'action': 'rank', 'topic_id': topic_id, 'rank_method': rank_method, 'window_size': window_size, 'top_n': top_n}, request_callback, "json");

	    $("#previous_process_bar").css('width', "100%")
	    $("#previous_process").removeClass("active");
	    $("#previous_process").removeClass("progress-striped");
	    if (data.length) {
		$("#loading_previous_data").text("计算完成!");
		if (data.length < page_num)
		    page_num = data.length
		create_previous_table(data, page_num);
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
	$.post("/identify/area/", {'action': 'previous_rank', 'topic_id': topic_id, 'rank_method': rank_method, 'window_size': window_size, 'top_n': top_n}, request_callback, "json");
    }

    identify_request();

})(jQuery);