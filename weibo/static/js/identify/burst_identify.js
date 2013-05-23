var previous_data = null;
var current_data = null;
(function ($) {
    function request_callback(data) {
	var status = data['status'];
	var data = data['data'];
	if (status == 'current finished') {
	    $("#current_process_bar").css('width', "100%")
	    $("#current_process").removeClass("active");
	    $("#current_process").removeClass("progress-striped");
	    current_data = data;
	    if (current_data.length) {
		$("#loading_current_data").text("计算完成!");
		if (current_data.length < page_num) {
		    page_num = current_data.length
		    create_current_table(current_data, 0, page_num);
		}
		else {
		    create_current_table(current_data, 0, page_num);
		    var total_pages = 0;
		    if (current_data.length % page_num == 0) {
			total_pages = current_data.length / page_num;
		    }
		    else {
			total_pages = current_data.length / page_num + 1;
		    }
		    $('#rank_page_selection').bootpag({
			total: total_pages,
			page: 1,
			maxVisible: 30
		    }).on("page", function(event, num){
			start_row = (num - 1)* page_num;
			end_row = start_row + 20;
			if (end_row > current_data.length)
			    end_row = current_data.length;
			create_current_table(current_data, start_row, end_row);
		    });
		}
	    }
	    else {
		$("#loading_current_data").text("本期计算结果为空!");
	    }
	    
	}
	else if (status == 'previous finished') {
	    // current results
	    $.post("/identify/burst/", {'action': 'rank', 'rank_method': rank_method, 'window_size': window_size, 'top_n': top_n}, request_callback, "json");

	    $("#previous_process_bar").css('width', "100%")
	    $("#previous_process").removeClass("active");
	    $("#previous_process").removeClass("progress-striped");
	    previous_data = data;
	    if (previous_data.length) {
		$("#loading_previous_data").text("计算完成!");
		if (previous_data.length < page_num) {
		    page_num = previous_data.length
		    create_previous_table(previous_data, 0, page_num);
		}
		else {
		    create_previous_table(previous_data, 0, page_num);
		    var total_pages = 0;
		    if (previous_data.length % page_num == 0) {
			total_pages = previous_data.length / page_num;
		    }
		    else {
			total_pages = previous_data.length / page_num + 1;
		    }
		    $('#previous_rank_page_selection').bootpag({
			total: total_pages,
			page: 1,
			maxVisible: 30
		    }).on("page", function(event, num){
			start_row = (num - 1)* page_num;
			end_row = start_row + 20;
			if (end_row > previous_data.length)
			    end_row = previous_data.length;
			create_previous_table(previous_data, start_row, end_row);
		    });
		}
	    }
	    else {
		$("#loading_previous_data").text("上期结果不存在!");
	    }
	}
	else
	    return
    }
    
    function create_current_table(data, start_row, end_row) {
	var cellCount = 9;
	var table = '<table class="table table-bordered">';
	var thead = '<thead><tr><th>排名</th><th>博主ID</th><th>博主昵称</th><th>博主地域</th><th>博主微博</th><th>转发数</th><th>评论数</th><th>同比</th><th>敏感状态</th><th>全选<input type="checkbox"></th></tr></thead>';
	var tbody = '<tbody>';
	for (var i = start_row;i < end_row;i++) {
            var tr = '<tr>';
	    if (data[i][3].match("海外")) {
		tr = '<tr class="success">';
	    }
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
		// user statuses
		if (j == 3) {
		    var td = '<td>查看</td>';
		    tr += td;
		}
            }
	    tr += '</tr>';
	    tbody += tr;
	}
	tbody += '</tbody>';
	table += thead + tbody;
	table += '</table>'
	$("#rank_table").html(table);
    }

    function create_previous_table(data, start_row, end_row) {
	var cellCount = 7;
	var table = '<table class="table table-bordered">';
	var thead = '<thead><tr><th>排名</th><th>博主ID</th><th>博主昵称</th><th>博主地域</th><th>博主微博</th><th>转发数</th><th>评论数</th><th>敏感状态</th></tr></thead>';
	var tbody = '<tbody>';
	for (var i = start_row;i < end_row;i++) {
            var tr = '<tr>';
	    if (data[i][3].match("海外")) {
		tr = '<tr class="success">';
	    }
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
		// user statuses
		if (j==3) {
		    var td = '<td>查看</td>';
		    tr += td;
		}
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
	$.post("/identify/burst/", {'action': 'previous_rank', 'rank_method': rank_method, 'window_size': window_size, 'top_n': top_n}, request_callback, "json");
    }

    identify_request();

})(jQuery);