function choose_method(range, method) {
    $("#"+range+"_rank_method").val(method);
    cn_method = '';
    if(method=='followers')
        cn_method = '粉丝数';
    else if(method=='active')
	cn_method = '活跃度';
    else if(method=='important')
	cn_method = '重要度';
    $("#"+range+"_rank_method_choosen").text(cn_method);
    $("#"+range+"_rank_method_choosen").append(' <span class="caret"></span>');
    //close dropdown
    $("#"+range+"_rank_method_choosen").parent().removeClass('open');
    $("#"+range+"_rank_method_dropdown_menu").children().removeClass('active');
}

function choose_window(range, window) {
    $("#"+range+"_window_size").val(window);
    cn_window = '';
    if(window==1)
        cn_window = '1天';
    else if(window==7)
	cn_window = '1周';
    else if(window==30)
	cn_window = '1个月';
    else if(window==90)
	cn_window = '3个月';
    $("#"+range+"_window_size_choosen").text(cn_window);
    $("#"+range+"_window_size_choosen").append(' <span class="caret"></span>');
    //close dropdown
    $("#"+range+"_window_size_choosen").parent().removeClass('open');
    $("#"+range+"_window_size_dropdown_menu").children().removeClass('active');
}

function choose_dateRange() {
	$('#reportrange').daterangepicker(
    {
      ranges: {
         'Today': [moment(), moment()],
         'Yesterday': [moment().subtract('days', 1), moment().subtract('days', 1)],
         'Last 7 Days': [moment().subtract('days', 6), moment()],
         'Last 30 Days': [moment().subtract('days', 29), moment()],
         'This Month': [moment().startOf('month'), moment().endOf('month')],
         'Last Month': [moment().subtract('month', 1).startOf('month'), moment().subtract('month', 1).endOf('month')]
      },
      startDate: moment().subtract('days', 29),
      endDate: moment()
    },
    function(start, end) {
        $('#reportrange span').html(start.format('MMMM D, YYYY') + ' - ' + end.format('MMMM D, YYYY'));
    }
    );
}