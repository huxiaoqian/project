function choose_method(range, method) {
    $("#"+range+"_rank_method").val(method);
    cn_method = '';
    if(method=='finance')
        cn_method = '财经';
    else if(method=='media')
	cn_method = '媒体';
    else if(method=='culture')
	cn_method = '文化';
    else if(method=='technology')
    cn_method = '科技';
    else if(method=='entertainment')
    cn_method = '娱乐';
    else if(method=='education')
    cn_method = '教育';
    else if(method=='fashion')
    cn_method = '时尚';
    else if(method=='sports')
    cn_method = '体育';

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