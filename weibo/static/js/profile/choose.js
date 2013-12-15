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
function choose_statuses_count(range, window) {
    $("#"+range+"_window_size").val(window);
    cn_window = '';
    if(window==200)
        cn_window = '0-200万';
    else if(window==400)
  cn_window = '200-400万';
    else if(window==600)
  cn_window = '400-600万';
    else if(window==800)
  cn_window = '600-800万';
    else if(window==1000)
    cn_window = '800-1000万';
    else if(window==1200)
    cn_window = '1000-1200万';
    else if(window==1400)
    cn_window = '1200-1400万';
    else if(window==1600)
    cn_window = '1400-1600万';
    else if(window==1800)
    cn_window = '1600-1800万';
    else if(window==2000)
    cn_window = '1800-2000万';
    $("#"+range+"_window_size_choosen").text(cn_window);
    $("#"+range+"_window_size_choosen").append(' <span class="caret"></span>');
    //close dropdown
    $("#"+range+"_window_size_choosen").parent().removeClass('open');
    $("#"+range+"_window_size_dropdown_menu").children().removeClass('active');
}

function choose_followers_count(range, window) {
    $("#"+range+"_window_size").val(window);
    cn_window = '';
    if(window==600)
        cn_window = '0-600万';
    else if(window==1200)
    cn_window = '600-1200万';
    else if(window==1800)
    cn_window = '1200-1800万';
    else if(window==2400)
    cn_window = '1800-2400万';
    else if(window==3000)
    cn_window = '2400-3000万';
    else if(window==3600)
    cn_window = '3000-3600万';
    else if(window==4200)
    cn_window = '3600-4200万';
    else if(window==4800)
    cn_window = '4200-4800万';
    else if(window==5400)
    cn_window = '4800-5400万';
    else if(window==6000)
    cn_window = '5400-6000万';
    $("#"+range+"_window_size_choosen").text(cn_window);
    $("#"+range+"_window_size_choosen").append(' <span class="caret"></span>');
    //close dropdown
    $("#"+range+"_window_size_choosen").parent().removeClass('open');
    $("#"+range+"_window_size_dropdown_menu").children().removeClass('active');
}
function choose_friends_count(range, window) {
    $("#"+range+"_window_size").val(window);
    cn_window = '';
    if(window==400)
        cn_window = '0-400';
    else if(window==800)
    cn_window = '400-800';
    else if(window==1200)
    cn_window = '800-1200';
    else if(window==1600)
    cn_window = '1200-1600';
    else if(window==2000)
    cn_window = '1600-2000';
    else if(window==2400)
    cn_window = '2000-2400';
    else if(window==2800)
    cn_window = '2400-2800';
    else if(window==3200)
    cn_window = '2800-3200';
    else if(window==3600)
    cn_window = '3200-3600';
    else if(window==4000)
    cn_window = '3600-4000';
    $("#"+range+"_window_size_choosen").text(cn_window);
    $("#"+range+"_window_size_choosen").append(' <span class="caret"></span>');
    //close dropdown
    $("#"+range+"_window_size_choosen").parent().removeClass('open');
    $("#"+range+"_window_size_dropdown_menu").children().removeClass('active');
}
function choose_rankcount(range, method) {
    $("#"+range+"_rank_method").val(method);
    cn_method = '';
    if(method=='followers_count')
      cn_method = '粉丝数';
    else if(method=='friends_count')
      cn_method = '关注数';
    else if(method=='statuses_count')
      cn_method = '微博数';
    else if(method=='created_at')
      cn_method = '注册时间';
    $("#"+range+"_rank_method_choosen").text(cn_method);
    $("#"+range+"_rank_method_choosen").append(' <span class="caret"></span>');
    //close dropdown
    $("#"+range+"_rank_method_choosen").parent().removeClass('open');
    $("#"+range+"_rank_method_dropdown_menu").children().removeClass('active');
}
