function whole_choose_method(method) {
    $("#rank_method").val(method);
    cn_method = '';
    if(method=='followers')
        cn_method = '粉丝数';
    else if(method=='active')
	cn_method = '活跃度';
    else if(method=='important')
	cn_method = '重要度';
    $("#rank_method_choosen").text(cn_method);
    $("#rank_method_choosen").append(' <span class="caret"></span>');
    //close dropdown
    $('[data-toggle="dropdown"]').parent().removeClass('open');
}