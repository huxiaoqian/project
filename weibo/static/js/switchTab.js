/*********************************************************

Name: SwitchTab

Author: chilijung

Description: Pure javascript tab library (switching tabs by using Ajax), 
and you can customize your tabs in a fastest way.

License

(The MIT License)

Copyright (c) 2013  chilijung <chilijung@gmail.com>

Permission is hereby granted, free of charge, to any person 
obtaining a copy of this software and associated documentation 
files (the 'Software'), to deal in the Software without restriction, 
including without limitation the rights to use, copy, modify, merge, 
publish, distribute, sublicense, and/or sell copies of the Software, 
and to permit persons to whom the Software is furnished to do so, 
subject to the following conditions:

The above copyright notice and this permission notice shall be 
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, 
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES 
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, 
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR 
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE 
OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 ********************************************************/

var switchTab = function(options) {

	// pass options of set to default.
	this.tabClass = options.tabClass || '';
	this.contentId = options.contentId || '';
	this.activeClass = options.activeClass || '';
	this.showFirst = options.showFirst || '';
	this.tabIdArr = options.tabId || [];

	// showup the first tab
	this.showFirstFn(this);

	// switch to correct state from url
	this.state(this);

	// tab click function
	function tabClick (options) {
		var tabIdArr = options.tabIdArr,
			tabClass = options.tabClass,
			activeClass = options.activeClass,
			contentId = options.contentId
		

		// register onclick function for all the tabs
		for (var i = 0, len = tabIdArr.length; i < len; i++) {
			document.getElementById(tabIdArr[i]).onclick = function(data) {
				var tabId = data.srcElement.id;

				// history pushState
				var state_obj = { 'tab_state': tabId}
				history.pushState(state_obj, tabId, '#' + tabId);
				console.log(history);
				console.log(history.state)
				options.activeColor(tabClass, activeClass, tabId);
				options.fetchContent(options.getUrl(tabId), contentId)
			}
		}
	}
	tabClick(this);
}

switchTab.prototype.state = function(options) {
	var contentId = options.contentId,			
		tabClass = options.tabClass,
		activeClass = options.activeClass,
		contentId = options.contentId
	var hash = window.location.hash;
	var sub_hash = hash.substring(1);

	if(hash) {
		this.activeColor(tabClass, activeClass, sub_hash);
		this.fetchContent(this.getUrl(sub_hash), contentId)
	}
};


switchTab.prototype.getUrl = function(tabId) {
	return document.getElementById(tabId).getAttribute('tab-url');
};


// fetch content from URL
switchTab.prototype.fetchContent = function(tabUrl, contentId) {
	var xmlhttp;
	if (window.XMLHttpRequest) {// code for IE7+, Firefox, Chrome, Opera, Safari
	  	xmlhttp=new XMLHttpRequest();
	}else {// code for IE6, IE5
	  	xmlhttp=new ActiveXObject("Microsoft.XMLHTTP");
	}
	xmlhttp.onreadystatechange=function() {
		if (xmlhttp.readyState==4 && xmlhttp.status==200) {
		    document.getElementById(contentId).innerHTML=xmlhttp.responseText;
		}
	}
	xmlhttp.open("GET",tabUrl,true);
	xmlhttp.send();
};

// tab active color switch
switchTab.prototype.activeColor = function(tabClass, activeClass, tabId) {
	var elems = document.getElementsByClassName(tabClass);
	for(var i = 0; i < elems.length; i++) {
	    elems[i].className = elems[i].className.replace(activeClass , '' );
	}
	document.getElementById(tabId).className += ' ' + activeClass;
};

// the first tab
switchTab.prototype.showFirstFn = function(options) {
	var tabId = options.showFirst,
		tabClass = options.tabClass,
		activeClass = options.activeClass,
		contentId = options.contentId;
	options.activeColor(tabClass, activeClass, tabId);
	options.fetchContent(options.getUrl(tabId), contentId)
};
