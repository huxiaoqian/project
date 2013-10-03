var labelType, useGradients, nativeTextSupport, animate;

(function() {
  var ua = navigator.userAgent,
      iStuff = ua.match(/iPhone/i) || ua.match(/iPad/i),
      typeOfCanvas = typeof HTMLCanvasElement,
      nativeCanvasSupport = (typeOfCanvas == 'object' || typeOfCanvas == 'function'),
      textSupport = nativeCanvasSupport 
        && (typeof document.createElement('canvas').getContext('2d').fillText == 'function');
  //I'm setting this based on the fact that ExCanvas provides text support for IE
  //and that as of today iPhone/iPad current text support is lame
  labelType = (!nativeCanvasSupport || (textSupport && !iStuff))? 'Native' : 'HTML';
  nativeTextSupport = labelType == 'Native';
  useGradients = nativeCanvasSupport;
  animate = !(iStuff || !nativeCanvasSupport);
})();

var Log = {
  elem: false,
  write: function(text){
    if (!this.elem) 
      this.elem = document.getElementById('log');
    this.elem.innerHTML = text;
    this.elem.style.left = (500 - this.elem.offsetWidth / 2) + 'px';
  }
};

$(document).ready(function(){
  init_vis();
})

function init_vis(){
	$.ajax({     
    url:'/profile/person_network/1813080181',
    dataType: 'json', 
    success:function(data){ 
    	 console.log(data)    
       init(data);     
    }  
  })
}

function init(data){
  //init data
  var json=data
    var sb = new $jit.Sunburst({
        //id container for the visualization
        injectInto: 'infovis',
        //Distance between levels
        levelDistance: 75,
        //Change node and edge styles such as
        //color, width and dimensions.
        Node: {
          overridable: true,
          type: useGradients? 'gradient-multipie' : 'multipie'
        },
        //Select canvas labels
        //'HTML', 'SVG' and 'Native' are possible options
        Label: {
          type: labelType
        },
        //Change styles when hovering and clicking nodes
        NodeStyles: {
          enable: true,
          type: 'Native',
          stylesClick: {
            'color': '#33dddd'
          },
          stylesHover: {
            'color': '#dd3333'
          }
        },
        //Add tooltips
        Tips: {
          enable: true,
          onShow: function(tip, node) {
            var html = ""; 
            var data = node.data;
            if("profileImageUrl" in data) {
              html += "<li><img style=\"width: 100px; height: 100px;\" src= "+data.profileImageUrl+" ></li>";
            }
            if ("name" in data) {
              html+="<br /><b>昵称:</b> " + data.name;

            };
            if("friendsCount" in data) {
              html += "<br /><b>关注数:</b> " + data.friendsCount;
            }
            if("followersCount" in data) {
              html += "<br /><b>粉丝数:</b> " + data.followersCount;
            }
            if("statusesCount" in data) {
              html += "<br /><b>微博数:</b> " + data.statusesCount;
            }
            if("gender" in data) {
            	if (data.gender=='m'){
            		   var g='男'
            		}
            	else{
            		   var g ='女'
            		}
              html += "<br /><b>性别:</b> " + g;
            }
            if("verified" in data) {
            	if (data.verified){
            		   var g='认证用户'
            		}
            	else{
            		   var g ='非认证用户'
            		}
              html += "<br /> "+g;
            }
            if("id" in data) {
              html += "<br /> <b>用户id:</b> "+ data.id;
            }

            tip.innerHTML = html;
          }
        },
        //implement event handlers
        Events: {
          enable: true,
          onClick: function(node) {
            if(!node) return;
            //Build detailed information about the file/folder
            //and place it in the right column.
            var html = ""; 
            var data = node.data;
            if("profileImageUrl" in data) {
              html += "<li><img style=\"width: 100px; height: 100px;\" src= "+data.profileImageUrl+" ></li>";
            }
            if ("name" in data) {
              html+="<br /><b>昵称:</b> " + data.name;

            };
            if("friendsCount" in data) {
              html += "<br /><b>关注数:</b> " + data.friendsCount;
            }
            if("followersCount" in data) {
              html += "<br /><b>粉丝数:</b> " + data.followersCount;
            }
            if("statusesCount" in data) {
              html += "<br /><b>微博数:</b> " + data.statusesCount;
            }
            if("gender" in data) {
              if (data.gender=='m'){
                   var g='男'
                }
              else{
                   var g ='女'
                }
              html += "<br /><b>性别:</b> " + g;
            }
            if("verified" in data) {
              if (data.verified){
                   var g='认证用户'
                }
              else{
                   var g ='非认证用户'
                }
              html += "<br /> "+g;
            }
            if("id" in data) {
              html += "<br /> <b>用户id:</b> "+ data.id;
            }
            
            $jit.id('inner-details').innerHTML = html;
            //hide tip
            sb.tips.hide();
            //rotate
            sb.rotate(node, animate? 'animate' : 'replot', {
              duration: 1000,
              transition: $jit.Trans.Quart.easeInOut
            });
          }
        },
        // Only used when Label type is 'HTML' or 'SVG'
        // Add text to the labels. 
        // This method is only triggered on label creation
        onCreateLabel: function(domElement, node){
          var labels = sb.config.Label.type,
              aw = node.getData('angularWidth');
          if (labels === 'HTML' && (node._depth < 2 || aw > 2000)) {
            domElement.innerHTML = node.name;
          } else if (labels === 'SVG' && (node._depth < 2 || aw > 2000)) {
            domElement.firstChild.appendChild(document.createTextNode(node.name));
          }
        },
        // Only used when Label type is 'HTML' or 'SVG'
        // Change node styles when labels are placed
        // or moved.
        onPlaceLabel: function(domElement, node){
          var labels = sb.config.Label.type;
          if (labels === 'SVG') {
            var fch = domElement.firstChild;
            var style = fch.style;
            style.display = '';
            style.cursor = 'pointer';
            style.fontSize = "0.8em";
            fch.setAttribute('fill', "#fff");
          } else if (labels === 'HTML') {
            var style = domElement.style;
            style.display = '';
            style.cursor = 'pointer';
            style.fontSize = "0.8em";
            style.color = "#ddd";
            var left = parseInt(style.left);
            var w = domElement.offsetWidth;
            style.left = (left - w / 2) + 'px';
          }
        }
   });
    //load JSON data.
    sb.loadJSON(json);
    //compute positions and plot.
    sb.refresh();
    //end
}
