function ycdcommon()
{
    this.Ajax = new Ajax();
    this.Win = new win();
    this.File = new File();
    this.version = "1.0";
    this.alert = new Alert();
    $.ajaxSetup({
    	 complete:function(xhr,textStatus){
	    	 if(xhr.status == 518)
	              window.location.href = xhr.responseText;
	    }
	});
}

/**
 * 获取带"/"的项目名，如：/v_portal
 * @return {}
 */
function getProjectName() {
    var pathName = window.document.location.pathname;
    var projectName = pathName.substring(0, pathName.substr(1).indexOf('/') + 1);
    return  projectName;
}

/**
 * 获取客户端浏览器分辨率
 *  add by zmx 2020-07-25
 * @type {{w: winWH.w, h: winWH.h}}
 */
var winWH = (function getWinWH() {
	var width = window.screen.width ;// 屏幕分辨率的宽
	var height = window.screen.height;// 屏幕分辨率的高
	function _w(x) {
		return width * (x?x:1);
	}
	function _h(y) {
		return height * (y?y:1);
	}
	return {w:function (x) {
				return _w(x);
			},h:function (y) {
				return _h(y);
			}};
})();
function Alert(){
	window.alert = function(msg){
		layer.alert(msg, {icon: 7});     
	};
}
function File(){
	this.downloadFile=function(filename)
	{
		var url = window.location.href;
	    if (url.toLowerCase().indexOf("http://")!=0)
		    url = "http://"+url;
	    var tmp = url.split("/");
		var webpath="";
		if (tmp.length>4)
			webpath=tmp[3];
		window.open("/"+webpath+"/DownloadServlet?filename="+encodeURI(filename),"_self");
	};
}
function Ajax()
{
	this.syncAjax=function(_url,_data){
		try{
			_data = _data||{};
			_data.isAjax = true;
			var result = $.ajax({
				type : "post", data:_data, cache : false, url : _url, async:false
			}).responseJSON;
			return result;
		}catch(err){
			return null;
		}
	};
}
function win() {
    this.alert=function (msg) {
        alert(msg);
    };
    this.getUrlParam=function(paramname){
    	var params = new Object();
        var aParams = document.location.search.substr(1).split('&') ;
        for (var i=0 ; i < aParams.length ; i++) {
    	    var aParam = aParams[i].split('=');
    	    params[aParam[0]] = aParam[1];
        }
    	
        try
        {
           if (params[paramname]==undefined)
              return "";
           else
              return decodeURI(params[paramname]);
        }catch(e){
           return "";
        }	
    };
}
var YCDCommon = new ycdcommon();
/**
 * 隐藏参数传递
 * 
 * postUrl(url,params):传递参数对象 例如: postUrl('login.jsp', {type:'out'})
 * 						如需新打开窗口 isNewWin传true
 * getPostUrl():获取参数对象,必须在元素加载完成使用
 * 
 * 下面代码放到getPostUrl()所在的jsp页面的body中-->此方法只应用于jsp不显示参数跳转
 * <%
	String url_Post=request.getParameter("url_Post");
	if(url_Post!=null&&url_Post!=""){
		url_Post = new String((url_Post).getBytes("ISO-8859-1"),"UTF-8");
	}else{
		url_Post="";
	}
	%>
	<p id='url_Post' style="display: none;"><%=url_Post%></p>
 * @return {}
 */
//1.要把页面的url后面显示的参数隐藏
//2.把传入参数的对象专为string转换新的value给其添加新的key
function postUrl(url,params,isNewWin,guid){
	var target="_self";
	if(isNewWin){
		target='_blank';//如需新打开窗口 form 的target属性要设置为'_blank'
	}
	if(guid){
		target=guid;
		debugger
		if(window.top.pageOpenObj){
			window.top.pageOpenObj[target]=target;
		}
	}
	//为了把params进行加工,处理value
    var strings = JSON.stringify(params);	
   	strings = encodeURI(strings);
	//重新构建params
	params={
		url_Post:strings
	};
    //创建form
    var temp_form = document.createElement("form");
    // 设置form属性
    temp_form.action = url;
    temp_form.target = target;
    temp_form.method = "post";
    temp_form.style.display="none";
    // 处理需要传递参数
    for(var x in params){
        var opt = document.createElement("textarea");
        opt.name = x;
        opt.value = params[x];
        temp_form.appendChild(opt);
    }
    document.body.appendChild(temp_form);
    temp_form.submit();
}
function getPostUrl(){
	var urlParams=$("#url_Post").text();
	urlParams=decodeURI(urlParams);
	return (urlParams=="")?"":JSON.parse(urlParams);
}

// 生成随机字符组合表名、字段名称等  
function randomString(len,pre,suf) {
	len = len || 26;
	var $chars = 'ABCDEFGHJKMNPQRSTWXYZ123456789_';
	var maxPos = $chars.length;
	var newChars = (pre || "") + "" ;
	for (i = 0; i < len; i++) {
		newChars += $chars.charAt(Math.floor(Math.random() * maxPos));
	}
	newChars += (suf || "") + "" ;
	return newChars;
}
