
class outline_viewer {
	constructor(){
		this._name = "oviewer_"+(outline_viewer._instance_counter++);

		document.write("<div class='treeoutline' id='" + this._name + "'> </div>");
	}
}
outline_viewer.prototype.notify = function(e) {
	function is_dict(v) {
		return typeof v==='object' && v!==null && !(v instanceof Array) && !(v instanceof Date);
	}	
	function tree(obj, instname, level=0){
		var inner_elems = ""
		var kv = 0;
		for (var key in obj){
			if (obj.hasOwnProperty(key)) {
				if (is_dict(obj[key])){
					var ind = level * 5;
					var w = 100 - ind;
					var css = "width: " + w + "%; pad-left: " + ind + "%; text-align: left;";
					btnname = instname + "__" + level + "__" + kv;
					var elem = "<button id=\"" + btnname + "\" style=\"" + css + "\"> + " + key +" </button>";
					inner_elems = inner_elems + elem + tree(obj[key], instname, level + 1);
				}
			}
			++kv;
		}
		inner_elems = "<div style=\"text-align: right;\">" + inner_elems + "</div>"
		return inner_elems;
	}
	innerhtml = tree(e.data, this._name);
	document.getElementById(this._name).innerHTML = innerhtml;
	function register(obj, callback, instname, level=0){
		var kv = 0;
		for (var key in obj) {
			if (obj.hasOwnProperty(key)){
				if (is_dict(obj[key])) {
					btnname = instname + "__" + level + "__" + kv;
					btninst = document.getElementById(btnname);
					btninst.onclick = ev => callback(ev);
					register(obj[key], callback, instname, level + 1);
				}
			}
			++kv;
		}
	}
	register(e.data, this.on_select, this._name);
}
outline_viewer.prototype.on_select = function(e) {
	alert(e.target.id);
	console.log(e.target);
}
outline_viewer._instance_counter = 0;
