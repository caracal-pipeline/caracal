/*
 * meerkathi yaml configuration file tree drawer viewer
 * (c) benjamin hugo, ska-sa. 2017
 */

function is_dict(v) {
	return typeof v==='object' && v!==null && !(v instanceof Array) && !(v instanceof Date);
}


class outline_viewer {
    /*
     * Basic 'accordion'-style tree viewer
     * Should be registered to a yaml config modelcontroller
     */
	constructor(parent){
        /*
         * parent - DOM parent to which this control should be added
         */
		this._name = "oviewer_"+(outline_viewer._instance_counter++);
		this._model = null;
		this._init = false;
		this._children = [];
		parent.innerHTML += "<div class='treeoutline' id='" + this._name + "'> </div>";
	}
}
outline_viewer.prototype.notify = function(type, payload) {
	if (type == "load"){
		if (this._init) {
			document.getElementById(this._name).innerHTML = "";
			this._children = [];
		}

		this._init = true;
		// add an indented button for each level of the tree
		// (depth first recursion down to leafs)
		function tree(obj, instname, level=0){
			var inner_elems = ""
			var kv = 0;
			for (var key in obj){
				if (obj.hasOwnProperty(key)) {
					if (is_dict(obj[key])){
						marker = (!(obj[key].hasOwnProperty("enable")) ? "&squf;" : 
							  obj[key]["enable"] ? "&squf;" : "&curren;");
						var ind = level * 10;
						var w = 100 - ind;
						var disp = (level == 0) ? 'inline' : 'none';
						var css = "width: " + w + "%; " +
							  "pad-left: " + ind + 
							  "%; text-align: left; " + 
							  "display: " + disp + ";";
						btnname = instname + "__" + kv;
						var elem = "<button id=\"" + btnname + 
							   "\" style=\"" + css + 
							   "\" class=\"treeviewsel\"> " +
							   marker + "   " +
							   key.replace(/__/g," round ").replace(/_/g, " ") +
							   " </button>";
						inner_elems = inner_elems + elem + tree(obj[key], btnname, level + 1);
					}
				}
				++kv;
			}
			inner_elems = "<div style=\"text-align: right;\">" + inner_elems + "</div>"
			return inner_elems;
		}
		innerhtml = tree(payload.data, this._name);
		document.getElementById(this._name).innerHTML = innerhtml;
		// register callback for buttons
		// (depth first recursion down to leafs)
		function register(obj, callback, instname, inst, level=0){
			var kv = 0;
			res = [];
			for (var key in obj) {
				if (obj.hasOwnProperty(key)){
					if (is_dict(obj[key])) {
						btnname = instname + "__" + kv;
						btninst = document.getElementById(btnname);
						btninst.onclick = ev => callback(ev, inst);
						res.concat(register(obj[key], callback, btnname, inst, level + 1));
						res.push(btninst);
					}
				}
				++kv;
			}
			return res;
		}
		this._children.concat(register(payload.data, this.on_select, this._name, this));
		// update reference to model to which this viewer is registered as observer
		this._model = payload;
		this.on_select({"target":{"id":this._name + "__0"}}, this);
	} else if (type == "key_update") {
		function check_enabled(obj, instname, level=0){
			var kv = 0;
			for (var key in obj) {
				if (obj.hasOwnProperty(key)){
					if (is_dict(obj[key])) {
						marker = (!(obj[key].hasOwnProperty("enable")) ? "&squf;" : 
							  obj[key]["enable"] ? "&squf;" : "&curren;");

						btnname = instname + "__" + kv;
						btninst = document.getElementById(btnname);
						btninst.innerHTML = marker + "   " +
								    key.replace(/__/g," round ").replace(/_/g, " ");
						check_enabled(obj[key], btnname, level + 1);
					}
				}
				++kv;
			}
		}
		check_enabled(payload.data, this._name); 
	
	}
}
outline_viewer.prototype.on_select = function(e, inst) {
	// get selected and its ancestors (from button id)
	sel = e.target.id.split("__").reverse();
	sel.pop();
	sel = sel.reverse();
	// collapse everything except the buttons which parents are in the selected subtree
	function collapse_all(obj, instname, selected, level=0){
		var kv = 0;
		for (var key in obj) {
			if (obj.hasOwnProperty(key)){
				if (is_dict(obj[key])) {
					btnname = instname + "__" + kv;
					btnfamily = btnname.split("__").reverse();
					btnfamily.pop();
					btnfamily = btnfamily.reverse();
					//if the parent is selected then this option needs expanding
					//never hide roots
					var issel = btnfamily[level] == selected[level];
					var hide = false;
					for (var i = level - 1; i >= 0; --i){
						if (btnfamily[i] != selected[i]){
							hide = true;
							issel = false;
							break;
						}
					}
					btninst = document.getElementById(btnname);
					btninst.style.display = (hide) ? 'none' : 'inline';
					if (issel) { 
						btninst.setAttribute("disabled", true);
					} else {
						btninst.removeAttribute("disabled");
					}

					collapse_all(obj[key], btnname, selected, level + 1);
					

				}
			}
			++kv;
		}
	}
	collapse_all(inst._model.data, inst._name, sel);
	inst._model.subtree_select = sel;
}
outline_viewer._instance_counter = 0;
