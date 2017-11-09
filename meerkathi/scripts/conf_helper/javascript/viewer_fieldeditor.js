
function is_dict(v) {
	return typeof v==='object' && v!==null && !(v instanceof Array) && !(v instanceof Date);
}


class fieldeditor_viewer {
	constructor(parent){
		this._name = "feditor_"+(fieldeditor_viewer._instance_counter++);
		this._model = null;
		this._children = [];
		this._init = false;
		parent.innerHTML += "<div class='opteditor' id='" + this._name + "'> </div>";
	}
}
fieldeditor_viewer.prototype.notify = function(type, payload) {
	if (type == "load" || type == "subtree_select"){
		if (this._init) {
			document.getElementById(this._name).innerHTML = "";
			this._children = [];
		}
		this._init = true;
		// add an indented button for each level of the tree
		// (depth first recursion down to leafs)
		function fillform(obj, instname, model, level=0){
			var inner_elems = "";
			var kv = 0;
			for (var key in obj){
				if (obj.hasOwnProperty(key)) {
					if (is_dict(obj[key])){
						nm = instname + "__" + kv;
						inner_elems = inner_elems + fillform(obj[key], nm, model, level + 1);
					} else {
						sel = model.subtree_select;
						fieldfamily = instname.split("__").reverse();
						fieldfamily.pop();
						fieldfamily = fieldfamily.reverse();
						if (fieldfamily.length == sel.length) {
							var ischild = true;
							for (var i = fieldfamily.length - 1; i >= 0; --i){
								if (fieldfamily[i] != sel[i]){
									ischild = false;
									break;
								}
							}
							if (ischild) {

								lblname = instname + "__" + kv + "__label";
								edtname = instname + "__" + kv;
								lookup = {"undefined" : "text",
									  "object" : "text",
									  "boolean" : "checkbox",
									  "number"  : "number",
									  "string"  : "text"};
								accessor = {"undefined" : "value=\""+obj[key]+"\"",
									    "object" : "value=\""+obj[key]+"\"",
 									    "boolean" : "checked",
									    "number"  : "value=\""+obj[key]+"\"",
									    "string"  : "value=\""+obj[key]+"\""};

								var edt = "<input type=\"" + lookup[typeof obj[key]] + "\" "+
									  "id=\"" + edtname + 
									  "\" class=\"editoreditbox\" " +
									  accessor[typeof obj[key]] + "/>";
								var lbl = "<label id=\"" + lblname + 
									  "\" class=\"editorlabel\"> " + 
									  key.replace(/-/g," round ").replace(/_/g, " ") + 
									  " </label>";
								inner_elems = inner_elems + 
									      "<div class=\"editorlabelsep\">" + 
									      lbl + edt + 
									      " </div>";
							}
						}
					}
				}
				++kv;
			}
			inner_elems = "<div style=\"text-align: left;\">" + inner_elems + "</div>"
			return inner_elems;
		}
		innerhtml = fillform(payload.data, this._name, payload);	
		document.getElementById(this._name).innerHTML = innerhtml;
		// register callback for buttons
		// (depth first recursion down to leafs)
		function register(obj, callback, instname, inst, model, level=0){
			var kv = 0;
			res = [];
			for (var key in obj) {
				if (obj.hasOwnProperty(key)){
					if (is_dict(obj[key])) {
						nm = instname + "__" + kv;
						res.concat(register(obj[key], callback, nm, inst, model, level + 1));
					} else {
						sel = model.subtree_select;
						fieldfamily = instname.split("__").reverse();
						fieldfamily.pop();
						fieldfamily = fieldfamily.reverse();
						if (fieldfamily.length == sel.length) {
							var ischild = true;
							for (var i = fieldfamily.length - 1; i >= 0; --i){
								if (fieldfamily[i] != sel[i]){
									ischild = false;
									break;
								}
							}
							if (ischild) {
								lblname = instname + "__" + kv + "__label";
								edtname = instname + "__" + kv;
								edt = document.getElementById(edtname);
								lbl = document.getElementById(lblname);
								edt.onchange = ev => callback(ev, inst);
								res.push(edt);
								res.push(lbl);
							}
						}
					}
				}
				++kv;
			}
			return res;
		}
		this._children.concat(register(payload.data, this.on_change, this._name, this, payload));

		// update reference to model to which this viewer is registered as observer
		this._model = payload;
	}
};

fieldeditor_viewer.prototype.on_change = function(e, inst) {
	// get selected and its ancestors (from button id)
	sel = e.target.id.split("__").reverse();
	sel.pop();
	sel = sel.reverse();
	// collapse everything except the buttons which parents are in the selected subtree
        nv = (typeof e.target.checked == "boolean") ? e.target.checked : e.target.value;
	inst._model.update_key(sel, nv);
};
fieldeditor_viewer._instance_counter = 0;
