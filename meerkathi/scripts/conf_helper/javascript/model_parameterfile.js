/*
 * MeerKATHI yaml configuration file model-controller
 * (C) Benjamin Hugo, SKA-SA. 2017
 *
 */

class parameter_file {
	/*
	 * YAML config loader (model-controller)
	 */
	constructor(){
		this._fn = "";
		this._dict = "";
		this._viewers = [];
		this._errviewers = [];
		this._subtree_sel = [0];
	}
	load(file) {
		/*
		 * file - either:
		 *        a file handle to a yaml file: will load with jsyaml
		 *        or: a JSON string of externally parsed yaml
		 */
		if (typeof file == "string"){
		    this._fn = "new.yml";
		    try {
			this._dict = JSON.parse(file);
				for (var i = 0; i < this._viewers.length; ++i){
					this._viewers[i].notify("load", this);
				}		
		    } catch (err) {
			for (var i = 0; i < this._errviewers.length; ++i){
			    this._errviewers[i].notify_err("Cannot parse string. Verify that it is valid JSON.",
				err);
			}
		    }
		} else {
			var fconf = new FileReader();
			this._fn = file.name;
			fconf.onload = ev => this.on_load_handler(ev);
			try {
				fconf.readAsText(file);
			} catch (err) {
				for (var i = 0; i < this._errviewers.length; ++i){
					this._errviewers[i].notify_err("Cannot open file '"+ this._fn + "'. Verify that it is a valid yaml file", err);
			}
		    }
		}
	}

	get data() {
		return JSON.parse(JSON.stringify(this._dict));
	}

	get subtree_select() {
		return this._subtree_sel.slice();
	}

	get json_file() {
		return new Blob([JSON.stringify(this._dict, null, 4)], {type: "application/json"});
	}

	get yaml_file(){
		return new Blob([jsyaml.safeDump(this._dict)], {type: "text/yaml"});
	}

	set subtree_select(newsel) {
		/*
		 * Selects a subtree
		 * newsel - an array with full path to the tree root
		 * postcondition - notifies registered viewers of a subtree selection change
		 */
		if (!Array.isArray(newsel))
			throw "Expected array for new selection";
		var d = this._dict;
		for (var i = 0; i < newsel.length; ++i) {
			if (!(typeof d == "object" && Object.keys(d).length > newsel[i])){
				throw "New selection out of bounds for level " + i + ": " + newsel[i];
			} else d = d[Object.keys(d)[newsel[i]]];
		}
		this._subtree_sel = newsel.slice();
		//notify all of new subtree selection
		for (var i = 0; i < this._viewers.length; ++i){
			this._viewers[i].notify("subtree_select", this);
		}
	}

	update_key(level, value) {
		/*
		 * Update a leaf value
		 * level - an array with full path to tree root of leaf to modify
		 * postcondition - notifies registered viewers of a leaf change
		 */
		try {
			if (!Array.isArray(level))
				throw "Expected array for new selection";
			var d = this._dict;
			for (var i = 0; i < level.length; ++i) {
				if (!(typeof d == "object" && Object.keys(d).length > level[i])){
					throw "New selection out of bounds for level " + i + ": " + level[i];
				} else d = i < level.length - 1 ? d[Object.keys(d)[level[i]]] : d;
			}
			d[Object.keys(d)[level[level.length - 1]]] = value;
			//notify all observers of the key update 
			for (var i = 0; i < this._viewers.length; ++i){
				this._viewers[i].notify("key_update", this);
			}
		} catch(err) {
			for (var i = 0; i < this._errviewers.length; ++i){
				this._errviewers[i].notify_err("", err);
			}	
		}
	}

	register_viewer(viewer) {
		/*
		 * registers a viewer object to this model controller
		 */
		if (typeof viewer !== "object") {
			throw "Viewer not an object";
		}
		if (typeof viewer.notify !== "function") {
			throw "Viewer doesn't expose notify";
		}
		this._viewers.push(viewer);
		if (typeof viewer.notify_err == "function") {
			this._errviewers.push(viewer);
		}
	}
};

parameter_file.prototype.toString = function() {
	return JSON.stringify(this._dict, null, 4);
};

parameter_file.prototype.on_load_handler = function(e) {
	try {
		this._dict = jsyaml.safeLoad(e.target.result);
		for (var i = 0; i < this._viewers.length; ++i){
			this._viewers[i].notify("load", this);
		}		
	} catch(err) {
		for (var i = 0; i < this._errviewers.length; ++i){
			this._errviewers[i].notify_err("Cannot open file '"+ this._fn + "'. Verify that it is a valid yaml file",
				err);
		}
	}
};

