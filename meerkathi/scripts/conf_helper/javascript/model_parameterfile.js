/*
 * MeerKATHI yaml configuration file model-controller
 * (C) Benjamin Hugo, SKA-SA. 2017
 *
 */

class parameter_file {
	constructor(){
		this._fn = "";
		this._dict = "";
		this._viewers = [];
		this._errviewers = [];
		this._subtree_sel = [0];
	}
	load(file) {
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

	set subtree_select(newsel) {
		if (!Array.isArray(newsel))
			throw "Expected array for new selection";

		for (var i = 0; i < newsel.length; ++i) {
			if (!(typeof this._dict == "object" && Object.keys(this._dict).length > newsel[i])){
				throw "New selection out of bounds for level " + i + ": " + newsel[i];
			}
		}
		this._subtree_sel = newsel.slice();
		//notify all of new subtree selection
		for (var i = 0; i < this._viewers.length; ++i){
			this._viewers[i].notify("subtree_select", this);
		}		
	}

	register_viewer(viewer) {
		if (typeof viewer !== "object") {
			throw "Viewer not an object";
		}
		if (typeof viewer.notify !== "function") {
			throw "Viewer doesn't expose notify";
		}
		this._viewers.push(viewer);
		if (typeof viewer.notify_err === "function") {
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

