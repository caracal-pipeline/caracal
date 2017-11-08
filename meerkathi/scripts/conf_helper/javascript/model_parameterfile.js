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
	}
	load(file) {
		var fconf = new FileReader();
		this._fn = file.name;
		fconf.onload = ev => this.on_load_handler(ev);
		try {
			fconf.readAsText(file);
		} catch (err) {
			for (var i = 0; i < this._errviewers.length; ++i){
				this._errviewers[i].notify_err("Cannot open file '"+ this._fn + "'. Verify that it is a valid yaml file",
					err);
			}
		}		
	}
	get data() {
		return this._dict;
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
	}
	parameter_file.prototype.toString = function() {
	return JSON.stringify(this._dict, null, 4);
	};
	parameter_file.prototype.on_load_handler = function(e) {
	try {
		this._dict = jsyaml.safeLoad(e.target.result);
		for (var i = 0; i < this._viewers.length; ++i){
			this._viewers[i].notify(this);
		}		
	} catch(err) {
		for (var i = 0; i < this._errviewers.length; ++i){
			this._errviewers[i].notify_err("Cannot open file '"+ this._fn + "'. Verify that it is a valid yaml file",
				err);
		}
	}
};

