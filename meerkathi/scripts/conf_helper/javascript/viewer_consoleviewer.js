/*
 * MeerKATHI yaml configuration file console viewer (debug)
 * (C) Benjamin Hugo, SKA-SA. 2017
 *
 */

class console_viewer {
	/*
	 * Debugging viewer
	 *
	 * Need to be registered to a yaml config model controller
	 */
	constructor() {
	}
}
console_viewer.prototype.notify = function(type, payload) {
	console.log(type + ": " + payload);
}
console_viewer.prototype.notify_err = function(msg, e){
	console.log(msg);
	console.log(e);
}

