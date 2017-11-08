/*
 * MeerKATHI yaml configuration file console viewer (debug)
 * (C) Benjamin Hugo, SKA-SA. 2017
 *
 */

class console_viewer {
	constructor() {
	}
}
console_viewer.prototype.notify = function(e) {
	console.log(e.toString());
}
console_viewer.prototype.notify_err = function(msg, e){
	console.log(msg);
	throw e;
}

