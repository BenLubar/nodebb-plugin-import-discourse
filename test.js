var fs = require('fs-extra');
var async = require('async');
var Exporter = require('./index');

function getAll(name, callback) {
	var start = 0;
	var limit = 500000;
	var done = false;
	var result = {};
	var fn = Exporter['getPaginated' + name];
	if (!fn) {
		throw 'Missing ' + name;
	}

	async.whilst(
		function(err) {
			if (err) {
				return true;
			}
			return !done;
		},
		function(next) {
			fn(start, limit, function(err, map) {
				if (err) {
					return next(err);
				}
				done = true;
				for (var i in map) {
					if (map.hasOwnProperty(i)) {
						result[i] = map[i];
						done = false;
						start++;
					}
				}
				if (done) {
					console.log(name, start);
				}
				next();
			});
		},
		function(err) {
			callback(err, result);
		}
	);
}

async.series([
	function(next) {
		Exporter.setup({
			dbhost: 'localhost',
			dbport: 5432,
			dbname: 'discourse',
			dbuser: 'postgres',
			dbpass: '',

			tablePrefix: 'restore.',
		}, next);
	},
	function(next) {
		getAll('Groups', next);
	},
	function(next) {
		getAll('Users', next);
	},
	function(next) {
		getAll('Categories', next);
	},
	function(next) {
		getAll('Topics', next);
	},
	function(next) {
		getAll('Posts', next);
	},
	function(next) {
		getAll('Votes', next);
	},
	function(next) {
		Exporter.teardown(next);
	}
], function(err, results) {
	if (err) {
		throw err;
	}

	fs.writeFileSync('./results.json', JSON.stringify(results, undefined, '\t'));
	process.exit(0);
});
