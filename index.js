var async = require('async');
var pg = require('pg');

(function(Exporter) {
	var _table_prefix;
	var _url;
	Exporter.setup = function(config, callback) {
		_table_prefix = config.tablePrefix;
		_url = "postgres://" + encodeURIComponent(config.dbuser) + ":" + encodeURIComponent(config.dbpass) + "@" + encodeURIComponent(config.dbhost) + ":" + config.dbport + "/" + encodeURIComponent(config.dbname);
		callback(null, config);
	};

	Exporter.getPaginatedUsers = function(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query('SELECT u.id AS _uid, u.email AS _email, u.username AS _username, u.created_at AS _joindate, u.name AS _fullname, u.blocked::int AS _banned, p.website AS _website, p.location AS _location, u.views AS _profileviews, CASE WHEN u.admin THEN \'administrator\' WHEN u.moderator THEN \'moderator\' ELSE \'\' END AS _level, s.likes_received AS _reputation, \'/users/\' || u.username_lower AS _path FROM ' + _table_prefix + 'users AS u LEFT JOIN ' + _table_prefix + 'user_profiles AS p ON u.id = p.user_id LEFT JOIN ' + _table_prefix + 'user_stats AS s ON u.id = s.user_id ORDER BY _uid ASC LIMIT $1::int OFFSET $2::int', [limit, start], function(err, result) {
				done(err);

				if (err) {
					return callback(err);
				}

				var users = {};

				result.rows.forEach(function(row) {
					row._joindate = +row._joindate;
					users[row._uid] = row;
				});

				callback(null, users);
			});
		});
	};

	Exporter.getPaginatedCategories = function(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query('SELECT c.id AS _cid, c.name AS _name, c.description AS _description, c."position" AS _order, c.slug AS _slug, c.parent_category_id AS _parentCid, \'/c/\' || CASE WHEN c.parent_category_id IS NULL THEN \'\' ELSE (SELECT p.slug FROM ' + _table_prefix + 'categories AS p WHERE p.id = c.parent_category_id) || \'/\' END || c.slug AS _path FROM ' + _table_prefix + 'categories AS c ORDER BY _cid ASC LIMIT $1::int OFFSET $2::int', [limit, start], function(err, result) {
				done(err);

				if (err) {
					return callback(err);
				}

				var categories = {};

				result.rows.forEach(function(row) {
					categories[row._cid] = row;
				});

				callback(null, categories);
			});
		});
	};

	Exporter.getPaginatedTopics = function(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query('SELECT t.id AS _tid, t.user_id AS _uid, t.category_id AS _cid, t.title AS _title, p.raw AS _content, t.created_at AS _timestamp, t.views AS _viewcount, CASE WHEN t.closed THEN 1 ELSE 0 END AS _locked, CASE WHEN t.deleted_at IS NULL THEN 0 ELSE 1 END AS _deleted, CASE WHEN t.pinned_at IS NULL THEN 0 ELSE 1 END AS _pinned FROM ' + _table_prefix + 'topics AS t INNER JOIN ' + _table_prefix + 'posts AS p ON p.topic_id = t.id AND p.post_number = 1 ORDER BY _tid ASC LIMIT $1::int OFFSET $2::int', [limit, start], function(err, result) {
				done(err);

				if (err) {
					return callback(err);
				}

				var topics = {};

				result.rows.forEach(function(row) {
					row._timestamp = +row._timestamp;
					topics[row._tid] = row;
				});

				callback(null, topics);
			});
		});
	};

	Exporter.teardown = function(callback) {
		callback();
	};
})(module.exports);
