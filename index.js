var async = require('async');
var pg = require('pg');

(function(Exporter) {
	var _table_prefix;
	var _url;
	var _config;

	var allowed_keys = {
		"user_id_greater": function(x) { return parseInt(x, 10); },
		"user_created_after": function(x) { return new Date(x); },
		"user_where": function(x) { return String(x); },
		"topic_id_greater": function(x) { return parseInt(x, 10); },
		"topic_created_after": function(x) { return new Date(x); },
		"topic_where": function(x) { return String(x); },
		"post_id_greater": function(x) { return parseInt(x, 10); },
		"post_created_after": function(x) { return new Date(x); },
		"post_where": function(x) { return String(x); },
		"vote_id_greater": function(x) { return parseInt(x, 10); },
		"vote_created_after": function(x) { return new Date(x); },
		"vote_where": function(x) { return String(x); },
		"bookmark_id_greater": function(x) { return parseInt(x, 10); },
		"bookmark_created_after": function(x) { return new Date(x); },
		"bookmark_where": function(x) { return String(x); },
	};

	Exporter.setup = function(config, callback) {
		_table_prefix = config.tablePrefix;
		_url = "postgres://" + encodeURIComponent(config.dbuser) + ":" + encodeURIComponent(config.dbpass) + "@" + encodeURIComponent(config.dbhost) + ":" + config.dbport + "/" + encodeURIComponent(config.dbname);

		_config = {};
		if (!Object.keys(config.custom).every(function(key) {
			if (key in allowed_keys) {
				try {
					_config[key] = allowed_keys[key](config.custom[key]);
					if (_config[key] != _config[key]) {
						throw "not a number";
					}
					return true;
				} catch (e) {
					callback("Error in key " + key + ": " + e);
					return false;
				}
			}
			callback("Illegal custom key: " + key);
			return false;
		})) {
			return;
		}

		callback(null, config);
	};

	Exporter.getPaginatedGroups = function(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query({
				text: 'SELECT ' +
				'g.id AS _gid, ' +
				'g.name AS _name, ' +
				'g.created_at AS _timestamp ' +
				'FROM ' + _table_prefix + 'groups AS g ' +
				'WHERE g.id >= 10 ' +
				'ORDER BY _gid ASC ' +
				'LIMIT $1::int ' +
				'OFFSET $2::int',
				types: ["int", "int"]
			}, [limit, start], function(err, result) {
				done(err);

				if (err) {
					return callback(err);
				}

				var groups = {};

				result.rows.forEach(function(row) {
					row._timestamp = +row._timestamp;
					groups[row._gid] = row;
				});

				callback(null, groups);
			});
		});
	};

	Exporter.getPaginatedUsers = function(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query({
				text: 'SELECT ' +
				'u.id AS _uid, ' +
				'u.email AS _email, ' +
				'u.username AS _username, ' +
				'u.created_at AS _joindate, ' +
				'u.name AS _fullname, ' +
				'u.blocked::int AS _banned, ' +
				'p.website AS _website, ' +
				'p.location AS _location, ' +
				'u.views AS _profileviews, ' +
				'CASE ' +
					'WHEN u.admin THEN \'administrator\' ' +
					'WHEN u.moderator THEN \'moderator\' ' +
					'ELSE \'\' ' +
				'END AS _level, ' +
				'ARRAY(SELECT g.group_id FROM ' + _table_prefix + 'group_users AS g WHERE g.user_id = u.id AND g.group_id >= 10 ORDER BY g.group_id ASC) AS _groups, ' +
				's.likes_received AS _reputation, ' +
				'\'/users/\' || u.username_lower AS _path ' +
				'FROM ' + _table_prefix + 'users AS u ' +
				'LEFT JOIN ' + _table_prefix + 'user_profiles AS p ' +
				'ON u.id = p.user_id ' +
				'LEFT JOIN ' + _table_prefix + 'user_stats AS s ' +
				'ON u.id = s.user_id ' +
				'WHERE u.id > $3::int ' +
				'AND u.created_at > $4::timestamp ' +
				("user_where" in _config ? 'AND (' + _config["user_where"] + ') ' : '') +
				'ORDER BY _uid ASC ' +
				'LIMIT $1::int ' +
				'OFFSET $2::int',
				types: ["int", "int", "int", "timestamp"]
			}, [limit, start, _config["user_id_greater"] || -1, _config["user_created_after"] || new Date(0)], function(err, result) {
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

			client.query({
				text: 'SELECT ' +
				'c.id AS _cid, ' +
				'c.name AS _name, ' +
				'c.description AS _description, ' +
				'c."position" AS _order, ' +
				'c.slug AS _slug, ' +
				'c.parent_category_id AS _parentCid, ' +
				'\'/c/\' || CASE ' +
					'WHEN c.parent_category_id IS NULL THEN \'\' ' +
					'ELSE (SELECT p.slug FROM ' + _table_prefix + 'categories AS p WHERE p.id = c.parent_category_id) || \'/\' ' +
				'END || c.slug AS _path ' +
				'FROM ' + _table_prefix + 'categories AS c ' +
				'ORDER BY _cid ASC ' +
				'LIMIT $1::int ' +
				'OFFSET $2::int',
				types: ["int", "int"]
			}, [limit, start], function(err, result) {
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

			client.query({
				text: 'SELECT ' +
				't.id AS _tid, ' +
				'p.id AS _pid, ' +
				't.user_id AS _uid, ' +
				't.category_id AS _cid, ' +
				't.title AS _title, ' +
				'p.raw AS _content, ' +
				't.created_at AS _timestamp, ' +
				't.views AS _viewcount, ' +
				't.closed::int AS _locked, ' +
				'p.updated_at AS _edited, ' +
				'CASE WHEN p.deleted_at IS NULL 0 ELSE 1 END AS _deleted, ' +
				'p.like_count AS _votes, ' +
				'p.like_count AS _reputation, ' +
				'(t.pinned_at IS NOT NULL)::int AS _pinned ' +
				'FROM ' + _table_prefix + 'topics AS t ' +
				'INNER JOIN ' + _table_prefix + 'posts AS p ' +
				'ON p.topic_id = t.id AND p.post_number = 1 ' +
				'WHERE t.archetype = \'regular\' ' +
				'AND t.id > $3::int ' +
				'AND t.created_at > $4::timestamp ' +
				("topic_where" in _config ? 'AND (' + _config["topic_where"] + ') ' : '') +
				'ORDER BY _tid ASC ' +
				'LIMIT $1::int ' +
				'OFFSET $2::int',
				types: ["int", "int", "int", "timestamp"]
			}, [limit, start, _config["topic_id_greater"] || -1, _config["topic_created_after"] || new Date(0)], function(err, result) {
				done(err);

				if (err) {
					return callback(err);
				}

				var topics = {};

				result.rows.forEach(function(row) {
					row._timestamp = +row._timestamp;
					row._edited = +row._edited;
					topics[row._tid] = row;
				});

				callback(null, topics);
			});
		});
	};

	Exporter.getPaginatedPosts = function(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query({
				text: 'SELECT ' +
				'p.id AS _pid, ' +
				'p.topic_id AS _tid, ' +
				'p.user_id AS _uid, ' +
				'p.raw AS _content, ' +
				'p.created_at AS _timestamp, ' +
				'p.updated_at AS _edited, ' +
				'CASE WHEN p.deleted_at IS NULL 0 ELSE 1 END AS _deleted, ' +
				'p.like_count AS _votes, ' +
				'p.like_count AS _reputation ' +
				'FROM ' + _table_prefix + 'posts AS p ' +
				'LEFT JOIN ' + _table_prefix + 'topics AS t ' +
				'ON p.topic_id = t.id ' +
				'WHERE p.post_number <> 1 AND t.archetype = \'regular\' ' +
				'AND p.id > $3::int ' +
				'AND p.created_at > $4::timestamp ' +
				("post_where" in _config ? 'AND (' + _config["post_where"] + ') ' : '') +
				'ORDER BY _pid ASC ' +
				'LIMIT $1::int ' +
				'OFFSET $2::int',
				types: ["int", "int", "int", "timestamp"]
			}, [limit, start, _config["post_id_greater"] || -1, _config["post_created_after"] || new Date(0)], function(err, result) {
				done(err);

				if (err) {
					return callback(err);
				}

				var posts = {};

				result.rows.forEach(function(row) {
					row._timestamp = +row._timestamp;
					row._edited = +row._edited;
					posts[row._pid] = row;
				});

				callback(null, posts);
			});
		});
	};

	Exporter.getPaginatedVotes = function(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query({
				text: 'SELECT ' +
				'a.id AS _vid, ' +
				'a.post_id AS _pid, ' +
				'p.topic_id AS _tid, ' +
				'p.post_number AS _pn, ' +
				'a.user_id AS _uid, ' +
				'1 as _action ' +
				'FROM ' + _table_prefix + 'post_actions AS a ' +
				'INNER JOIN ' + _table_prefix + 'posts AS p ' +
				'ON a.post_id = p.id ' +
				'WHERE a.post_action_type_id = (SELECT t.id FROM ' + _table_prefix + 'post_action_types AS t WHERE t.name_key = \'like\') ' +
				'AND a.id > $3::int ' +
				'AND a.created_at > $4::timestamp ' +
				("vote_where" in _config ? 'AND (' + _config["vote_where"] + ') ' : '') +
				'ORDER BY _vid ASC ' +
				'LIMIT $1::int ' +
				'OFFSET $2::int',
				types: ["int", "int", "int", "timestamp"]
			}, [limit, start, _config["vote_id_greater"] || -1, _config["vote_created_after"] || new Date(0)], function(err, result) {
				done(err);

				if (err) {
					return callback(err);
				}

				var votes = {};

				result.rows.forEach(function(row) {
					if (row._pn == 1) {
						delete row._pid;
					} else {
						delete row._tid;
					}
					delete row._pn;
					votes[row._vid] = row;
				});

				callback(null, votes);
			});
		});
	};

	Exporter.getPaginatedBookmarks = function(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query({
				text: 'SELECT ' +
				'a.id AS _bid, ' +
				'a.post_id AS _pid, ' +
				'p.topic_id AS _tid, ' +
				'a.user_id AS _uid, ' +
				'p.post_number - 1 AS _index ' +
				'FROM ' + _table_prefix + 'post_actions AS a ' +
				'INNER JOIN ' + _table_prefix + 'posts AS p ' +
				'ON a.post_id = p.id ' +
				'WHERE a.post_action_type_id = (SELECT t.id FROM ' + _table_prefix + 'post_action_types AS t WHERE t.name_key = \'bookmark\') ' +
				'AND a.id > $3::int ' +
				'AND a.created_at > $4::timestamp ' +
				("bookmark_where" in _config ? 'AND (' + _config["bookmark_where"] + ') ' : '') +
				'ORDER BY _bid ASC ' +
				'LIMIT $1::int ' +
				'OFFSET $2::int',
				types: ["int", "int", "int", "timestamp"]
			}, [limit, start, _config["bookmark_id_greater"] || -1, _config["bookmark_created_after"] || new Date(0)], function(err, result) {
				done(err);

				if (err) {
					return callback(err);
				}

				var bookmarks = {};

				result.rows.forEach(function(row) {
					bookmarks[row._bid] = row;
				});

				callback(null, bookmarks);
			});
		});
	};

	Exporter.teardown = function(callback) {
		callback();
	};
})(module.exports);
