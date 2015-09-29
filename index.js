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

	Exporter.getPaginatedGroups = function(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query('SELECT ' +
				'g.id AS _gid, ' +
				'g.name AS _name, ' +
				'g.created_at AS _timestamp ' +
				'FROM ' + _table_prefix + 'groups AS g ' +
				'WHERE g.id >= 10 ' +
				'ORDER BY _gid ASC ' +
				'LIMIT $1::int ' +
				'OFFSET $2::int', [limit, start], function(err, result) {
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

			client.query('SELECT ' +
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
				'ORDER BY _uid ASC ' +
				'LIMIT $1::int ' +
				'OFFSET $2::int', [limit, start], function(err, result) {
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

			client.query('SELECT ' +
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
				'OFFSET $2::int', [limit, start], function(err, result) {
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

			client.query('SELECT ' +
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
				'p.deleted_at AS _deleted, ' +
				'p.like_count AS _votes, ' +
				'p.like_count AS _reputation, ' +
				'(t.pinned_at IS NOT NULL)::int AS _pinned ' +
				'FROM ' + _table_prefix + 'topics AS t ' +
				'INNER JOIN ' + _table_prefix + 'posts AS p ' +
				'ON p.topic_id = t.id AND p.post_number = 1 ' +
				'WHERE t.archetype = \'regular\' ' +
				'ORDER BY _tid ASC ' +
				'LIMIT $1::int ' +
				'OFFSET $2::int', [limit, start], function(err, result) {
				done(err);

				if (err) {
					return callback(err);
				}

				var topics = {};

				result.rows.forEach(function(row) {
					row._timestamp = +row._timestamp;
					row._edited = +row._edited;
					row._deleted = +row._deleted;
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

			client.query('SELECT ' +
				'p.id AS _pid, ' +
				'p.topic_id AS _tid, ' +
				'p.user_id AS _uid, ' +
				'p.raw AS _content, ' +
				'p.created_at AS _timestamp, ' +
				'p.updated_at AS _edited, ' +
				'p.deleted_at AS _deleted, ' +
				'p.like_count AS _votes, ' +
				'p.like_count AS _reputation ' +
				'FROM ' + _table_prefix + 'posts AS p ' +
				'LEFT JOIN ' + _table_prefix + 'topics AS t ' +
				'ON p.topic_id = t.id ' +
				'WHERE p.post_number <> 1 AND t.archetype = \'regular\' ' +
				'ORDER BY _pid ASC ' +
				'LIMIT $1::int ' +
				'OFFSET $2::int', [limit, start], function(err, result) {
				done(err);

				if (err) {
					return callback(err);
				}

				var posts = {};

				result.rows.forEach(function(row) {
					row._timestamp = +row._timestamp;
					row._edited = +row._edited;
					row._deleted = +row._deleted;
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

			client.query('SELECT ' +
				'a.id AS _vid, ' +
				'a.post_id AS _pid, ' +
				'p.topic_id AS _tid, ' +
				'a.user_id AS _uid ' +
				'FROM ' + _table_prefix + 'post_actions AS a ' +
				'INNER JOIN ' + _table_prefix + 'posts AS p ' +
				'ON a.post_id = p.id ' +
				'WHERE a.post_action_type_id = (SELECT t.id FROM ' + _table_prefix + 'post_action_types AS t WHERE t.name_key = \'like\') ' +
				'ORDER BY _vid ASC ' +
				'LIMIT $1::int ' +
				'OFFSET $2::int', [limit, start], function(err, result) {
				done(err);

				if (err) {
					return callback(err);
				}

				var votes = {};

				result.rows.forEach(function(row) {
					votes[row._vid] = row;
				});

				callback(null, votes);
			});
		});
	};

	Exporter.teardown = function(callback) {
		callback();
	};
})(module.exports);
