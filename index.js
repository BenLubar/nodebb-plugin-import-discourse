var async = require('async');
var pg = require('pg');
var mssql = require('mssql');
var db = module.parent.parent.require('./database');

(function(Exporter) {
	var _table_prefix;
	var _url;
	var _cs;
	var _config;
	var _imported;

	var allowed_keys = {
		"cs": function(x) { return x; },
		"skip_cs": function(x) { if (x !== true) throw "skip_cs can only be true if provided"; },
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

		if (_config.skip_cs) {
			_cs = null;
			return callback(null, config);
		}

		_cs = _config.cs;
		if (!_cs) {
			return callback("Need {\"cs\":\"community server connection string\"} in custom field");
		}

		mssql.connect(_cs).then(function(err) {
			if (err) {
				return callback(err);
			}

			getImportedIDs(config, callback);
		});
	};

	function getImportedIDs(config, callback) {
		_imported = {c: {}, u: {}, topics_offset: -1, posts_offset: -1};

		async.waterfall([
			function(next) {
				new mssql.Request().query('SELECT u.Email AS k, u.UserID AS v FROM dbo.cs_Users AS u', next);
			}, function(rows, next) {
				var user_emails = {};
				rows.forEach(function(row) {
					user_emails[row.k] = row.v;
				});

				next(null, user_emails);
			}, function(user_emails, next) {
				pg.connect(_url, function(err, client, done) {
					next(err, user_emails, client, done);
				});
			}, function(user_emails, client, done, next) {
				client.query('SELECT u.email AS k, u.id AS v FROM ' + _table_prefix + 'users AS u', function(err, result) {
					done(err);
					next(err, user_emails, result);
				});
			}, function(user_emails, result, next) {
				var scores = [], values = [];
				result.rows.forEach(function(row) {
					if (row.k in user_emails) {
						_imported.u[user_emails[row.k]] = row.v;
						scores.push(row.v);
						values.push(user_emails[row.k]);
					}
				});

				db.sortedSetAdd('_telligent:_users', scores, values, next);
			}, function(next) {
				pg.connect(_url, next);
			}, function(client, done, next) {
				client.query('SELECT f.value::int AS k, f.category_id AS v FROM ' + _table_prefix + 'category_custom_fields AS f WHERE f.name = \'import_id\'', function(err, result) {
					done(err);
					next(err, result);
				});
			}, function(result, next) {
				var scores = [], values = [];
				result.rows.forEach(function(row) {
					_imported.c[row.k] = row.v;
					scores.push(row.v);
					values.push(row.k);
				});

				db.sortedSetAdd('_telligent:_categories', scores, values, next);
			}
		], function(err) {
			callback(err, config);
		});
	}

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
				'\'/users/\' || u.username_lower AS _path, ' +
				'u.last_posted_at AS _lastposttime, ' +
				'u.last_seen_at AS _lastonline, ' +
				'f.url AS _picture ' +
				'FROM ' + _table_prefix + 'users AS u ' +
				'LEFT JOIN ' + _table_prefix + 'user_profiles AS p ' +
				'ON u.id = p.user_id ' +
				'LEFT JOIN ' + _table_prefix + 'user_stats AS s ' +
				'ON u.id = s.user_id ' +
				'LEFT JOIN ' + _table_prefix + 'uploads AS f ' +
				'ON f.id = u.uploaded_avatar_id ' +
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
					row._lastposttime = +row._lastposttime;
					row._lastonline = +row._lastonline;
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
				'c.parent_category_id AS "_parentCid", ' +
				'\'/c/\' || CASE ' +
					'WHEN c.parent_category_id IS NULL THEN \'\' ' +
					'ELSE (SELECT p.slug FROM ' + _table_prefix + 'categories AS p WHERE p.id = c.parent_category_id) || \'/\' ' +
				'END || c.slug AS _path, ' +
				'\'#\' || c.text_color AS _color, ' +
				'\'#\' || c.color AS "_bgColor" ' +
				'FROM ' + _table_prefix + 'categories AS c ' +
				'LEFT OUTER JOIN ' + _table_prefix + 'category_custom_fields AS f ' +
				'ON f.category_id = c.id AND f.name = \'import_id\' ' +
				'WHERE f.value IS NULL ' +
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

	// XXX: assumes topics are imported iff the first post is imported

	Exporter.getPaginatedTopics = function(start, limit, callback) {
		if (start === 0 && _imported.topics_offset !== 0) {
			_imported.topics_offset = -1;
		}
		if (_config.skip_cs) {
			_imported.topics_offset = 0;
		}
		if (_imported.topics_offset === -1) {
			var req = new mssql.Request();
			req.input('limit', mssql.Int, limit);
			req.input('start', mssql.Int, start);
			req.query('SELECT TOP @limit * ' +
				'FROM (SELECT ' +
				'ROW_NUMBER() OVER (ORDER BY t.ThreadID ASC) AS _rowid, '  +
				't.ThreadID * 2 AS _tid, ' +
				'p.PostID * 2 AS _pid, ' +
				't.UserID AS _uid, ' +
				't.PostAuthor AS _guest, ' +
				't.SectionID AS _cid, ' +
				'p.IPAddress AS _ip, ' +
				'p.Subject AS _title, ' +
				'p.Body AS _content, ' +
				't.PostDate AS _timestamp, ' +
				't.TotalViews AS _viewcount, ' +
				't.IsLocked AS _locked, ' +
				'1 - t.IsApproved AS _deleted, ' +
				't.IsSticky AS _pinned ' +
				'FROM dbo.cs_Threads AS t ' +
				'INNER JOIN dbo.cs_Posts AS p ' +
				'ON p.ThreadID = t.ThreadID AND p.ParentID = p.PostID ' +
				'WHERE p.PostType = 1 ' +
				'AND p.PostConfiguration = 0) AS topics' +
				'WHERE _rowid > @start ' +
				'ORDER BY t.ThreadID ASC', function(err, rows) {
				if (err) {
					return callback(err);
				}

				if (rows.length === 0) {
					_imported.topics_offset = start;
					return discoursePaginatedTopics(0, limit, callback);
				}

				var topics = {};

				rows.forEach(function(row) {
					delete row._rowid;
					if (row._uid === 1001) {
						delete row._uid;
					} else {
						row._uid = _imported.u[row._uid];
						delete row._guest;
					}
					row._cid = _imported.c[row._cid];
					row._timestamp = +row._timestamp;
					row._edited = +row._edited;
					topics[row._tid] = row;
				});

				callback(null, topics);
			});
		} else {
			discoursePaginatedTopics(start - _imported.topics_offset, limit, callback);
		}
	};

	function discoursePaginatedTopics(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query({
				text: 'SELECT ' +
				't.id * 2 + 1 AS _tid, ' +
				'p.id * 2 + 1 AS _pid, ' +
				't.user_id AS _uid, ' +
				't.category_id AS _cid, ' +
				't.title AS _title, ' +
				'p.raw AS _content, ' +
				't.created_at AS _timestamp, ' +
				't.views AS _viewcount, ' +
				't.closed::int AS _locked, ' +
				'p.updated_at AS _edited, ' +
				'CASE WHEN p.deleted_at IS NULL THEN 0 ELSE 1 END AS _deleted, ' +
				'(t.pinned_at IS NOT NULL)::int AS _pinned ' +
				'FROM ' + _table_prefix + 'topics AS t ' +
				'INNER JOIN ' + _table_prefix + 'posts AS p ' +
				'ON p.topic_id = t.id AND p.post_number = 1 ' +
				'LEFT OUTER JOIN ' + _table_prefix + 'topic_custom_fields AS f ' +
				'ON f.topic_id = t.id AND f.name = \'import_id\' ' +
				'WHERE t.archetype = \'regular\' ' +
				'AND f.value IS NULL ' +
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
	}

	// XXX: assumes imported posts never reply to non-imported posts
	// XXX: does not import tags, but they're mostly deleted anyway

	Exporter.getPaginatedPosts = function(start, limit, callback) {
		if (start === 0 && _imported.posts_offset !== 0) {
			_imported.posts_offset = -1;
		}
		if (_config.skip_cs) {
			_imported.posts_offset = 0;
		}
		if (_imported.posts_offset === -1) {
			var req = new mssql.Request();
			req.input('limit', mssql.Int, limit);
			req.input('start', mssql.Int, start);
			req.query('SELECT TOP @limit * ' +
				'FROM (SELECT ' +
				'ROW_NUMBER() OVER (ORDER BY p.PostID ASC) AS _rowid, '  +
				'p.ThreadID * 2 AS _tid, ' +
				'p.PostID * 2 AS _pid, ' +
				'p.UserID AS _uid, ' +
				'p.PostAuthor AS _guest, ' +
				'CASE WHEN p.ParentID = p.PostID THEN NULL ELSE p.ParentID END AS [_toPid], ' +
				'p.IPAddress AS _ip, ' +
				'CASE WHEN p.Subject = pp.Subject THEN \'\' WHEN p.Subject = \'Re: \' + pp.Subject THEN \'\' ELSE \'# \' + p.Subject + \'\n\n\' END + p.Body AS _content, ' +
				'p.PostDate AS _timestamp, ' +
				'1 - t.IsApproved AS _deleted ' +
				'FROM dbo.cs_Posts AS p ' +
				'LEFT OUTER JOIN dbo.cs_Posts AS pp ' +
				'ON p.ParentID = pp.PostID ' +
				'WHERE p.PostType = 1 ' +
				'AND p.PostConfiguration = 0 ' +
				'AND p.ParentID <> p.PostID) AS topics' +
				'WHERE _rowid > @start ' +
				'ORDER BY p.PostID ASC', function(err, rows) {
				if (err) {
					return callback(err);
				}

				if (rows.length === 0) {
					_imported.posts_offset = start;
					return discoursePaginatedPosts(0, limit, callback);
				}

				var posts = {};

				rows.forEach(function(row) {
					delete row._rowid;
					if (row._uid === 1001) {
						delete row._uid;
					} else {
						row._uid = _imported.u[row._uid];
						delete row._guest;
					}
					row._timestamp = +row._timestamp;
					posts[row._pid] = row;
				});

				callback(null, posts);
			});
		} else {
			discoursePaginatedPosts(start - _imported.posts_offset, limit, callback);
		}
	};

	function discoursePaginatedPosts(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query({
				text: 'SELECT ' +
				'p.id * 2 + 1 AS _pid, ' +
				'COALESCE((SELECT t.value::int * 2 ' +
					'FROM ' + _table_prefix + 'topic_custom_fields AS t ' +
					'WHERE t.name = \'import_id\' AND t.topic_id = p.topic_id), ' +
				'p.topic_id * 2 + 1) AS _tid, ' +
				'p.user_id AS _uid, ' +
				'p.raw AS _content, ' +
				'p.created_at AS _timestamp, ' +
				'p.updated_at AS _edited, ' +
				'CASE WHEN p.deleted_at IS NULL THEN 0 ELSE 1 END AS _deleted, ' +
				'COALESCE((SELECT c.value::int * 2 ' +
					'FROM ' + _table_prefix + 'post_custom_fields AS c ' +
					'WHERE c.name = \'import_id\' AND c.post_id = r.id), ' +
				'r.id * 2 + 1) AS "_toPid" ' +
				'FROM ' + _table_prefix + 'posts AS p ' +
				'LEFT JOIN ' + _table_prefix + 'topics AS t ' +
				'ON p.topic_id = t.id ' +
				'LEFT OUTER JOIN ' + _table_prefix + 'posts AS r ' +
				'ON p.topic_id = r.topic_id ' +
				'AND p.reply_to_post_number = r.post_number ' +
				'LEFT OUTER JOIN ' + _table_prefix + 'post_custom_fields AS f ' +
				'ON f.topic_id = p.id AND f.name = \'import_id\' ' +
				'WHERE p.post_number <> 1 AND t.archetype = \'regular\' ' +
				'AND f.value IS NULL ' +
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
	}

	Exporter.getPaginatedVotes = function(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query({
				text: 'SELECT ' +
				'a.id AS _vid, ' +
				'COALESCE((SELECT t.value::int * 2 ' +
					'FROM ' + _table_prefix + 'post_custom_fields AS t ' +
					'WHERE t.name = \'import_id\' AND t.post_id = a.post_id), ' +
				'a.post_id * 2 + 1) AS _pid, ' +
				'COALESCE((SELECT t.value::int * 2 ' +
					'FROM ' + _table_prefix + 'topic_custom_fields AS t ' +
					'WHERE t.name = \'import_id\' AND t.topic_id = p.topic_id), ' +
				'p.topic_id * 2 + 1) AS _tid, ' +
				'p.post_number AS _pn, ' +
				'a.user_id AS _uid, ' +
				'1 AS _action ' +
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
				'COALESCE((SELECT t.value::int * 2 ' +
					'FROM ' + _table_prefix + 'post_custom_fields AS t ' +
					'WHERE t.name = \'import_id\' AND t.post_id = a.post_id), ' +
				'a.post_id * 2 + 1) AS _pid, ' +
				'COALESCE((SELECT t.value::int * 2 ' +
					'FROM ' + _table_prefix + 'topic_custom_fields AS t ' +
					'WHERE t.name = \'import_id\' AND t.topic_id = p.topic_id), ' +
				'p.topic_id * 2 + 1) AS _tid, ' +
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
		mssql.close();
		callback();
	};
})(module.exports);
