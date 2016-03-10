var async = require('async');
var pg = require('pg');
var mssql = require('mssql');
var db = require('../../src/database');
var utils = require('../../public/src/utils');
var winston = module.parent.require('winston');

(function(Exporter) {
	var _table_prefix;
	var _url;
	var _cs;
	var _config;
	var _imported;

	var allowed_keys = {
		"cs": function(x) { return x; },
		"skip_cs": function(x) { if (x !== true) throw "skip_cs can only be true if provided"; return x; },
		"user_id_greater": function(x) { return parseInt(x, 10); },
		"user_created_after": function(x) { return new Date(x); },
		"user_where": function(x) { return String(x); },
		"topic_id_greater": function(x) { return parseInt(x, 10); },
		"topic_created_after": function(x) { return new Date(x); },
		"topic_where": function(x) { return String(x); },
		"post_id_greater": function(x) { return parseInt(x, 10); },
		"post_created_after": function(x) { return new Date(x); },
		"post_where": function(x) { return String(x); },
		"room_id_greater": function(x) { return parseInt(x, 10); },
		"room_created_after": function(x) { return new Date(x); },
		"room_where": function(x) { return String(x); },
		"message_id_greater": function(x) { return parseInt(x, 10); },
		"message_created_after": function(x) { return new Date(x); },
		"message_where": function(x) { return String(x); },
		"vote_id_greater": function(x) { return parseInt(x, 10); },
		"vote_created_after": function(x) { return new Date(x); },
		"vote_where": function(x) { return String(x); },
		"bookmark_id_greater": function(x) { return parseInt(x, 10); },
		"bookmark_where": function(x) { return String(x); },
		"favourite_id_greater": function(x) { return parseInt(x, 10); },
		"favourite_created_after": function(x) { return new Date(x); },
		"favourite_where": function(x) { return String(x); },
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

		_imported = {c: {}, u: {}, topics_offset: -1, posts_offset: -1};

		if (_config.skip_cs) {
			_cs = null;
			return callback(null, config);
		}

		_cs = _config.cs;
		if (!_cs) {
			return callback("Need {\"cs\":\"community server connection string\"} in custom field");
		}

		mssql.connect(_cs, function(err) {
			if (err) {
				return callback(err);
			}

			getImportedIDs(config, callback);
		}).config.options.requestTimeout = 60 * 60 * 1000;
	};

	function getImportedIDs(config, callback) {
		async.waterfall([
			function(next) {
				new mssql.Request().query('SELECT u.Email AS k, u.UserID AS v FROM dbo.cs_Users AS u', function(err, rows) {
					next(err, rows);
				});
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

				db.sortedSetAdd('_telligent:_users', scores, values, function(err) { next(err); });
			}, function(next) {
				new mssql.Request().query('SELECT s.SectionID AS k, s.Name AS v FROM dbo.cs_Sections AS s WHERE s.SectionID >= 10', function(err, rows) {
					next(err, rows);
				});
			}, function(rows, next) {
				var cscats = {};
				rows.forEach(function(row) {
					cscats[row.k] = row.v;
				});
				next(null, cscats);
			}, function(cscats, next) {
				pg.connect(_url, function(err, client, done) {
					next(err, client, done, cscats);
				});
			}, function(client, done, cscats, next) {
				client.query('SELECT f.value::int AS k, f.category_id AS v FROM ' + _table_prefix + 'category_custom_fields AS f WHERE f.name = \'import_id\'', function(err, result) {
					done(err);
					next(err, result, cscats);
				});
			}, function(result, cscats, next) {
				var scores = [], values = [];
				result.rows.forEach(function(row) {
					_imported.c[row.k] = row.v;
					scores.push(row.v);
					values.push(row.k);
					delete cscats[row.k];
				});

				next(null, scores, values, cscats);
			}, function(scores, values, cscats, next) {
				pg.connect(_url, function(err, client, done) {
					next(err, client, done, scores, values, cscats);
				});
			}, function(client, done, scores, values, cscats, next) {
				client.query('SELECT c.id AS id, c.slug AS slug FROM ' + _table_prefix + 'categories AS c', function(err, result) {
					done(err);
					next(err, scores, values, cscats, result);
				});
			}, function(scores, values, cscats, result, next) {
				result.rows.forEach(function(row) {
					Object.keys(cscats).some(function(id) {
						if (utils.slugify(cscats[id]) == row.slug) {
							_imported.c[id] = row.id;
							scores.push(row.id);
							values.push(id);
							delete cscats[id];

							return true;
						}
						return false;
					});
				});

				Object.keys(cscats).forEach(function(id) {
					winston.warn('skipping posts from Community Server category #' + id + ': ' + cscats[id]);
				});

				next(null, scores, values);
			}, function(scores, values, next) {
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
				'ORDER BY g.id ASC ' +
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

	function discourseUsersQuery() {
		return 'SELECT ' +
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
			'WHERE u.id > $1::int ' +
			'AND u.created_at > $2::timestamp ' +
			("user_where" in _config ? 'AND (' + _config["user_where"] + ') ' : '') +
			'ORDER BY u.id ASC';
	}

	Exporter.getPaginatedUsers = function(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query({
				text: discourseUsersQuery() + ' LIMIT $3::int OFFSET $4::int',
				types: ["int", "timestamp", "int", "int"]
			}, [_config["user_id_greater"] || -1, _config["user_created_after"] || new Date(0), limit, start], function(err, result) {
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

	function discourseRoomsQuery() {
		return 'SELECT ' +
			't.id AS "_roomId", ' +
			't.user_id AS _uid, ' +
			'ARRAY(SELECT tu.user_id FROM topic_allowed_users AS tu WHERE topic_id = t.id AND tu.user_id <> t.user_id UNION SELECT gu.user_id FROM topic_allowed_groups AS tg RIGHT JOIN group_users AS gu ON gu.group_id = tg.group_id WHERE tg.topic_id = t.id AND gu.user_id <> t.user_id) AS _uids, ' +
			't.title AS "_roomName", ' +
			't.created_at AS _timestamp ' +
			'FROM ' + _table_prefix + 'topics AS t ' +
			'WHERE t.archetype = \'private_message\' ' +
			'AND t.id > $1::int ' +
			'AND t.created_at > $2::timestamp ' +
			("room_where" in _config ? 'AND (' + _config["room_where"] + ') ' : '') +
			'ORDER BY t.id ASC';
	}

	Exporter.getPaginatedRooms = function(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query({
				text: discourseRoomsQuery() + ' LIMIT $3::int OFFSET $4::int',
				types: ["int", "timestamp", "int", "int"]
			}, [_config["room_id_greater"] || -1, _config["room_created_after"] || new Date(0), limit, start], function(err, result) {
				done(err);

				if (err) {
					return callback(err);
				}

				var rooms = {};

				result.rows.forEach(function(row) {
					if (utils.slugify(row._roomName).length === 0) {
						row._roomName += ' (invalid title)';
					}
					row._timestamp = +row._timestamp;
					rooms[row._roomId] = row;
				});

				callback(null, rooms);
			});
		});
	};

	function discourseMessagesQuery() {
		return 'SELECT ' +
			'p.id AS _mid, ' +
			'p.topic_id AS "_roomId", ' +
			'p.user_id AS _fromuid, ' +
			'p.raw AS _content, ' +
			'p.created_at AS _timestamp ' +
			'FROM ' + _table_prefix + 'posts AS p ' +
			'LEFT JOIN ' + _table_prefix + 'topics AS t ' +
			'ON p.topic_id = t.id ' +
			'WHERE t.archetype = \'private_message\' ' +
			'AND p.id > $1::int ' +
			'AND p.created_at > $2::timestamp ' +
			("message_where" in _config ? 'AND (' + _config["message_where"] + ') ' : '') +
			'ORDER BY p.id ASC';
	}

	Exporter.getPaginatedMessages = function(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query({
				text: discourseMessagesQuery() + ' LIMIT $3::int OFFSET $4::int',
				types: ["int", "timestamp", "int", "int"]
			}, [_config["message_id_greater"] || -1, _config["message_created_after"] || new Date(0), limit, start], function(err, result) {
				done(err);

				if (err) {
					return callback(err);
				}

				var messages = {};

				result.rows.forEach(function(row) {
					row._timestamp = +row._timestamp;
					messages[row._mid] = row;
				});

				callback(null, messages);
			});
		});
	}

	function discourseCategoriesQuery() {
		return 'SELECT ' +
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
			'ORDER BY c.id ASC';
	}

	Exporter.getPaginatedCategories = function(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query({
				text: discourseCategoriesQuery() + ' LIMIT $1::int OFFSET $2::int',
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

	function telligentTopicsQuery() {
		return 'SELECT ' +
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
			'AND p.PostConfiguration = 0 ' +
			'AND t.SectionID >= 10 ' +
			'ORDER BY t.ThreadID ASC';
	}

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
			req.query(telligentTopicsQuery() + ' OFFSET @start ROWS FETCH NEXT @limit ROWS ONLY', function(err, rows) {
				if (err) {
					return callback(err);
				}

				if (rows.length === 0) {
					_imported.topics_offset = start;
					return discoursePaginatedTopics(0, limit, callback);
				}

				var topics = {};

				rows.forEach(function(row) {
					if (row._uid === 1001) {
						delete row._uid;
					} else {
						row._uid = _imported.u[row._uid];
						delete row._guest;
					}
					row._cid = _imported.c[row._cid] || ('cs-' + row._cid);
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

	// XXX: assumes topics are imported iff the first post is imported
	function discourseTopicsQuery() {
		return 'SELECT ' +
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
			'AND t.id > $1::int ' +
			'AND t.created_at > $2::timestamp ' +
			("topic_where" in _config ? 'AND (' + _config["topic_where"] + ') ' : '') +
			'ORDER BY t.id ASC';
	}

	function discoursePaginatedTopics(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query({
				text: discourseTopicsQuery() + ' LIMIT $3::int OFFSET $4::int',
				types: ["int", "timestamp", "int", "int"]
			}, [_config["topic_id_greater"] || -1, _config["topic_created_after"] || new Date(0), limit, start], function(err, result) {
				done(err);

				if (err) {
					return callback(err);
				}

				var topics = {};

				result.rows.forEach(function(row) {
					if (utils.slugify(row._title).length === 0) {
						row._title += ' (invalid title)';
					}
					row._timestamp = +row._timestamp;
					row._edited = +row._edited;
					topics[row._tid] = row;
				});

				callback(null, topics);
			});
		});
	}

	// XXX: does not import tags, but they're mostly deleted anyway

	function telligentPostsQuery() {
		return 'SELECT ' +
			'p.ThreadID * 2 AS _tid, ' +
			'p.PostID * 2 AS _pid, ' +
			'p.UserID AS _uid, ' +
			'p.PostAuthor AS _guest, ' +
			'CASE WHEN p.ParentID = p.PostID THEN NULL ELSE p.ParentID END AS [_toPid], ' +
			'p.IPAddress AS _ip, ' +
			'CASE WHEN p.Subject = pp.Subject THEN \'\' WHEN p.Subject = \'Re: \' + pp.Subject THEN \'\' ELSE \'# \' + p.Subject + CHAR(10) + CHAR(10) END + CAST(p.Body AS nvarchar(max)) AS _content, ' +
			'p.PostDate AS _timestamp, ' +
			'1 - p.IsApproved AS _deleted ' +
			'FROM dbo.cs_Posts AS p ' +
			'LEFT OUTER JOIN dbo.cs_Posts AS pp ' +
			'ON p.ParentID = pp.PostID ' +
			'INNER JOIN dbo.cs_Threads AS t ' +
			'ON p.ThreadID = t.ThreadID ' +
			'WHERE p.PostType = 1 ' +
			'AND p.PostConfiguration = 0 ' +
			'AND p.ParentID <> p.PostID ' +
			'AND t.SectionID >= 10 ' +
			'ORDER BY p.PostID ASC';
	}

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
			req.query(telligentPostsQuery() + ' OFFSET @start ROWS FETCH NEXT @limit ROWS ONLY', function(err, rows) {
				if (err) {
					return callback(err);
				}

				if (rows.length === 0) {
					_imported.posts_offset = start;
					return discoursePaginatedPosts(0, limit, callback);
				}

				var posts = {};

				rows.forEach(function(row) {
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

	// XXX: assumes imported posts never reply to non-imported posts

	function discoursePostsQuery() {
		return 'SELECT ' +
			'p.id * 2 + 1 AS _pid, ' +
			'COALESCE(tf.value::int * 2, p.topic_id * 2 + 1) AS _tid, ' +
			'p.user_id AS _uid, ' +
			'p.raw AS _content, ' +
			'p.created_at AS _timestamp, ' +
			'p.updated_at AS _edited, ' +
			'CASE WHEN p.deleted_at IS NULL THEN 0 ELSE 1 END AS _deleted, ' +
			'COALESCE(rf.value::int * 2, r.id * 2 + 1) AS "_toPid" ' +
			'FROM ' + _table_prefix + 'posts AS p ' +
			'LEFT JOIN ' + _table_prefix + 'topics AS t ' +
			'ON p.topic_id = t.id ' +
			'LEFT OUTER JOIN ' + _table_prefix + 'posts AS r ' +
			'ON p.topic_id = r.topic_id ' +
			'AND p.reply_to_post_number = r.post_number ' +
			'LEFT OUTER JOIN ' + _table_prefix + 'post_custom_fields AS f ' +
			'ON f.post_id = p.id AND f.name = \'import_id\' ' +
			'LEFT OUTER JOIN ' + _table_prefix + 'topic_custom_fields AS tf ' +
			'ON tf.topic_id = p.topic_id AND tf.name = \'import_id\' ' +
			'LEFT OUTER JOIN ' + _table_prefix + 'post_custom_fields AS rf ' +
			'ON rf.post_id = r.id AND rf.name = \'import_id\' ' +
			'WHERE p.post_number <> 1 AND t.archetype = \'regular\' ' +
			'AND f.value IS NULL ' +
			'AND p.id > $1::int ' +
			'AND p.created_at > $2::timestamp ' +
			("post_where" in _config ? 'AND (' + _config["post_where"] + ') ' : '') +
			'ORDER BY p.id ASC';
	}

	function discoursePaginatedPosts(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query({
				text: discoursePostsQuery() + ' LIMIT $3::int OFFSET $4::int',
				types: ["int", "timestamp", "int", "int"]
			}, [_config["post_id_greater"] || -1, _config["post_created_after"] || new Date(0), limit, start], function(err, result) {
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

	function discourseVotesQuery() {
		return 'SELECT ' +
			'a.id AS _vid, ' +
			'COALESCE(pf.value::int * 2, a.post_id * 2 + 1) AS _pid, ' +
			'COALESCE(tf.value::int * 2, p.topic_id * 2 + 1) AS _tid, ' +
			'p.post_number AS _pn, ' +
			'a.user_id AS _uid, ' +
			'1 AS _action ' +
			'FROM ' + _table_prefix + 'post_actions AS a ' +
			'INNER JOIN ' + _table_prefix + 'posts AS p ' +
			'ON a.post_id = p.id ' +
			'LEFT OUTER JOIN ' + _table_prefix + 'post_custom_fields AS pf ' +
			'ON pf.post_id = a.post_id AND pf.name = \'import_id\' ' +
			'LEFT OUTER JOIN ' + _table_prefix + 'topic_custom_fields AS tf ' +
			'ON tf.topic_id = p.topic_id AND tf.name = \'import_id\' ' +
			'WHERE a.post_action_type_id = (SELECT t.id FROM ' + _table_prefix + 'post_action_types AS t WHERE t.name_key = \'like\') ' +
			'AND a.id > $1::int ' +
			'AND a.created_at > $2::timestamp ' +
			("vote_where" in _config ? 'AND (' + _config["vote_where"] + ') ' : '') +
			'ORDER BY a.id ASC';
	}

	Exporter.getPaginatedVotes = function(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query({
				text: discourseVotesQuery() + ' LIMIT $3::int OFFSET $4::int',
				types: ["int", "timestamp", "int", "int"]
			}, [_config["vote_id_greater"] || -1, _config["vote_created_after"] || new Date(0), limit, start], function(err, result) {
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

	function discourseBookmarksQuery() {
		return 'SELECT ' +
			'tu.id AS _bid, ' +
			'COALESCE(tf.value::int * 2, tu.topic_id * 2 + 1) AS _tid, ' +
			'tu.user_id AS _uid, ' +
			'tu.last_read_post_number - 1 AS _index ' +
			'FROM ' + _table_prefix + 'topic_users AS tu ' +
			'LEFT OUTER JOIN ' + _table_prefix + 'topic_custom_fields AS tf ' +
			'ON tf.topic_id = tu.topic_id AND tf.name = \'import_id\' ' +
			'WHERE tu.id > $1::int ' +
			("bookmark_where" in _config ? 'AND (' + _config["bookmark_where"] + ') ' : '') +
			'ORDER BY tu.id ASC';
	}

	Exporter.getPaginatedBookmarks = function(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query({
				text: discourseBookmarksQuery() + ' LIMIT $2::int OFFSET $3::int',
				types: ["int", "int", "int"]
			}, [_config["bookmark_id_greater"] || -1, limit, start], function(err, result) {
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

	function discourseFavouritesQuery() {
		return 'SELECT ' +
			'a.id AS _fid, ' +
			'COALESCE(pf.value::int * 2, a.post_id * 2 + 1) AS _pid, ' +
			'a.user_id AS _uid, ' +
			'FROM ' + _table_prefix + 'post_actions AS a ' +
			'LEFT OUTER JOIN ' + _table_prefix + 'post_custom_fields AS pf ' +
			'ON pf.post_id = a.post_id AND pf.name = \'import_id\' ' +
			'WHERE a.post_action_type_id = (SELECT t.id FROM ' + _table_prefix + 'post_action_types AS t WHERE t.name_key = \'bookmark\') ' +
			'AND a.id > $3::int ' +
			'AND a.created_at > $4::timestamp ' +
			("favourite_where" in _config ? 'AND (' + _config["favourite_where"] + ') ' : '') +
			'ORDER BY a.id ASC';
	}

	Exporter.getPaginatedFavourites = function(start, limit, callback) {
		pg.connect(_url, function(err, client, done) {
			if (err) {
				return callback(err);
			}

			client.query({
				text: discourseFavouritesQuery() + ' LIMIT $3::int OFFSET $4::int',
				types: ["int", "timestamp", "int", "int"]
			}, [_config["favourite_id_greater"] || -1, _config["favourite_created_after"] || new Date(0), limit, start], function(err, result) {
				done(err);

				if (err) {
					return callback(err);
				}

				var favourites = {};

				result.rows.forEach(function(row) {
					favourites[row._fid] = row;
				});

				callback(null, favourites);
			});
		});
	};

	Exporter.teardown = function(callback) {
		mssql.close();
		callback();
	};
})(module.exports);
