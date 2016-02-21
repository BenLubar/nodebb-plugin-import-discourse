(function(Plugin) {
	var db = module.parent.require('./database'),
	    Posts = module.parent.require('./posts'),
	    User = module.parent.require('./user'),
	    Categories = module.parent.require('./categories');

	Plugin.load = function(params, callback) {
		params.router.get('/t/:title/:tid/:post_index?', Plugin.topicRedirect);
		params.router.get('/p/:pid', Plugin.postRedirect);
		params.router.get('/user_avatar/:host/:user/:size/:name', Plugin.avatarRedirect);
		params.router.get('/c/:parent/:child?', Plugin.categoryRedirect);

		params.router.get('/user/Profile.aspx', Plugin.telligentUserRedirect);
		params.router.get('/forums/:id.aspx', Plugin.telligentCategoryRedirect);
		params.router.get('/forums/t/:tid.aspx', Plugin.telligentTopicRedirect);
		params.router.get('/forums/p/:tid/:pid.aspx', Plugin.telligentPostRedirect);

		callback();
	};

	Plugin.topicRedirect = function(req, res, next) {
		db.sortedSetScore('_imported:_topics', req.params.tid * 2 + 1, function(err, id) {
			if (err || !id) {
				return next();
			}
			res.redirect(301, '/topic/' + id + '/' + req.params.title + (req.params.post_index ? '/' + req.params.post_index : ''));
		});
	};

	Plugin.postRedirect = function(req, res, next) {
		db.sortedSetScore('_imported:_posts', req.params.pid * 2 + 1, function(err, id) {
			if (err || !id) {
				return next();
			}
			Posts.getPostFields(id, 'tid', function(err, tid) {
				if (err || !tid) {
					return next();
				}

				Posts.getPidIndex(id, tid, null, function(err, index) {
					if (err || !index) {
						return next();
					}

					res.redirect(301, '/topic/' + tid + '/by-post/' + index);
				});
			});
		});
	};

	Plugin.avatarRedirect = function(req, res, next) {
		User.getUidByUserslug(req.params.user, function(err, id) {
			if (err || !id) {
				return next();
			}

			User.getUserField(id, 'picture', function(err, url) {
				if (err || !url) {
					return next();
				}

				res.redirect(302, url);
			});
		});
	};

	Plugin.categoryRedirect = function(req, res, next) {
		var slug = req.params.child || req.params.parent;

		Categories.getAllCategoryFields(['cid', 'slug'], function(err, cats) {
			if (err) {
				return next();
			}

			if (!cats.some(function(cat) {
				if (cat.slug === cat.cid + '/' + slug) {
					res.redirect(301, '/category/' + cat.slug);
					return true;
				}
				return false;
			})) {
				next();
			}
		});
	};

	Plugin.telligentUserRedirect = function(req, res, next) {
		if (isNaN(req.query.UserID)) {
			return next();
		}

		db.sortedSetScore('_telligent:_users', req.query.UserID, function(err, id) {
			if (err || !id) {
				return next();
			}

			db.sortedSetScore('_imported:_users', id, function(err, id) {
				if (err || !id) {
					return next();
				}

				User.getUserField(id, 'userslug', function(err, slug) {
					if (err || !slug) {
						return next();
					}

					res.redirect(301, '/user/' + slug);
				})
			});
		});
	};

	Plugin.telligentCategoryRedirect = function(req, res, next) {
		db.sortedSetScore('_telligent:_categories', req.params.id, function(err, id) {
			if (err || !id) {
				return next();
			}

			db.sortedSetScore('_imported:_categories', id, function(err, id) {
				if (err || !id) {
					return next();
				}

				Categories.getCategoryField(id, 'slug', function(err, slug) {
					if (err || !slug) {
						return next();
					}

					res.redirect(301, '/category/' + slug);
				})
			});
		});
	};

	Plugin.telligentTopicRedirect = function(req, res, next) {
		db.sortedSetScore('_imported:_topics', req.params.tid * 2, function(err, id) {
			if (err || !id) {
				return next();
			}

			res.redirect(301, '/topic/' + id + (isNaN(req.query.PageIndex) ? '' : '/from-cs/' + (req.query.PageIndex * 50 - 49));
		});
	};

	Plugin.telligentPostRedirect = function(req, res, next) {
		db.sortedSetScore('_imported:_posts', req.params.pid * 2, function(err, id) {
			if (err || !id) {
				return next();
			}

			Posts.getPostFields(id, 'tid', function(err, tid) {
				if (err || !tid) {
					return next();
				}

				Posts.getPidIndex(id, tid, null, function(err, index) {
					if (err || !index) {
						return next();
					}

					res.redirect(301, '/topic/' + tid + '/by-post/' + index);
				});
			});
		});
	};
})(module.exports);
