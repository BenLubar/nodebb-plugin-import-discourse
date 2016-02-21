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
})(module.exports);
