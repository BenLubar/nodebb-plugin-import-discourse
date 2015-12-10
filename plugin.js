(function(Plugin) {
	var db = module.parent.require('./database'),
	    Posts = module.parent.require('./posts');

	Plugin.load = function(params, callback) {
		params.router.get('/t/:title/:tid/:post_index?', Plugin.topicRedirect);
		params.router.get('/p/:pid', Plugin.postRedirect);

		callback();
	};

	Plugin.topicRedirect = function(req, res, next) {
		db.sortedSetScore('_imported:_topics', req.params.tid, function(err, id) {
			if (err || !id) {
				return next();
			}
			res.redirect(301, '/topic/' + id + '/' + req.params.title + (req.params.post_index ? '/' + req.params.post_index : ''));
		});
	};

	Plugin.postRedirect = function(req, res, next) {
		db.sortedSetScore('_imported:_posts', req.params.pid, function(err, id) {
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
	};
})(module.exports);
