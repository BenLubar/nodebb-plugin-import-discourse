(function(Plugin) {
	var db = module.parent.require('./database'),
	    Posts = module.parent.require('./posts'),
	    Topics = module.parent.require('./topics'),
	    User = module.parent.require('./user'),
	    Categories = module.parent.require('./categories'),
	    Messaging = module.parent.require('./messaging'),
	    redirect = module.parent.require('./controllers/helpers').redirect;

	Plugin.load = function(params, callback) {
		params.router.get('/t/:tid', Plugin.topicRedirect);
		params.router.get('/api/t/:tid', Plugin.topicRedirect);
		params.router.get('/t/:title/:tid/:post_index?', Plugin.topicRedirect);
		params.router.get('/api/t/:title/:tid/:post_index?', Plugin.topicRedirect);
		params.router.get('/p/:pid', Plugin.postRedirect);
		params.router.get('/api/p/:pid', Plugin.postRedirect);
		params.router.get('/user_avatar/:host/:user/:size/:name', Plugin.avatarRedirect);
		params.router.get('/api/user_avatar/:host/:user/:size/:name', Plugin.avatarRedirect);
		params.router.get('/c/:parent/:child?', Plugin.categoryRedirect);
		params.router.get('/api/c/:parent/:child?', Plugin.categoryRedirect);

		callback();
	};

	Plugin.topicRedirect = function(req, res, next) {
		db.sortedSetScore('_imported:_topics', req.params.tid, function(err, id) {
			if (err || !id) {
				db.sortedSetScore('_imported:_rooms', req.params.tid, function(err, roomId) {
					if (err || !roomId) {
						return next();
					}

					Messaging.isUserInRoom(req.uid, roomId, function(err, inRoom) {
						if (err || !inRoom) {
							return next();
						}

						redirect(res, '/chats/' + roomId);
					});
				});
			}

			Topics.getTopicField(id, 'slug', function(err, slug) {
				if (err || !slug) {
					return next();
				}

				redirect(res, '/topic/' + encodeURI(slug) + (req.params.post_index ? '/' + req.params.post_index : ''));
			});
		});
	};

	Plugin.postRedirect = function(req, res, next) {
		db.sortedSetScore('_imported:_posts', req.params.pid, function(err, id) {
			if (err || !id) {
				db.sortedSetScore('_imported:_messages', req.params.pid, function(err, mid) {
					if (err || !mid) {
						return next();
					}

					Messaging.getMessageField(mid, 'roomId', function(err, roomId) {
						if (err || !roomId) {
							next();
						}

						Messaging.isUserInRoom(req.uid, roomId, function(err, inRoom) {
							if (err || !inRoom) {
								return next();
							}

							redirect(res, '/chats/' + roomId);
						});
					});
				});
			}

			Posts.getPostFields(id, 'tid', function(err, tid) {
				if (err || !tid) {
					return next();
				}

				Posts.getPidIndex(id, tid, null, function(err, index) {
					if (err || !index) {
						return next();
					}


					Topics.getTopicField(tid, 'slug', function(err, slug) {
						if (err || !slug) {
							return next();
						}

						redirect(res, '/topic/' + encodeURI(slug) + '/' + index);
					});
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

				redirect(res, url);
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
					redirect(res, '/category/' + encodeURI(cat.slug));
					return true;
				}
				return false;
			})) {
				next();
			}
		});
	};
})(module.exports);
