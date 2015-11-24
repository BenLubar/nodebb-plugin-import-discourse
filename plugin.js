(function(Plugin) {
	var db = module.parent.require('./database');

	Plugin.load = function(params) {
		params.router.get('/t/:title/:tid(/:post_index)?', Plugin.topicRedirect);
	};

	Plugin.topicRedirect = function(req, res, next) {
		db.sortedSetScore('_imported:_topics', req.params.tid, function(err, id) {
			if (err || !id) {
				return next();
			}
			res.redirect(301, nconf.get('url') + '/topic/' + id + '/' + req.params.title + (req.params.post_index ? '/' + req.params.post_index : ''));
		});
	};
})(module.exports);
