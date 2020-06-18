var async = require('async')
var jsonist = require('jsonist')
var through = require('through2')

module.exports = function (config) {
  return through.obj(function (obj, enc, cb) {
    async.retry({
      times: config.retryTimes || 1,
      interval: config.retryInterval || 100
    }, function (callback) {
      var reqOpts = {
        method: config.method || 'post',
        uri: config.url
      }
      if (config.key) {
        reqOpts.uri = reqOpts.uri + '/_doc/' + encodeURIComponent(obj[config.key])
        delete obj[config.key]
      }

      var update = function () {
        jsonist[reqOpts.method].call(null, reqOpts.uri, obj, function (err, resp) {
          if (err) {
            if (config.swallowErrors) return callback(null, { ok: false, error: err.toString() })
            else return callback(err)
          }
          if (resp && resp._id) obj._id = resp._id
          callback(null, obj)
        })
      }
      update()
    }, cb)
  })
}
