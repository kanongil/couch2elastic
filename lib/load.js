// Loads elastic search for the very fisrt time
var ndjson = require('ndjson')
var through2 = require('through2')
var jsonfilter = require('jsonfilter')
var jsonist = require('jsonist')
var hyperquest = require('hyperquest')
var toEs = require('./to_es')

module.exports = function (config, log, done) {
  var toElasticConfig = config.load
  toElasticConfig.url = config.elasticsearch
  toElasticConfig.urlTemplate = config.urlTemplate
  toElasticConfig.removeMeta = config.removeMeta
  toElasticConfig.key = '_id'

  return hyperquest(config.database + '/_all_docs?include_docs=true')
    .pipe(jsonfilter('rows.*'))
    .pipe(ndjson.parse())
    .pipe(through2.obj(async function (row, enc, cb) {
      // ignore design docs
      if (row.id.indexOf('_design') === 0) return cb()
      var result = row.doc
      if (config.mapper) {
        try {
          result = await config.mapper(row.doc)
          if (!result) return cb()
          result._id = row.id // we need this to work correct
        } catch (e) {
          log.error(e)
          return cb()
        }
      }
      cb(null, result)
    }))
    //
    .pipe(toEs(toElasticConfig))
    .on('error', function (err) {
      log.error('document error', err)
    })
    .pipe(ndjson.stringify())
    .once('end', function () {
      // save the seq
      jsonist.get(config.database, function (err, about) {
        if (err) return done(err)
        jsonist.put(config.seq_url, { _meta: { seq: about.update_seq } }, function (err, resp) {
          return done(err)
        })
      })
    })
}
