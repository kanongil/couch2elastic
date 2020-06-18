var async = require('async')
var follow = require('follow')
var jsonist = require('jsonist')
var once = require('lodash').once

module.exports = function (config, log, since) {
  var _retry = async.retry.bind(null, config.retry)
  var followConfig = {
    db: config.database,
    include_docs: true,
    since: since || 'now'
  }
  var checkpointCounter = 0
  var updatingCheckpointCounter = false
  var lastSeenSeq = 0

  var shutdown = function () {
    log.info('shutdown called')
    if (!lastSeenSeq) return process.exit(0)
    jsonist.put(config.seq_url, { _meta: { seq: lastSeenSeq } }, function (err, resp) {
      if (err) log.error('Could not record sequence in elasticsearch', lastSeenSeq, err)
      else log.info({ type: 'checkpoint', seq: lastSeenSeq }, 'stored in elasticsearch')
      process.exit(0)
    })
  }

  var onDone = function (log, _id, _rev, type, seq, err, resp, _prevEsDoc) {
    if (seq > lastSeenSeq) lastSeenSeq = seq
    if (err) {
      log.error('error occured', type, _id, _rev, err)
      return
    }
    if (resp && resp.error) {
      log.error('error occured', type, _id, _rev, resp.error)
      return
    }
    var logged = { type: 'change', seq: seq, change: type, id: _id, rev: _rev, err: err }
    if (_prevEsDoc) logged.doc = _prevEsDoc
    log.info(logged, 'success')
    checkpointCounter++
    if (!updatingCheckpointCounter && checkpointCounter > config.checkpointSize) {
      updatingCheckpointCounter = true
      checkpointCounter = 0
      // store the thing change seq
      jsonist.put(config.seq_url, { _meta: { seq: seq } }, function (err, resp) {
        updatingCheckpointCounter = false
        if (err) log.error('Could not record sequence in elasticsearch', seq, err)
        else log.info({ type: 'checkpoint', seq: lastSeenSeq }, 'stored in elasticsearch')
        return
      })
    }
  }

  var q = async.queue(async function (change) {
    if (change.id.indexOf('_design') === 0) return

    var doc = change.doc
    var esDocUrl = config.elasticsearch + '/_doc/' + encodeURIComponent(change.id)
    if (doc._deleted) {
      const { err, prevEsDoc } = await handleDelete(config, esDocUrl)
      return onDone(log, change.id, null, 'delete', change.seq, err, null, prevEsDoc)
    }

    var _rev = doc._rev
    if (config.mapper) {
      try {
        var mapped = await config.mapper(change.doc)
        doc = mapped
      } catch (e) {
        log.error(e)
        log.error({ change: feed.original_db_seq }, change.doc._id, _rev, 'An error occured in the mapping', e)
        return
      }
    }
    if (!doc) {
      log.error({ change: feed.original_db_seq }, change.doc._id, _rev, 'No document came back from the mapping')
      return
    }
    delete doc._id;
    await _retry(jsonist.put.bind(null, esDocUrl, doc))
    return onDone(log, change.doc._id, _rev, 'update', change.seq)
  }, config.concurrency)

  var _caughtUp = false
  var _shutdown = once(shutdown)

  q.drain(function () {
    log.info({ _caughtUp: _caughtUp }, 'drain called')
    if (_caughtUp) setTimeout(_shutdown, 400)
  })

  var feed = follow(followConfig, function (err, change) {
    if (err) return log.error(err)
    q.push(change)
    if (q.length() > config.maxQueueSize) {
      feed.pause()
      console.log('pausing feed because maxQueueSize has been reached')
    }
  })

  q.empty(function () {
    feed.resume() // queue has become empty, resuming feed if paused
  })

  if (config.endOnCatchup) {
    feed.on('catchup', function () {
      log.info('catchup event received. Pausing feed')
      _caughtUp = true
      feed.pause()
      var remain = q.length()
      if (remain === 0) {
        log.info('no tasks. shutdown')
        return _shutdown()
      }
      var ensureComplete = function () {
        var nowRemain = q.length()
        log.info(nowRemain + ' tasks remain')
        if (nowRemain === remain) {
          log.info('no queue change. shutdown')
          return _shutdown()
        }
        remain = nowRemain
        setTimeout(ensureComplete, 4000)
      }
      setTimeout(ensureComplete, 4000)
    })
  }

  feed.on('confirm', function (info) {
    log.info({ type: 'start', seq: feed.original_db_seq }, 'started')
  })
}

async function handleDelete (config, esDocUrl) {
  var tasks = []
  var _prevEsDoc
  var _err

  if (config.logDeleted) {
    tasks.push(function (cb) {
      jsonist.get(esDocUrl, function (err, prevEsDoc) {
        if (err) _err = err
        _prevEsDoc = prevEsDoc
        cb()
      })
    })
  }
  tasks.push(function (cb) {
    jsonist.delete(esDocUrl, cb)
  })

  try {
    await async.series(tasks);
  }
  catch (err) {
    console.log('async error occured, but continuing', err);
  }

  return { err: _err, prevEsDoc: _prevEsDoc}
//  onDone(log, change.id, null, 'delete', change.seq, _err, null, _prevEsDoc)
}
