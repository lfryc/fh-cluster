/*
 Copyright Red Hat, Inc., and individual contributors

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
var backoff = require('backoff');
var cluster = require('cluster');
var os = require('os');
var _ = require('lodash');

module.exports = function fhCluster(workerFunc, optionalNumWorkers, optionalBackoffStrategy) {
  var defaultExponentialBackoffStrategy = new backoff.ExponentialStrategy({
    initialDelay: 500,
    maxDelay: 5000
  });
  var backoffStrategy = _.find([optionalBackoffStrategy, defaultExponentialBackoffStrategy], isValidBackoffStrategy);
  var backoffResetTimeout = resetBackoffResetTimeout(backoffStrategy);

  var numWorkers = _.find([optionalNumWorkers, os.cpus().length], isValidNumWorkers);


  if (cluster.isMaster) {
    startMaster( numWorkers, backoffStrategy, backoffResetTimeout );
  } else {
    startWorker( workerFunc );
  }
};

/**********/
/* MASTER */
/**********/

function startMaster( numWorkers, backoffStrategy, backoffResetTimeout ) {
  process.on('SIGTERM', cleanMasterShutdown('SIGTERM'));
  process.on('SIGHUP', cleanMasterShutdown('SIGHUP'));
  process.on('SIGINT', cleanMasterShutdown('SIGINT'));

  process.on('message', function(msg) {
    if (msg === 'shutdown') {
      cleanMasterShutdown('shutdown')();
    }
  });

  cluster.on('listening', onWorkerListening);
  cluster.on('disconnect', setupOnExitBackOff(backoffStrategy, backoffResetTimeout));

  cluster.on('fork', function(worker) {
    console.log('worker:' + worker.id + " is forked");
    //console.log('state: ' + worker.state);
  });
  cluster.on('online', function(worker) {
    console.log('worker:' + worker.id + " is online");
    //console.log('state: ' + worker.state);
  });
  cluster.on('listening', function(worker) {
    console.log('worker:' + worker.id + " is listening");
    //console.log('state: ' + worker.state);
  });
  cluster.on('disconnect', function(worker) {
    console.log('worker:' + worker.id + " is disconnected");
    //console.log('state: ' + worker.state);
  });
  cluster.on('exit', function(worker) {
    console.log('worker:' + worker.id + " is dead");
    //console.log('state: ' + worker.state);
  });

  _.times(numWorkers, function() {
    cluster.fork();
  });
}

function onWorkerListening(worker, address) {
  var host = address.address || address.addressType === 4 ? '0.0.0.0' : '::';
  var addr = host + ':' + address.port;
  console.log('Cluster worker',  worker.id, 'is now listening at', addr);
}

function setupOnExitBackOff(strategy, backoffResetTimeout) {
  return function(worker) {
    if (worker.suicide) {
      console.log('Worker #', worker.id, 'commited suicide, won\'t retry');
      return;
    }
    var nextRetry = strategy.next();
    console.log('Worker #', worker.id, 'disconnected. Will retry in',
      nextRetry, 'ms');

    setTimeout(function() {
      cluster.fork();
      resetBackoffResetTimeout(strategy, backoffResetTimeout);
    }, nextRetry);
  };
}

function resetBackoffResetTimeout(backoffStrategy, timeout) {
  if (timeout) {
    clearTimeout(timeout);
  }

  return setTimeout(function() {
    backoffStrategy.reset();
  }, 60*60*1000);
}

function cleanMasterShutdown(type) {
  return function (cb) {
    console.log('exiting master with ' + type + ' [' + process.pid + ']');
    eachWorker(function(worker) {
      worker.send('shutdown');
    });
    waitForAllDead(function() {
      console.log('exited master with ' + type + ' [' + process.pid + ']');
      if (cb) return cb();
      process.exit(0);
    });
  }
}

function waitForAllDead(cb) {
  var someAlive = false;
  eachWorker(function(worker) {
    if (!isWorkerDead(worker)) {
      console.log('worker ' + worker.id + ' not dead');
      someAlive = true;
      setTimeout(function() {
        waitForAllDead(cb);
      }, 50);
    }
  });
  if (!someAlive) {
    console.log('all dead');
    cb();
  }
}

function isWorkerDead(worker ) {
  return ('isWorkerDead' in worker) ? worker.isDead() : worker.state === 'dead';
}

function eachWorker(callback) {
  for (var id in cluster.workers) {
    callback(cluster.workers[id]);
  }
}

function isValidNumWorkers(number) {
  return number && typeof number === 'number' && number > 0;
}

function isValidBackoffStrategy(strategy) {
  return strategy
    && _.includes(_.functions(strategy), 'next')
    && _.includes(_.functions(strategy), 'reset');
}

/**********/
/* WORKER */
/**********/

function startWorker( workerFunc ) {
  process.on('SIGTERM', cleanWorkerShutdown('SIGTERM'));
  process.on('SIGHUP', cleanWorkerShutdown('SIGHUP'));
  process.on('SIGINT', cleanWorkerShutdown('SIGINT'));

  workerFunc(cluster.worker);
}

function cleanWorkerShutdown(type) {
  return function (cb) {
    console.log('exiting worker ' + cluster.worker.id + ' with ' + type + ' [' + process.pid + ']');
    if (cb) return cb();
    process.nextTick(function() {
      cluster.worker.kill();
    });
  }
}