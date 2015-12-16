
var Backoff = require('backoff');

module.exports = function( fn, backoffStrategy ) {
  var backoffStrategy = backoffStrategy ? backoffStrategy : new Backoff.ExponentialStrategy({
    initialDelay: 500,
    maxDelay: 5000
  });

  var backoff = Backoff.Backoff( backoffStrategy );

  backoff.on('backoff', function(number, delay) {
    console.log('Will retry in', delay, 'ms');
  });

  backoff.on('ready', function() {
    try {
      fn();
      backoff.reset();
    } catch (e) {
      backoff.backoff();
    }
  });

  backoff.backoff();
};