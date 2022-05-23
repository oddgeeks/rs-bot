'use strict';

var nextTick;
if (typeof process !== 'undefined' && process && typeof process.nextTick === 'function') {
  nextTick = process.nextTick;
} else {
  nextTick = (callback) => setTimeout(callback, 0);
}

module.exports = function Semaphore(capacity) {
  let semaphore = {
    capacity: capacity || 1,
    running: 0,
    waiting: [],

    Wait: () => {
      if (semaphore.running === semaphore.capacity) {
        return new Promise((resolve) => semaphore.waiting.push(resolve));
      } else {
        semaphore.running++;
        return Promise.resolve();
      }
    },

    Release: () => {
      if (semaphore.running === semaphore.capacity) {
        let next = semaphore.waiting.shift();
        if (next !== undefined) {
          nextTick(next);
          return;
        }
      }
      semaphore.running--;
    },

    Available: (n) => {
      n = n || 1;
      return semaphore.current + n <= semaphore.capacity;
    },
  };
  return semaphore;
};
