import test from 'ava';
import { Observable } from '@reactivex/rxjs';
import PQueue from 'p-queue';

import { createSource } from '../lib/source';
import { createSink } from '../lib/sink';

test('it exists', t => {
  t.is(typeof createSource, 'function');
});

test('it requires a url, name and topic', t => {
  t.throws(() => createSource(), Error);
  t.throws(() => createSource({}), Error);

  const url = 'localhost:9092';
  const name = 'test';
  const topic = 'mock_output_topic';
  t.notThrows(() => createSource({ url, name, topic }), Error);
});

test('it returns an observable source', t => {
  const url = 'localhost:9092';
  const name = 'test';
  const topic = 'mock_output_topic';
  const res = createSource({ url, name, topic });
  t.is(res instanceof Observable, true);
});

test('it should be able to connect to Kafka', t => {
  const url = 'localhost:9092';
  const name = 'test';
  const topic = 'mock_output_topic';
  const sink = createSink({ url, name, topic });

  const quad = { subject: '', predicate: '', object: '' };
  const action = { type: 'write', quad };

  let readCount = 0;

  return new Promise((resolve, reject) => {
    const source = createSource({ url, name, topic });
    const subscription = source.subscribe({
      next: (...args) => {
        readCount += 1;
        resolve(...args);
        subscription.unsubscribe();
      },
      error: (...args) => {
        reject(...args)
      },
      complete: (...args) => {
        reject(...args)
      }
    });

    sink.next(action);
  })
  .then((...args) => {
    t.is(readCount, 1);
  }, t.fail);

});

test('it should commit offset only after having read a messageSet', t => {
  const url = 'localhost:9092';
  const name = 'test-commit';
  const topic = 'special_commit_topic';
  const sink = createSink({ url, name, topic });

  const quad = { subject: 'commit-action', predicate: '', object: '' };
  const action = { type: 'write', quad };

  const fillerAction = { type: 'write', quad: { subject: 'filler', predicate: 'filler', object: 'filler' } };

  let readCount = 0;
  let readCount2 = 0;

  const queue = new PQueue({
    concurrency: 1
  });

  const actions = [
    fillerAction,
    fillerAction,
    fillerAction,
    fillerAction,
    fillerAction,
    action
  ];

  return Promise.all(actions.map((a, i) => queue.add(() => {
    console.log('Sending next action. Remaining:', actions.length - 1 - i);
    return sink.next(a);
  })))

  // return new Promise((resolve, reject) => {
  //
  //
  //   Promise.all(actions.map((a, i) => queue.add(() => {
  //     console.log('Sending next action. Remaining:', actions.length - 1 - i);
  //     return sink.next(a));
  //   })).then(() => resolve());
  //
  //   queue.add(() => {
  //     return
  //   });
  //
  //   sink.next(fillerAction);
  //   sink.subscribe({
  //     next: () => {
  //       if (actions.length === 0) {
  //         console.log('Actions sent. Moving on.')
  //         resolve();
  //         // return;
  //         // return queue.add(() => )
  //       }
  //     }
  //   });
  // })
  .then(() => {
    return new Promise((resolve, reject) => {
      const source1 = createSource({ url, name: 'test-commit-source1', topic });
      const subscription = source1.subscribe({
        next: (...args) => {
          readCount += 1;
          const [ a ] = args;

          // Read all the actions until the one we just sent
          if (a.quad.subject === 'commit-action') {
            resolve(...args);
            console.log('Done reading. Final readCount:', readCount);
            subscription.unsubscribe();
          }
        },
        error: (...args) => {
          reject(...args)
        },
        complete: (...args) => {
          reject(...args)
        }
      });
    });
  })
  .then(() => {
    return new Promise((resolve, reject) => {
      const source2 = createSource({ url, name: 'test-commit-source2', topic });
      const subscription = source2.subscribe({
        next: (...args) => {
          readCount2 += 1;

          // Read all the actions except one, then unsubscribe
          if (readCount2 === readCount - 1) {
            console.log('Done reading second time. Final readCount:', readCount2);
            resolve(...args);
            // subscription.unsubscribe();
          }
        },
        error: (...args) => {
          reject(...args)
        },
        complete: (...args) => {
          reject(...args)
        }
      });
    });
  })
  .then(() => {
    return new Promise((resolve, reject) => {
      const source2 = createSource({ url, name: 'test-commit-source2', topic });
      // Subscribe one more time to see if we commited the offset correctly
      const subscription = source2.subscribe({
        next: (...args) => {
          // Read the remaining action (which should be the one we initially sent)
          resolve(...args);
          subscription.unsubscribe();
        },
        error: (...args) => {
          reject(...args)
        },
        complete: (...args) => {
          reject(...args)
        }
      });
    });
  })
  .then((...args) => {
    const [ a ] = args;
    return t.is(a.quad.subject, 'commit-action');
  }, (...args) => t.fail(...args));
});
