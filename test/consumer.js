import test from 'ava';
import { Observable, Subject } from '@reactivex/rxjs';
import PQueue from 'p-queue';

import { Consumer } from '../lib/consumer';
import { Producer } from '../lib/producer';

test('it exists', t => {
  t.is(typeof Consumer, 'function');
});

test('it requires a url, name and topic', t => {
  t.throws(() => Consumer(), Error);
  t.throws(() => Consumer({}), Error);

  const url = 'localhost:9092';
  const name = 'test';
  const topic = 'mock_output_topic';
  t.notThrows(() => Consumer({ url, name, topic }), Error);
});

test('it returns a source observable and sink subject', t => {
  const url = 'localhost:9092';
  const name = 'test';
  const topic = 'mock_output_topic';
  const res = Consumer({ url, name, topic });
  t.is(res.source instanceof Observable, true);
  t.is(res.sink instanceof Subject, true);
});

test('it should be able to connect to Kafka', t => {
  const url = 'localhost:9092';
  const name = 'test';
  const topic = 'mock_output_topic';
  const producer = Producer({ url, name, topic });

  const quad = { subject: '', predicate: '', object: '' };
  const action = { type: 'write', quad };

  let readCount = 0;

  return new Promise((resolve, reject) => {
    const consumer = Consumer({ url, name, topic });
    const subscription = consumer.source.subscribe({
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

    producer.sink.next(action);
  })
  .then((...args) => {
    t.is(readCount, 1);
  }, t.fail);

});

test('it should start at the latest committed offset', t => {
  const url = 'localhost:9092';
  const name = 'test-commit';
  const topic = 'special_commit_topic';
  const producer = Producer({ url, name, topic });

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

  return Promise.all(actions.map((a, i) => queue.add(async () => {
    console.log('Sending next action. Remaining:', actions.length - 1 - i);
    return producer.sink.next(a);
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
      const consumer1 = Consumer({ url, name: 'test-commit-source1', topic });
      const subscription = consumer1.source.subscribe({
        next: (...args) => {
          readCount += 1;
          const [ [ a, p ] ] = args;

          consumer1.sink.next(p);
          // Read all the actions until the one we just sent
          if (a.quad.subject === 'commit-action') {
            resolve(...args);
            console.log('Done reading. Final readCount:', readCount);
            subscription.unsubscribe();
            return;
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
      const consumer2 = Consumer({ url, name: 'test-commit-source2', topic });
      const subscription = consumer2.source.subscribe({
        next: (...args) => {

          const [ [ a, p ] ] = args;
          readCount2 += 1;

          consumer2.sink.next(p);
          // Read all the actions except one, then unsubscribe
          if (readCount2 === readCount - 1) {
            console.log('Done reading second time. Final readCount:', readCount2);
            resolve(...args);
            subscription.unsubscribe();
            return;
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
      const consumer2 = Consumer({ url, name: 'test-commit-source2', topic });
      // Subscribe one more time to see if we commited the offset correctly
      const subscription = consumer2.source.subscribe({
        next: (...args) => {
          const [ [ a, p ] ] = args;

          // Read the remaining action (which should be the one we initially sent)
          consumer2.sink.next(p);
          resolve(...args);
          // subscription.unsubscribe();
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
    const [ [ a ] ] = args;
    return t.is(a.quad.subject, 'commit-action');
  }, (...args) => t.fail(...args));
});
