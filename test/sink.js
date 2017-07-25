import test from 'ava';
import { createSink } from '../lib/sink';
import { Subject } from '@reactivex/rxjs';

test('it exists', t => {
  t.is(typeof createSink, 'function');
});

test('it requires a url, name and topic', t => {
  t.throws(() => createSink(), Error);
  t.throws(() => createSink({}), Error);

  const url = 'localhost:9092';
  const name = 'test';
  const topic = 'mock_output_topic';
  t.notThrows(() => createSink({ url, name, topic }), Error);
});

test('it returns a sink subject', t => {
  const url = 'localhost:9092';
  const name = 'test';
  const topic = 'mock_output_topic';
  const res = createSink({ url, name, topic });
  t.is(res instanceof Subject, true);
});

test('it should be able to connect to Kafka', t => {
  const url = 'localhost:9092';
  const name = 'test';
  const topic = 'mock_output_topic';
  const sink = createSink({ url, name, topic });

  const quad = { subject: 'sink-subject', predicate: 'sink-predicate', object: 'sink-object' };
  const action = { type: 'write', quad };

  sink.next(action);

  return new Promise((resolve, reject) => {
    const subscription = sink.subscribe({
      next: (...args) => {
        // console.log(...args);
        resolve(...args)
        subscription.unsubscribe();
      },
      error: (...args) => {
        // console.log(...args);
        reject(...args)
      },
      complete: (...args) => {
        // console.log(...args);
        reject(...args)
      }
    });
  }).then((...args) => {
    const [ a ] = args;
    return t.is(JSON.stringify(a), JSON.stringify(action));
  }, (...args) => t.fail(...args));
});
