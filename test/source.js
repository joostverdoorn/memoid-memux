import test from 'ava';
import { Observable } from '@reactivex/rxjs';

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

  return new Promise((resolve, reject) => {
    const source = createSource({ url, name, topic });

    console.log('Subscribing to source...');
    source.subscribe({
      next: (...args) => {
        console.log(...args);
        resolve(...args)
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

    // sink.subscribe({
    //   next: () => {
    //   }
    // });

    console.log('Sending action...');
    sink.next(action);

  }).then((...args) => t.pass(...args), t.fail);

});
