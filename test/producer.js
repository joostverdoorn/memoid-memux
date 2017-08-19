import test from 'ava';
import { Producer } from '../lib/producer';
import { Observable, Subject } from '@reactivex/rxjs';

test('it exists', t => {
  t.is(typeof Producer, 'function');
});

test('it requires a url, name and topic', t => {
  t.throws(() => Producer(), Error);
  t.throws(() => Producer({}), Error);

  const url = 'localhost:9092';
  const name = 'test';
  const topic = 'mock_output_topic';
  t.notThrows(() => Producer({ url, name, topic }), Error);
});

test('it returns a source observable and sink subject', t => {
  const url = 'localhost:9092';
  const name = 'test';
  const topic = 'mock_output_topic';
  const res = Producer({ url, name, topic });
  t.is(res.source instanceof Observable, true);
  t.is(res.sink instanceof Subject, true);
});

test('it should be able to connect to Kafka', t => {
  const url = 'localhost:9092';
  const name = 'test';
  const topic = 'mock_output_topic';
  const producer = Producer({ url, name, topic });

  const quad = { subject: 'sink-subject', predicate: 'sink-predicate', object: 'sink-object' };
  const action = { type: 'write', quad };

  producer.sink.next({ action });

  return new Promise((resolve, reject) => {
    const subscription = producer.source.subscribe({
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
    const [ { action: a } ] = args;
    return t.is(JSON.stringify(a), JSON.stringify({ type: action.type, quad: Object.assign({ label: name }, action.quad) }));
  }, (...args) => t.fail(...args));
});
