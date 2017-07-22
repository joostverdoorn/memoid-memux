import test from 'ava';
import { createSource } from '../lib/source';
import { Observable } from '@reactivex/rxjs';

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

// test('it should be able to connect to Kafka', t => {
//   const url = 'localhost:9092';
//   const output = 'mock_output_topic';
//   const name = 'test';
//
//   const log = console.log.bind(console);
//   const error = console.error.bind(console);
//
//   const { send } = memux({ url, output, name });
//   const quad = { subject: '', predicate: '', object: '' };
//   const action = { type: 'write', quad };
//
//   return (async () => send(action))().then(() => t.pass(true), t.fail);
// });
