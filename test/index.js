import test from 'ava';
import memux from '../lib';

test('it exists', t => {
  t.not(memux, undefined);
});

test('it returns a source', t => {
  const res = memux({});
  t.is('source' in res, true);
});

test('it returns a sink', t => {
  const res = memux({});
  t.is('sink' in res, true);
});

test('it should be able to connect to Kafka', t => {
  const url = 'localhost:9092';
  const output = 'mock_output_topic';
  const name = 'test';

  const log = console.log.bind(console);
  const error = console.error.bind(console);

  const { send } = memux({ url, output, name });
  const quad = { subject: '', predicate: '', object: '' };
  const action = { type: 'write', quad };

  return (async () => send(action))().then(() => t.pass(true), t.fail);
});
