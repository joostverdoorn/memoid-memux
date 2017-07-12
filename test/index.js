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
