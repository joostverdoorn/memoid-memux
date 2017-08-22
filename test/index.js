import test from 'ava';
import memux, { isOperation, isProgress } from '../lib';

test('isOperation', t => {
  const quad = { subject: '', predicate: '', object: '' };

  let operation;
  operation = { action: 'write', key: 'bla', data: quad };
  t.is(isOperation(operation), true);
  operation = { action: 'delete', key: 'bla', data: quad };
  t.is(isOperation(operation), true);

  let notOperation;
  t.not(isOperation(notOperation), true);
  notOperation = null;
  t.not(isOperation(notOperation), true);
  notOperation = 1234;
  t.not(isOperation(notOperation), true);
  notOperation = 'test';
  t.not(isOperation(notOperation), true);
  notOperation = {};
  t.not(isOperation(notOperation), true);
  notOperation = () => {};
  t.not(isOperation(notOperation), true);
});

test('isProgress', t => {
  const progress = { offset: 1234, partition: 1234, topic: '' };
  t.is(isProgress(progress), true);

  let notProgress;
  t.not(isProgress(notProgress), true);
  notProgress = null;
  t.not(isProgress(notProgress), true);
  notProgress = 1234;
  t.not(isProgress(notProgress), true);
  notProgress = 'test';
  t.not(isProgress(notProgress), true);
  notProgress = {};
  t.not(isProgress(notProgress), true);
  notProgress = () => {};
  t.not(isProgress(notProgress), true);
});

test('it send and receives', async (t) => {
  try {
    let _resolve, _reject;
    const resultPromise = new Promise((resolve, reject) => {
      _resolve = resolve;
      _reject = reject;
    });

    const receive = (message) => {
      console.log('Received message!', message);
      _resolve(message);
    };

    const send = await memux({
      name: 'dummy-broker',
      url: 'tcp://localhost:9092',
      input: 'test_update_requests',
      output: 'test_update_requests',
      receive,
      options: {
        concurrency: 1
      }
    });

    const doc = {
      '@id': 'http://some.domain/testdoc',
      'http://schema.org/name': [ 'bla' ],
    };
    const operation = { action: 'write', key: doc['@id'], data: doc}

    const waitingPromise = new Promise((resolve) => {
      setTimeout(() => resolve(), 2000);
    });

    await send(operation);

    let result = await resultPromise;
    return t.deepEqual(result, { ...operation, label: 'dummy-broker' });
  } catch(e) {
    console.error(e);
    throw e;
  }
});
