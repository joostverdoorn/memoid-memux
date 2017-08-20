import test from 'ava';
import { isOperation, isProgress } from '../lib';

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
