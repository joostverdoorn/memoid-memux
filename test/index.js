import test from 'ava';
import { isQuad, isAction, isProgress } from '../lib';

test('isQuad', t => {
  const quad = { subject: '', predicate: '', object: '' };
  t.is(isQuad(quad), true);

  let notQuad;
  t.not(isQuad(notQuad), true);
  notQuad = null;
  t.not(isQuad(notQuad), true);
  notQuad = 1234;
  t.not(isQuad(notQuad), true);
  notQuad = 'test';
  t.not(isQuad(notQuad), true);
  notQuad = {};
  t.not(isQuad(notQuad), true);
  notQuad = () => {};
  t.not(isQuad(notQuad), true);
});

test('isAction', t => {
  const quad = { subject: '', predicate: '', object: '' };

  let action;
  action = { type: 'write', quad };
  t.is(isAction(action), true);
  action = { type: 'delete', quad };
  t.is(isAction(action), true);

  let notAction;
  t.not(isAction(notAction), true);
  notAction = null;
  t.not(isAction(notAction), true);
  notAction = 1234;
  t.not(isAction(notAction), true);
  notAction = 'test';
  t.not(isAction(notAction), true);
  notAction = {};
  t.not(isAction(notAction), true);
  notAction = () => {};
  t.not(isAction(notAction), true);
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
