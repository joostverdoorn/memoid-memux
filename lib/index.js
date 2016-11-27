var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };

const { Producer, SimpleConsumer, GroupConsumer, LATEST_OFFSET } = require('no-kafka');
const { Observable, Subject, ReplaySubject } = require('rxjs');
const PQueue = require('p-queue');

const OFFSET_COMMIT_INTERVAL = 1000;
const RETENTION_TIME = 1000 * 365 * 24;

const log = (...things) => {
  console.log(new Date().toISOString(), ...things);
};

const onError = error => {
  console.error(error);
  process.exit(1);
};

const isQuad = quad => {
  return typeof quad === 'object' && typeof quad['subject'] === 'string' && typeof quad['predicate'] === 'string' && typeof quad['object'] === 'string';
};

const isAction = action => {
  return typeof action === 'object' && (action['action'] === 'write' || action['action'] === 'delete') && isQuad(action.quad);
};

const memux = ({ url, name, input, output }) => {
  const source = input ? createSource(url, name, input) : null;
  const sink = output ? createSink(url, name, output) : null;
  return { sink, source };
};

const createSource = (connectionString, groupId, topic) => processor => {
  const input = new Subject();
  const output = new Subject();
  const offsets = new Subject();
  const consumer = new SimpleConsumer({ connectionString, groupId });
  const partition = 0;

  consumer.init().then(() => {
    consumer.fetchOffset([{ topic, partition }]).then(([{ offset }]) => {
      consumer.subscribe(topic, partition, { offset }, (messageSet, topic, partition) => {
        messageSet.forEach(({ offset, message: { value } }) => {
          let action;

          try {
            action = JSON.parse(value.toString());
          } catch (error) {
            if (!error instanceof SyntaxError) throw error;
          }

          log('received');
          if (isAction(action)) input.next({ action, topic, partition, offset });
        });
      });
    });
  });

  input.subscribe(({ action, topic, partition, offset }) => {
    const observable = Observable.from(processor(action)).filter(isAction).publishReplay().refCount();
    observable.subscribe(null, onError, () => {
      offsets.next({ topic, partition, offset });
      output.next(observable);
    });
  });

  offsets.bufferTime(OFFSET_COMMIT_INTERVAL).subscribe(offs => {
    consumer.commitOffset(offs).catch(onError);
  });

  return output.concatAll();
};

const createSink = (connectionString, label, topic) => {
  const producer = new Producer({ connectionString });
  const sink = new ReplaySubject();
  const queue = new PQueue({ concurrency: 1 });

  producer.init().then(() => {
    sink.filter(isAction).subscribe(({ action, quad }) => {
      const message = { value: JSON.stringify({ action, quad: _extends({ label }, quad) }) };
      queue.add(() => {
        return producer.send({ topic, message }).then(() => log('sent'));
      }).catch(onError);
    });
  });

  return sink;
};

module.exports = memux;