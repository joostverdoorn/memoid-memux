var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };

const { Producer, SimpleConsumer, GroupConsumer, LATEST_OFFSET, EARLIEST_OFFSET } = require('no-kafka');
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
  return typeof quad === 'object' && typeof quad.subject === 'string' && typeof quad.predicate === 'string' && typeof quad.object === 'string';
};

const isAction = action => {
  return typeof action === 'object' && (action.type === 'write' || action.type === 'delete') && isQuad(action.quad);
};

const isProgress = progress => {
  return typeof progress === 'object' && typeof progress.offset === 'number' && typeof progress.partition === 'number' && typeof progress.topic === 'string';
};

const memux = ({ url, name, input = null, output = null }) => {
  const { source, sink } = input ? createSource(url, name, input) : {};
  const send = output ? createSend(url, name, output) : null;
  return { source, sink, send };
};

const createSource = (connectionString, groupId, topic) => {
  const sink = new Subject();
  const source = new Subject();
  const consumer = new SimpleConsumer({ connectionString, groupId, recoveryOffset: EARLIEST_OFFSET });
  const partition = 0;

  consumer.init().then(() => {
    sink.bufferTime(OFFSET_COMMIT_INTERVAL).subscribe(progress => {
      consumer.commitOffset(progress).catch(onError);
    }, onError);

    consumer.fetchOffset([{ topic, partition }]).then(([{ offset }]) => {
      offset = offset === LATEST_OFFSET ? EARLIEST_OFFSET : offset;
      // console.log(offset);

      consumer.subscribe(topic, partition, { time: EARLIEST_OFFSET }, (messageSet, topic, partition) => {
        messageSet.forEach(({ offset, message: { value } }) => {
          const data = value.toString();
          const progress = { topic, partition, offset };

          let action;

          try {
            action = JSON.parse(data);
          } catch (error) {
            if (!error instanceof SyntaxError) throw error;
          }

          log('RECV', data);
          if (isAction(action)) source.next({ action, progress });
        });
      });
    });
  });

  return { source, sink };
};

const createSend = (connectionString, label, topic) => {
  const producer = new Producer({ connectionString });
  const ready = producer.init().catch(onError);

  return ({ type, quad }) => {
    if (!isAction({ type, quad })) return onError(new Error('Trying to send a non-action: ' + JSON.stringify({ type, quad })));
    const value = JSON.stringify({ type, quad: _extends({ label }, quad) });

    return ready.then(() => {
      return producer.send({ topic, message: { value } });
    }).then(() => {
      return log('SEND', value);
    });
  };
};

module.exports = memux;