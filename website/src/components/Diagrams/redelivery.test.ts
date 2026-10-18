import {expect, it} from 'vitest';
import {redelivery} from './redelivery';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('checks the dead-letter limit before the handler sees a delivery', () => {
  const {links} = redelivery(words);
  const from = (id: string): string[] =>
    links.filter((link) => link.from === id).map((link) => link.to);
  expect(from('delivery')).toEqual(['check']);
  expect(from('check')).toEqual(['deadLetter', 'handler']);
  expect(from('handler')).toEqual(['ack', 'nack', 'timeout']);
  // A negative acknowledgement and the client's timer ask for the same redelivery; an ack and
  // a dead letter end there.
  expect(from('nack')).toEqual(['again']);
  expect(from('timeout')).toEqual(['again']);
  expect([...from('ack'), ...from('deadLetter')]).toEqual([]);
});
