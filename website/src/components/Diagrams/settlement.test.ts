import {describe, expect, it} from 'vitest';
import {OUTCOMES, settlement} from './settlement';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

describe('what a delivery asks the client for each outcome', () => {
  it('is the call the crate makes on the client consumer', () => {
    // `ack` and `nack(false)` call `ack_with_id`, `nack(true)` and the end of a delayed retry
    // call `nack_with_id`; an unsettled delivery calls nothing, and the client's timer acts.
    expect(
      Object.fromEntries(OUTCOMES.map((outcome) => [outcome.id, [outcome.call, outcome.held]])),
    ).toEqual({
      ack: ['ack_with_id', false],
      retry: ['nack_with_id', false],
      retryAfter: ['nack_with_id', true],
      drop: ['ack_with_id', false],
      unsettled: [null, false],
    });
  });

  it('draws each outcome to its call and on to what the broker does', () => {
    const {links} = settlement(words);
    for (const outcome of OUTCOMES) {
      const out = links.filter((link) => link.from === outcome.id).map((link) => link.to);
      expect(out).toEqual([`${outcome.id}Call`]);
      const effect = links.filter((link) => link.from === `${outcome.id}Call`).map((l) => l.to);
      expect(effect).toEqual([`${outcome.id}Effect`]);
    }
  });

  it('writes a delayed call as the wait, then the call', () => {
    const {boxes} = settlement(words);
    expect(boxes.find((box) => box.id === 'retryAfterCall')!.lines).toEqual([
      'held',
      'nack_with_id',
    ]);
    expect(boxes.find((box) => box.id === 'unsettledCall')!.lines).toEqual(['clientTimer']);
  });
});
