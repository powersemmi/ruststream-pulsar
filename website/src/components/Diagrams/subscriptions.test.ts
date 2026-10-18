import {expect, it} from 'vitest';
import {CURSORS, TYPES, subscriptions} from './subscriptions';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('points each subscription straight at the entry its own cursor reads next', () => {
  const {boxes, links} = subscriptions(words);
  for (const [subscription, entry] of Object.entries(CURSORS)) {
    const link = links.find((candidate) => candidate.from === subscription)!;
    expect(link.to).toBe(`e${entry}`);
    const from = boxes.find((box) => box.id === subscription)!;
    const to = boxes.find((box) => box.id === link.to)!;
    expect(from.x + from.w * link.exitAt!).toBe(to.x + to.w / 2);
  }
});

it('draws the four subscription types, each with two consumers', () => {
  const {boxes} = subscriptions(words);
  expect(TYPES).toEqual(['exclusive', 'shared', 'failover', 'keyShared']);
  for (const type of TYPES) {
    expect(boxes.filter((box) => box.id.startsWith(`${type}C`)).map((box) => box.id)).toEqual([
      `${type}C1`,
      `${type}C2`,
    ]);
  }
});
