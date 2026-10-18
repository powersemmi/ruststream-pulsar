import {expect, it} from 'vitest';
import {PARTITIONS, lanes} from './lanes';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('reads every partition through one consumer and one stream before the lanes', () => {
  const {links} = lanes(words);
  const from = (id: string): string[] =>
    links.filter((link) => link.from === id).map((link) => link.to);
  for (let partition = 0; partition < PARTITIONS; partition += 1) {
    expect(from(`p${partition}`)).toEqual(['consumer']);
  }
  expect(from('consumer')).toEqual(['stream']);
  expect(from('stream')).toEqual(['keyed']);
  expect(from('keyed')).toEqual(['lane0', 'lane1']);
});
