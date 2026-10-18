import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * The pulsar client underneath the crate: it looks a topic up through the one service URL, dials
 * the broker that owns it, and keeps one connection per broker that its producers and consumers
 * share. A consumer grants flow permits ahead; a pattern subscription lists its topics again on a
 * timer.
 */
export function client(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    kind: Box['kind'],
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind, lines: lines(words, id), ...fields});
  const boxes: Box[] = [
    box('url', 12, 0, 336, 44, 'service', {code: true}),
    box('lookup', 12, 72, 336, 44, 'broker'),
    box('pool', 0, 148, 360, 170, 'group'),
    box('producer', 12, 174, 160, 44, 'broker', {code: true}),
    box('consumer', 188, 174, 160, 44, 'broker', {code: true}),
    box('refresh', 12, 232, 160, 74, 'note'),
    box('permits', 188, 232, 160, 74, 'core'),
    box('cluster', 12, 342, 336, 44, 'broker'),
    box('retries', 0, 410, 360, 44, 'note'),
  ];
  const links: Link[] = [
    {from: 'url', to: 'lookup', label: word(words, 'asks')},
    {from: 'lookup', to: 'pool', label: word(words, 'owner')},
    {from: 'pool', to: 'cluster', label: word(words, 'tcp')},
  ];
  return {boxes, links};
}
