import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Where the crate sits: the service on top, the core's runtime under it, this crate between the
 * runtime and the pulsar client, and the cluster at the bottom. Each subscription owns a consumer
 * of its own, driven by a task of its own; the publishers of a connection share one producer per
 * topic.
 */
export function design(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  // The crate's columns leave a gutter at each side, where a link from a box to the client under
  // it runs past the boxes in between.
  const left = 28;
  const right = 182;
  const column = 150;
  const centre = (x: number): number => (x + column / 2 - 12) / 336;
  const boxes: Box[] = [
    box('service', 0, 0, 360, 80, {kind: 'group'}),
    box('handlers', 12, 26, 160, 42, {kind: 'service', code: true}),
    box('app', 188, 26, 160, 42, {kind: 'service', code: true}),
    box('core', 0, 104, 360, 72, {kind: 'group'}),
    box('runtime', 12, 128, 336, 36, {kind: 'core'}),
    box('crate', 0, 200, 360, 204, {kind: 'group'}),
    box('subscriber', left, 226, column, 58, {
      kind: 'broker',
      code: true,
      href: '/architecture/path/',
    }),
    box('publishers', right, 226, column, 58, {
      kind: 'broker',
      code: true,
      href: '/components/publishing/',
    }),
    box('message', left, 294, column, 30, {
      kind: 'broker',
      code: true,
      href: '/architecture/settlement/',
    }),
    box('seeker', right, 294, column, 30, {
      kind: 'broker',
      code: true,
      href: '/components/positions/seek/',
    }),
    box('broker', left, 334, column, 58, {
      kind: 'broker',
      code: true,
      href: '/components/broker/',
    }),
    box('descriptors', right, 334, column, 58, {
      kind: 'broker',
      code: true,
      href: '/components/subscriptions/',
    }),
    box('client', 0, 428, 360, 72, {kind: 'group'}),
    box('consumer', left, 454, column, 36, {
      kind: 'broker',
      code: true,
      href: '/architecture/pulsar/client/',
    }),
    box('producer', right, 454, column, 36, {
      kind: 'broker',
      code: true,
      href: '/architecture/pulsar/client/',
    }),
    box('cluster', 12, 524, 336, 36, {kind: 'broker', href: '/architecture/pulsar/'}),
    box('inProcess', 0, 584, 360, 44, {kind: 'note'}),
  ];
  const links: Link[] = [
    {from: 'handlers', to: 'runtime', enterAt: 80 / 336, label: word(words, 'mounted')},
    {from: 'runtime', to: 'subscriber', exitAt: centre(left), label: word(words, 'polls')},
    {from: 'subscriber', to: 'consumer', exit: 'left', enter: 'left'},
    {from: 'publishers', to: 'producer', exit: 'right', enter: 'right'},
    {from: 'consumer', to: 'cluster', enterAt: centre(left)},
    {from: 'producer', to: 'cluster', enterAt: centre(right)},
  ];
  return {boxes, links};
}
