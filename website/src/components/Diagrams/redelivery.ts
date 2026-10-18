import type {Box, Link} from './flow';
import {lines, type Words} from './words';

/*
 * What happens to a delivery on Pulsar: the client checks its dead-letter limit before the
 * handler sees it, the handler acknowledges or asks for it again, and a delivery nobody answered
 * is asked for again by the client's own timer. Both requests bring it back with a count one
 * higher.
 */
export function redelivery(words: Words): {boxes: Box[]; links: Link[]} {
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
    box('delivery', 12, 0, 336, 44, 'broker'),
    box('check', 0, 72, 236, 89, 'core'),
    box('deadLetter', 248, 72, 112, 89, 'bad', {href: '/components/settlement/dead-letters/'}),
    box('handler', 12, 189, 336, 44, 'service'),
    box('ack', 0, 261, 112, 58, 'good'),
    box('nack', 124, 261, 112, 58, 'core'),
    box('timeout', 248, 261, 112, 58, 'core', {href: '/components/settlement/ack-timeout/'}),
    box('again', 124, 347, 236, 44, 'broker'),
    box('counted', 0, 419, 360, 44, 'note'),
  ];
  // Where along the handler a link leaves to land straight on a box under it.
  const over = (x: number): number => (x - 12) / 336;
  const links: Link[] = [
    {from: 'delivery', to: 'check', exitAt: over(106), enterAt: 106 / 236},
    {from: 'check', to: 'deadLetter', exit: 'right', enter: 'left'},
    {from: 'check', to: 'handler', exitAt: 106 / 236, enterAt: over(106)},
    {from: 'handler', to: 'ack', exitAt: over(56)},
    {from: 'handler', to: 'nack', exitAt: over(180)},
    {from: 'handler', to: 'timeout', exitAt: over(304)},
    {from: 'nack', to: 'again', enterAt: 56 / 236},
    {from: 'timeout', to: 'again', enterAt: 180 / 236},
  ];
  return {boxes, links};
}
