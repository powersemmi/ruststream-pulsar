import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * A retry on Pulsar, where nothing is republished: the delivery is negatively acknowledged, at
 * once or after a wait held in the process, the broker delivers it again with its count raised,
 * and the client's dead-letter policy moves it on once the count reaches the cap.
 */
export function retries(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    kind: Box['kind'],
  ): Box => ({id, x, y, w, h, kind, lines: lines(words, id)});
  const boxes: Box[] = [
    box('delivery', 12, 0, 336, 44, 'broker'),
    box('outcome', 12, 72, 336, 44, 'service'),
    box('now', 0, 148, 172, 58, 'core'),
    box('later', 188, 148, 172, 58, 'core'),
    box('redelivered', 12, 238, 336, 44, 'broker'),
    box('cap', 12, 310, 336, 44, 'core'),
    box('again', 0, 386, 172, 58, 'step'),
    box('deadLetter', 188, 386, 172, 58, 'bad'),
    box('refused', 0, 468, 360, 44, 'note'),
    box('crash', 0, 524, 360, 44, 'note'),
  ];
  // Where along a full-width box a link meets the centre of a half-width box above or below it.
  const left = (86 - 12) / 336;
  const right = (274 - 12) / 336;
  const links: Link[] = [
    {from: 'delivery', to: 'outcome', label: word(words, 'handled')},
    {from: 'outcome', to: 'now', exitAt: left},
    {from: 'outcome', to: 'later', exitAt: right},
    {from: 'now', to: 'redelivered', enterAt: left},
    {from: 'later', to: 'redelivered', enterAt: right},
    {from: 'redelivered', to: 'cap'},
    {from: 'cap', to: 'again', exitAt: left, label: word(words, 'below')},
    {from: 'cap', to: 'deadLetter', exitAt: right, label: word(words, 'atCap')},
  ];
  return {boxes, links};
}
