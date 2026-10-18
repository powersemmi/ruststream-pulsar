import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * The broker's states as types on the left, and on the right what each transition does on
 * Pulsar: the client built at connect, a consumer and its driver task per subscription, a
 * producer on the first publish to a topic, and the producers closed at shutdown.
 */
export function lifecycle(words: Words): {boxes: Box[]; links: Link[]} {
  const left = (id: string, y: number, href?: string): Box => ({
    id,
    x: 12,
    y,
    w: 156,
    h: 58,
    kind: 'broker',
    code: true,
    lines: lines(words, id),
    href,
  });
  const right = (id: string, y: number, {h = 58, href}: {h?: number; href?: string} = {}): Box => ({
    id,
    x: 192,
    y,
    w: 156,
    h,
    kind: 'core',
    lines: lines(words, id),
    href,
  });
  const boxes: Box[] = [
    {id: 'states', x: 0, y: 0, w: 176, h: 410, kind: 'group', lines: lines(words, 'states')},
    {id: 'pulsar', x: 184, y: 0, w: 176, h: 410, kind: 'group', lines: lines(words, 'pulsar')},
    left('unconnected', 28),
    left('connected', 168, '/architecture/design/'),
    left('closed', 340),
    right('config', 28),
    right('client', 98, {href: '/architecture/pulsar/client/'}),
    right('subscriptions', 168, {h: 72, href: '/architecture/path/'}),
    right('producer', 252),
    right('close', 340),
    {
      id: 'afterShutdown',
      x: 0,
      y: 432,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'afterShutdown'),
    },
  ];
  const links: Link[] = [
    {from: 'unconnected', to: 'connected', label: word(words, 'connect')},
    {from: 'connected', to: 'closed', label: word(words, 'shutdown')},
    {from: 'config', to: 'client'},
    {from: 'client', to: 'subscriptions'},
    {from: 'subscriptions', to: 'producer', dashed: true},
    {from: 'producer', to: 'close'},
  ];
  return {boxes, links};
}
