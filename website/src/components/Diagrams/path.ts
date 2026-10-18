import type {Box, Link} from './flow';
import {chain} from './chain';
import type {Words} from './words';

/** The path of one message that is answered, from the client consumer to its acknowledgement. */
export function path(words: Words): {boxes: Box[]; links: Link[]} {
  return chain(words, [
    {key: 'consumer', kind: 'broker', href: '/architecture/pulsar/client/'},
    {key: 'take', kind: 'broker', href: '/components/context/'},
    {key: 'dispatch', kind: 'core', href: '/architecture/lanes/'},
    {key: 'decode'},
    {key: 'body', kind: 'service', href: '/components/subscriptions/'},
    {key: 'encode'},
    {key: 'publisher', kind: 'broker', href: '/components/publishing/'},
    {key: 'settle', kind: 'core', href: '/components/settlement/'},
    {key: 'ack', kind: 'broker', href: '/architecture/pulsar/acknowledgement/'},
  ]);
}
