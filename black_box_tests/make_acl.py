# -*- coding: utf-8 -*-
import logging

from kazoo.client import KazooClient
from a3kazoo.utils import zk_make_acl, zk_state_listener

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    zk = KazooClient(**{
        'hosts': '127.0.0.1:2281',
        'keyfile': '/data/ssl/client-key.pem',
        'certfile': '/data/ssl/client.pem',
        'ca': '/data/ssl/ca.pem',
        'use_ssl': True,
    })
    zk.add_listener(zk_state_listener(logger=logger))
    zk.start()

    zk_make_acl(zk=zk, path='/nodes/site/', username='test', password='pass', logger=logger)

    zk.stop()
    zk.close()
