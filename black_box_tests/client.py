# -*- coding: utf-8 -*-
import logging
import time
from kazoo.client import KazooClient
from a3kazoo.utils import zk_state_listener


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    zk = KazooClient(**{
        'hosts': '127.0.0.1:2281',
        'keyfile': '/data/ssl/client-key.pem',
        'certfile': '/data/ssl/client.pem',
        'ca': '/data/ssl/ca.pem',
        'use_ssl': True,
        'auth_data': [("digest", "test:pass"), ]
    })
    zk.add_listener(zk_state_listener(logger=logging.getLogger(__name__)))
    zk.start()

    path = '/nodes/site/aaa:'
    value = zk.create(path=path, ephemeral=True, sequence=True)
    node_id = value.rsplit('/', 2)[-1]
    print(node_id)

    time.sleep(2)
    zk.stop()
    zk.close()
