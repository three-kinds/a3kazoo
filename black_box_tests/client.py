# -*- coding: utf-8 -*-
import time
import logging
from a3kazoo.scene_cases.watch_nodes import NodeThread


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    node_thread = NodeThread(
        conf={
            'hosts': '127.0.0.1:2281',
            'keyfile': '/data/ssl/client-key.pem',
            'certfile': '/data/ssl/client.pem',
            'ca': '/data/ssl/ca.pem',
            'use_ssl': True,
            'auth_data': [("digest", "test:pass"), ]
        },
        nodes_path='/nodes/site/aaa:',
        should_force_exit=True
    )
    node_id = node_thread.get_node_id()
    print(f'node_id: {node_id}')
    node_thread.start()

    print('main start')
    time.sleep(10)
    print('main exit')
