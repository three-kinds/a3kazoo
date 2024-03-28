# -*- coding: utf-8 -*-
import logging
from a3py.simplified.concurrence import run_threads_until_any_exits
from a3kazoo.scene_cases.watch_nodes import AbstractNodeChangeService, WatchNodesThread


class NodeChangeService(AbstractNodeChangeService):
    logger = logging.getLogger('bt')

    def handle(self, online_node_id_set: set[str], offline_node_id_set: set[str]):
        # 先处理上线的
        for online_node_id in online_node_id_set:
            self._handle_online(online_node_id)
        # 再处理掉线的
        for offline_node_id in offline_node_id_set:
            self._handle_offline(offline_node_id)

    def _handle_online(self, online_node_id: str):
        self.logger.info(f'on: {online_node_id}')

    def _handle_offline(self, offline_node_id: str):
        self.logger.info(f'off: {offline_node_id}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    watch_nodes_thread = WatchNodesThread(
        conf={
            'hosts': '127.0.0.1:2281',
            'keyfile': '/data/ssl/client-key.pem',
            'certfile': '/data/ssl/client.pem',
            'ca': '/data/ssl/ca.pem',
            'use_ssl': True,
            'auth_data': [("digest", "test:pass"), ]
        },
        nodes_path='/nodes/site/',
        node_change_service=NodeChangeService(),
    )
    run_threads_until_any_exits(thread_list=[watch_nodes_thread, ])
