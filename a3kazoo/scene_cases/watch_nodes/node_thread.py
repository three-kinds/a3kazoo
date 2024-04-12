# -*- coding: utf-8 -*-
import logging
import atexit
from typing import Optional
from a3py.simplified.concurrence import force_exit_from_threads
from threading import Thread, Event
from kazoo.client import KazooClient
from a3kazoo.utils import zk_state_listener


logger = logging.getLogger(__name__)


class NodeThread(Thread):

    def __init__(self, conf: dict, nodes_path: str, should_force_exit: bool, *args, **kwargs):
        self._conf = conf
        self._nodes_path = nodes_path
        self._should_force_exit = should_force_exit
        self._zk: Optional[KazooClient] = None
        self._exit_event = Event()
        # 配置为守护线程
        super().__init__(daemon=True, *args, **kwargs)

    def get_node_id(self) -> str:
        zk = KazooClient(**self._conf)
        zk.add_listener(zk_state_listener(logger=logger, exit_event=self._exit_event, zk=zk, timeout_seconds=self._conf.get('timeout')))
        zk.start()

        value = zk.create(path=self._nodes_path, ephemeral=True, sequence=True)
        node_id = value.rsplit('/', 2)[-1]
        logger.info(f'获得节点id: {node_id}')

        def _close_zk(zk_client: KazooClient):
            logger.info(f'退出时断开zookeeper')
            zk_client.stop()
            zk_client.close()

        # 当进程退出时（非zookeeper影响），主动关闭连接
        atexit.register(_close_zk, zk)
        self._zk = zk
        return node_id

    def run(self):
        # 如果不使用线程等，当服务端挂掉，客户端还不知道，会继续执行
        self._exit_event.wait()

        self._exit_event.clear()

        # 当与zookeeper服务端断开连接时，主动将进程退出
        if self._should_force_exit:
            force_exit_from_threads(f'因与zookeeper服务端断开连接，所以整个进程退出')
