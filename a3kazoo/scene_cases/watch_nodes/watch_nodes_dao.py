# -*- coding: utf-8 -*-
import sys
from threading import Event
from kazoo.exceptions import NoNodeError
from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch

from a3kazoo.utils import zk_state_listener, zk_ensure_path
from .abstract_node_change_service import AbstractNodeChangeService


class WatchNodesDAO:

    def __init__(self, conf: dict, nodes_path: str, node_change_service: AbstractNodeChangeService):
        self._conf = conf
        self._nodes_path = nodes_path
        self._node_change_service = node_change_service
        self._logger = self._node_change_service.logger

    def run(self):
        exit_event = Event()
        zk = KazooClient(**self._conf)
        zk.add_listener(zk_state_listener(self._logger, exit_event))
        zk.start()

        try:
            zk_ensure_path(zk=zk, path=self._nodes_path, logger=self._logger)
        except NoNodeError:
            self._logger.critical(f"NoNodeError: 请确认路径与ACL, {self._nodes_path}")
            zk.stop()
            zk.close()
            sys.exit(-1)

        self._logger.info(f"开启观察...")
        ChildrenWatch(zk, self._nodes_path, self._node_change_service.on_change)
        exit_event.wait()
