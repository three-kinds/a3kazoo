# -*- coding: utf-8 -*-
import sys
import logging
from a3py.simplified.concurrence import GracefulExitThread

from kazoo.exceptions import NoNodeError
from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch

from a3kazoo.utils import zk_state_listener, zk_ensure_path
from .abstract_node_change_service import AbstractNodeChangeService

logger = logging.getLogger(__name__)


class WatchNodesThread(GracefulExitThread):

    def __init__(self, conf: dict, nodes_path: str, node_change_service: AbstractNodeChangeService, *args, **kwargs):
        self._conf = conf
        self._nodes_path = nodes_path
        self._node_change_service = node_change_service

        super().__init__(*args, **kwargs)

    def run(self):
        zk = KazooClient(**self._conf)
        zk.add_listener(zk_state_listener(logger, self.exit_event))
        zk.start()

        try:
            zk_ensure_path(zk=zk, path=self._nodes_path, logger=logger)
        except NoNodeError:
            logger.critical(f"NoNodeError: 请确认路径与ACL, {self._nodes_path}")
            zk.stop()
            zk.close()
            sys.exit(-1)

        logger.info(f"开启观察...")
        ChildrenWatch(zk, self._nodes_path, self._node_change_service.on_change)
        self.exit_event.wait()
