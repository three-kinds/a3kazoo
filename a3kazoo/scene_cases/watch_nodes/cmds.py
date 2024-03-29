# -*- coding: utf-8 -*-
import sys
import logging
from threading import Event

from kazoo.exceptions import NoNodeError
from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch

from a3kazoo.utils import zk_state_listener, zk_ensure_path
from .abstract_node_change_service import AbstractNodeChangeService


def run_watch_nodes_server(conf: dict, nodes_path: str, node_change_service: AbstractNodeChangeService, exit_event: Event, logger: logging.Logger):
    zk = KazooClient(**conf)
    zk.add_listener(zk_state_listener(logger, exit_event))
    zk.start()

    try:
        zk_ensure_path(zk=zk, path=nodes_path, logger=logger)
    except NoNodeError:
        logger.critical(f"NoNodeError: 请确认路径与ACL, {nodes_path}")
        zk.stop()
        zk.close()
        sys.exit(-1)

    logger.info(f"开启观察...")
    ChildrenWatch(zk, nodes_path, node_change_service.on_change)
    exit_event.wait()
