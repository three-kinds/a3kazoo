# -*- coding: utf-8 -*-
import abc
import sys
import logging
from typing import Set, List


class AbstractNodeChangeService(abc.ABC):
    logger = logging.getLogger(__name__)

    def __init__(self):
        self._last_node_id_set: Set | None = None

    def on_change(self, current_node_id_list: List[str]):
        if self._last_node_id_set is None:
            self._last_node_id_set = set(current_node_id_list)
            if len(current_node_id_list) > 0:
                self.logger.critical(f"服务端启动时，节点列表却不为空，无法继续")
                sys.exit(-1)
            return

        new_node_id_set = set(current_node_id_list)

        online_node_id_set = new_node_id_set - self._last_node_id_set
        offline_node_id_set = self._last_node_id_set - new_node_id_set

        self.handle(online_node_id_set, offline_node_id_set)

        # 保存last_node_id_set
        self._last_node_id_set = new_node_id_set

    @abc.abstractmethod
    def handle(self, online_node_id_set: set[str], offline_node_id_set: set[str]):
        raise NotImplementedError()
