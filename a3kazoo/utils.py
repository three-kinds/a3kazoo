# -*- coding: utf-8 -*-
import sys
import time
from threading import Event, Thread
from logging import Logger
from kazoo.client import KazooState, KazooClient
from kazoo.security import make_digest_acl


class CheckStateThread(Thread):

    def __init__(self, logger: Logger, exit_event: Event, zk: KazooClient, timeout_seconds, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logger
        self.exit_event = exit_event
        self.zk = zk
        self.timeout_seconds = timeout_seconds

    def run(self):
        start_tick = int(time.time())
        while True:
            time.sleep(0.25)
            if self.zk.state not in [KazooState.SUSPENDED, KazooState.LOST]:
                self.logger.info('网络恢复')
                break

            if int(time.time()) - start_tick >= self.timeout_seconds:
                self.logger.critical(f"zookeeper连接被中断，准备退出")
                self.exit_event.set()
                sys.exit(-1)


def zk_state_listener(logger: Logger, exit_event: Event, zk: KazooClient, timeout_seconds: int = 5):
    def _core(state: str):
        if state == KazooState.LOST:
            logger.warning(f"已断开zookeeper的连接")
        elif state == KazooState.SUSPENDED:
            logger.critical(f"网络波动")
            CheckStateThread(logger=logger, exit_event=exit_event, zk=zk, timeout_seconds=timeout_seconds).start()
        else:
            logger.info(f"已连接到zookeeper")

    return _core


def zk_ensure_path(zk: KazooClient, path: str, logger: Logger):
    # zk.ensure_path(self._nodes_path) 这个在高版本没用
    if zk.exists(path=path) is None:
        logger.info(f'没有此路径，准备创建')

        name_list = path.strip('/').split('/')
        ensure_name_list = list()
        for name in name_list:
            ensure_name_list.append(name)
            tmp_path = f'/{"/".join(ensure_name_list)}/'
            if zk.exists(path=path) is None:
                zk.create(path=tmp_path)
                logger.info(f'创建了路径: {tmp_path}')


def zk_make_acl(zk: KazooClient, path: str, username: str, password: str, logger: Logger):
    zk_ensure_path(zk=zk, path=path, logger=logger)
    acl = make_digest_acl(username=username, password=password, all=True)
    zk.set_acls(path, acls=[acl, ])
    logger.info(f'[SUCCESS]ACL创建成功: {path}')
