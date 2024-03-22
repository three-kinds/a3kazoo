# -*- coding: utf-8 -*-
import sys
from threading import Event
from logging import Logger
from kazoo.client import KazooState, KazooClient
from kazoo.security import make_digest_acl


def zk_state_listener(logger: Logger, exit_event: Event):
    def _core(state: str):
        if state == KazooState.LOST:
            logger.warning(f"已断开 zookeeper")
        elif state == KazooState.SUSPENDED:
            logger.critical(f"zookeeper 连接已中断，准备退出")
            exit_event.set()
            sys.exit(-1)
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
