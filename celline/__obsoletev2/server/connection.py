import paramiko
from celline.config import Config
import toml
import os
from typing import Dict, Optional


class RemoteServer:
    SERVER_SETTING_PATH = "server_connection.toml"

    class ServerSettingStruct:
        name: str
        ip: str
        uname: str
        secretkey_path: str
        port: int = 22

        def __init__(self, name: str, ip: str, uname: str, secretkey_path: str, port: int) -> None:
            self.name = name
            self.ip = ip
            self.uname = uname
            self.secretkey_path = secretkey_path
            self.port = port

    __cached_server_settings: Optional[Dict[str, ServerSettingStruct]] = None
    target_server = ServerSettingStruct

    @staticmethod
    def servers():
        if RemoteServer.__cached_server_settings is None:
            FULL_PATH = f"{Config.EXEC_ROOT}/{RemoteServer.SERVER_SETTING_PATH}"
            RemoteServer.__cached_server_settings = {}
            if os.path.isfile(FULL_PATH):
                with open(FULL_PATH, mode="r") as f:
                    server_data = toml.load(f)["servers"]
                    for _s in server_data:
                        setting = RemoteServer.ServerSettingStruct(
                            **server_data[_s])
                        RemoteServer.__cached_server_settings[setting.name] = setting
        return RemoteServer.__cached_server_settings

    @staticmethod
    def contains(server_name: str) -> bool:
        return server_name in RemoteServer.servers()

    @staticmethod
    def add_or_update_server(name: str, ip: str, uname: str, secretkey_path: str, port: int):
        if RemoteServer.__cached_server_settings is None:
            RemoteServer.__cached_server_settings = RemoteServer.servers()
        RemoteServer.__cached_server_settings[name] = RemoteServer.ServerSettingStruct(
            name, ip, uname, secretkey_path, port
        )
        RemoteServer.__flush()
        return

    @staticmethod
    def delete(name: str) -> bool:
        if not RemoteServer.contains(name):
            return False
        if RemoteServer.__cached_server_settings is None:
            return False
        del RemoteServer.__cached_server_settings[name]
        RemoteServer.__flush()
        return True

    @staticmethod
    def __flush():
        FULL_PATH = f"{Config.EXEC_ROOT}/{RemoteServer.SERVER_SETTING_PATH}"
        data = {}
        servers = RemoteServer.servers()
        for s in servers:
            data[s] = vars(RemoteServer.servers()[s])
        with open(FULL_PATH, mode="w") as f:
            toml.dump({
                "servers": data
            }, f)
        return

    def __init__(self, name: str) -> None:
        if not RemoteServer.contains(name):
            raise ModuleNotFoundError(
                f"Could not find target server. Please add {name}")
        self.target_server = RemoteServer.servers()[name]
        pass


class ServerConnection(object):
    client: paramiko.SSHClient
    server: RemoteServer

    def __init__(self,  server: RemoteServer) -> None:
        self.server = server
        pass

    def __enter__(self):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(hostname=self.server.target_server.ip, port=self.server.target_server.port,
                            username=self.server.target_server.uname, key_filename=self.server.target_server.secretkey_path)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def execute(self, cmd: str):
        return self.client.exec_command(cmd)

    def close(self):
        self.client.close()
