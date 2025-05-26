from dataclasses import dataclass

from dotenv import load_dotenv
from dependency_injector import containers, providers


@dataclass
class UserLogin:
    username: str
    password: str


class Container(containers.DeclarativeContainer):
    load_dotenv("./secrets.env")
    config = providers.Configuration(yaml_files=["config.yml", "./config2.yml"])

    config.x.y.z.from_env("PASSWORD")
    user_login = providers.Singleton(UserLogin, username=config.api.auth.username, password=config.api.auth.password)
    user_login2 = providers.Singleton(UserLogin, username=config.api.auth.username, password=config.x.y.z)


if __name__ == "__main__":
    container = Container()
    print(container.config())
    print(container.user_login().username, container.user_login().password)
