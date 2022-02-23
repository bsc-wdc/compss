from dataclasses import dataclass


@dataclass
class App:
    name: str
    remote_dir: str = None