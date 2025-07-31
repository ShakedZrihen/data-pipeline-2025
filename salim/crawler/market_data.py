from dataclasses import dataclass

@dataclass
class Market:
    url: str
    name: str = ""
    password: str = ""
    
