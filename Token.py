from enum import Enum


class Type(Enum):
    SELECT = "SELECT",
    FROM = "FROM",
    ID = "ID",
    WHERE = "WHERE",
    END = "END",
    ERR = "ERR",
    PC = "PC"


class Token:

    def __init__(self, t_type: Type, s_token: str = None):
        self.type = t_type
        self.cad = s_token if s_token else t_type.value
