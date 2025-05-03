from enum import Enum


class Type(Enum):
    #palabras claves
    SELECT = "SELECT"
    FROM = "FROM"
    WHERE = "WHERE"
    BETWEEN = "BETWEEN"
    INSERT = "INSERT"
    INTO = "INTO"
    VALUES = "VALUES"
    DELETE = "DELETE"
    CREATE = "CREATE"
    TABLE = "TABLE"
    FILE = "FILE"
    USING = "USING"
    INDEX = "INDEX"
    PRIMARY = "PRIMARY"
    KEY = "KEY"

    #tipo de datos
    INT = "INT"
    TEXT = "TEXT"
    DATE = "DATE"
    ARRAY = "ARRAY"
    FLOAT = "FLOAT"

    #operadores y simbolos
    PLUS = "+"
    MINUS = "-"
    MUL = "*"
    DIV = "/"
    EQ = "="
    LT = "<"
    GT = ">"
    LE = "<="
    GE = ">="
    NEQ = "<>"

    COMA = ","
    SEMICOLON = ";"
    LPAREN = "("
    RPAREN = ")"
    LBRACKET = "["
    RBRACKET = "]"
    STAR = "*"


    #literales
    STRING = "STRING"
    NUMBER = "NUMBER"
    ID = "ID"

    #final y error
    EOF = "EOF"
    ERR = "ERR"


class Token:
    def __init__(self, t_type: Type, s_token: str = None):
        self.type = t_type
        self.cad = s_token if s_token else t_type.value

    def __repr__(self):
        return f"TOKEN({self.type.name}, '{self.cad}')"

