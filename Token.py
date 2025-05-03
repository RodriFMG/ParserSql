from enum import Enum

class Type(Enum):
    # Palabras clave
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

    # Tipos de datos
    INT = "INT"
    TEXT = "TEXT"
    DATE = "DATE"
    ARRAY = "ARRAY"
    FLOAT = "FLOAT"

    # Literales booleanos
    TRUE = "TRUE"
    FALSE = "FALSE"

    # Operadores lógicos
    AND = "AND"
    OR = "OR"
    NOT = "NOT"

    # Símbolos y operadores
    STAR = "*"
    EQ = "="
    COMA = ","
    SEMICOLON = ";"
    LPAREN = "("
    RPAREN = ")"
    LBRACKET = "["
    RBRACKET = "]"

    # Operadores binarios
    PLUS = "+"
    MINUS = "-"
    DIV = "/"
    LESS = "<"
    EQLESS = "<="
    MAYOR = ">"
    EQMAYOR = ">="
    NEQ = "<>"

    # Literales
    STRING = "STRING"
    NUMBER = "NUMBER"
    ID = "ID"

    # Final y error
    EOF = "EOF"
    ERR = "ERR"

class Token:
    def __init__(self, t_type: Type, s_token: str = None):
        self.type = t_type
        self.text = s_token if s_token else t_type.value

    def __repr__(self):
        return f"TOKEN({self.type.name}, '{self.text}')"


