from enum import Enum


class BinaryOp(Enum):
    PLUS_OP = "+",
    MINUS_OP = "-",
    MUL_OP = "*",
    DIV_OP = "/",
    LESS_OP = "<",
    EQLESS_OP = "<=",
    MAYOR_OP = ">",
    EQMAYOR_OP = ">=",
    EQUAL_OP = "=",
    AND_OP = "AND",
    OR_OP = "OR",
    NOT_OP = "NOT"