from abc import ABC, abstractmethod
from Constantes import BinaryOp  # Se asume que defines tus operadores ahí


# EXPRESIONES

class Exp(ABC):
    @abstractmethod
    def accept(self, visitor):
        pass


class NumberExp(Exp):
    def __init__(self, value):
        super().__init__()
        self.value = value

    def accept(self, visitor):
        return visitor.visit(self)


class IdExp(Exp):
    def __init__(self, name):
        super().__init__()
        self.name = name

    def accept(self, visitor):
        return visitor.visit(self)


class BinaryExp(Exp):
    def __init__(self, left, op, right):
        super().__init__()
        self.left = left
        self.op = op
        self.right = right

    def accept(self, visitor):
        return visitor.visit(self)


class BoolExp(Exp):
    def __init__(self, boolean: bool):
        super().__init__()
        self.boolean = boolean

    def accept(self, visitor):
        return visitor.visit(self)


class LogicalExp(Exp):
    def __init__(self, left, op, right):
        super().__init__()
        self.left = left
        self.op = op
        self.right = right

    def accept(self, visitor):
        return visitor.visit_logical_exp(self)


class BetweenExp(Exp):
    def __init__(self, atribute, left, right):
        self.atribute = atribute
        self.left = left
        self.right = right

    def accept(self, visitor):
        return visitor.visit_between_exp(self)


class NotExp(Exp):
    def __init__(self, exp):
        super().__init__()
        self.exp = exp

    def accept(self, visitor):
        return visitor.visit_not_exp(self)


class StringExp(Exp):
    def __init__(self, value):
        super().__init__()
        self.value = value

    def accept(self, visitor):
        return visitor.visit_string_exp(self)


# ESTRUCTURA DE ATRIBUTOS

class Atributo:
    def __init__(self, nombre, tipo, indice=None):
        self.nombre = nombre
        self.tipo = tipo
        self.indice = indice  # 'SEQ', 'BTree', 'RTree', etc.


# SENTENCIAS

class Stms(ABC):
    @abstractmethod
    def accept(self, visitor):
        pass


class SelectStatement(Stms):
    def __init__(self, atributos, table, condition=None):
        super().__init__()
        self.atributos = atributos
        self.table = table
        self.condition = condition

    def accept(self, visitor):
        return visitor.visit_select(self)


class InsertStatement(Stms):
    def __init__(self, table, values, atributos):
        super().__init__()
        self.table = table
        self.values = values
        self.atributos = atributos

    def accept(self, visitor):
        return visitor.visit_insert(self)


class DeleteStatement(Stms):
    def __init__(self, table, condition):
        super().__init__()
        self.table = table
        self.condition = condition

    def accept(self, visitor):
        return visitor.visit_delete(self)


class CreateTable(Stms):
    def __init__(self, name, columns):
        super().__init__()
        self.name = name
        self.columns = columns

    def accept(self, visitor):
        return visitor.visit_create(self)


class CreateTableFromFile(Stms):
    def __init__(self, name, file_path, index_type, index_field):
        super().__init__()
        self.name = name
        self.file_path = file_path
        self.index_type = index_type
        self.index_field = index_field

    def accept(self, visitor):
        return visitor.visit_create_from_file(self)


class CreateIndex(Stms):
    def __init__(self, table, index_name, index_type, list_atributos):
        super().__init__()
        self.table = table
        self.index_name = index_name
        self.index_type = index_type
        self.list_atributos = list_atributos

    def accept(self, visitor):
        return visitor.visit_create_index(self)


class DropIndex(Stms):

    def __init__(self, index_name):
        super().__init__()
        self.index_name = index_name

    def accept(self, visitor):
        return visitor.visit_drop_index(self)

class AlterAddPrimaryKey(Stms):
    def __init__(self, table_name, column_name):
        super().__init__()
        self.table_name = table_name
        self.column_name = column_name

    def accept(self, visitor):
        return visitor.visit_alter_add_primary_key(self)

# PROGRAMA PRINCIPAL

class Program:
    def __init__(self, list_stms):
        self.list_stms = list_stms

    def accept(self, visitor):
        for stmt in self.list_stms:
            stmt.accept(visitor)
