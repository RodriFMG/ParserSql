from abc import abstractmethod, ABC


class Exp(ABC):
    @abstractmethod
    def accept(self, visitor):
        pass

class NumberExp(Exp):
    def __init__(self, value):
        self.value = int(value)

    def accept(self, visitor):
        return visitor.visit_number_exp(self)

class IdentifierExp(Exp):
    def __init__(self, name):
        self.name = name

    def accept(self, visitor):
        return visitor.visit_identifier_exp(self)

class BinaryExp(Exp):
    def __init__(self, left, op, right):
        self.left = left      # Exp
        self.op = op          # str
        self.right = right    # Exp

    def accept(self, visitor):
        return visitor.visit_binary_exp(self)


class Stms(ABC):
    @abstractmethod
    def accept(self, visitor):
        pass

class SelectStatement(Stms):
    def __init__(self, atributos, table, condition=None):
        self.atributos = atributos
        self.table = table
        self.condition = condition

    def accept(self, visitor):
        return visitor.visit_select(self)


class InsertStatement(Stms):
    def __init__(self, table, values):
        self.table = table
        self.values = values

    def accept(self, visitor):
        return visitor.visit_insert(self)

class DeleteStatement(Stms):
    def __init__(self, table, condition):
        self.table = table
        self.condition = condition

    def accept(self, visitor):
        return visitor.visit_delete(self)


    def accept(self, visitor):
        return visitor.visit_delete(self)

class CreateTable(Stms):
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns  #lista

    def accept(self, visitor):
        return visitor.visit_create(self)

class Program:
    def __init__(self, list_stms):
        self.list_stms = list_stms

    def accept(self, visitor):
        for stmt in self.list_stms:
            stmt.accept(visitor)
