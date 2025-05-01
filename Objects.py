from abc import abstractmethod, ABC


class Stms(ABC):

    @abstractmethod
    def accept(self, Visitor):
        Visitor.visit()


class SelectStatement(Stms, ABC):
    def __init__(self, atributos, table, query=None):
        # Todos
        if atributos == "*": # No sería mejor en uno solo?
            pass
        else:
            self.atributos = atributos
            self.table = table
            self.condition = condition

        def accept(self, visitor):
            return visitor.visit_select(self)



class Program:
    def __init__(self, list_stms):
        self.list_stms = list_stms

    def accept(self, visitor):
        for stmt in self.list_stms:
            stmt.accept(visitor)
