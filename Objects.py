from abc import abstractmethod, ABC


class Stms(ABC):

    @abstractmethod
    def accept(self, Visitor):
        Visitor.visit()


class SelectStatement(Stms, ABC):
    def __init__(self, atributos, table, query=None):

        super().__init__()
        # Todos
        if atributos == "*":
            pass
        else:
            self.atributos = atributos


class Program:

    def __init__(self, ListStms: []):
        self.ListStms = ListStms
