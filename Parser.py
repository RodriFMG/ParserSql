from Token import Token, Type
from Scanner import Scanner
from Objects import Stms


class ParserSQL:

    def __init__(self, scanner: Scanner):
        self.scanner = scanner
        self.previous: Token = None
        self.current: Token = self.scanner.next_token()

        if self.current.cad == "ERR":
            raise ValueError("Error en el Scanner")

    def match(self, ttype: Type):
        if self.check(ttype):
            self.advance()
            return True

        return False


    def check(self, typeToken: Type):
        if self.isAtEnd():
            return False

        return self.current.type == typeToken

    def advance(self):
        if not self.isAtEnd():
            saveToken: Token = self.current

            self.current = self.scanner.next_token()
            self.previous = saveToken

            if self.check(Type.ERR):
                raise ValueError("Error al avanzar.")

            return True

        return False

    def isAtEnd(self):
        return self.current.type == Type.END

    def ParseProgram(self):
        pass

    def ParseStmList(self):
        stms = []



    def ParseStm(self):
        pass
