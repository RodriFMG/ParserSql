from Token import Token, Type
from Scanner import Scanner
from Objects import Stms, Program, SelectStatement, NumberExp, IdExp, BoolExp, BinaryExp, StringExp, InsertStatement, DeleteStatement, CreateTable, CreateTableFromFile
from Constantes import BinaryOp

class ParserSQL:

    def __init__(self, scanner: Scanner):
        self.scanner = scanner
        self.previous: Token = None
        self.current: Token = self.scanner.next_token()

        if self.current.type == Type.ERR:
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

            if self.current.type == Type.ERR:
                raise ValueError("Error al avanzar.")
            return True
        return False

    def isAtEnd(self):
        return self.current.type == Type.EOF

    def ParseProgram(self):
        program = Program(self.ParseStmList())
        return program

    def ParseStmList(self):
        stms = [self.ParseStm()]
        while self.match(Type.SEMICOLON):
            if self.isAtEnd():
                break
            stms.append(self.ParseStm())
        return stms

    def ParseStm(self):
        if self.current is None:
            raise ValueError("Error en el Parse Stm, current = None")

        if self.match(Type.SELECT):
            atributos = []

            # Match atributos
            if self.match(Type.STAR):
                atributos = "*"
            elif self.match(Type.ID):
                atributos.append(self.previous.text)
                while self.match(Type.COMA):
                    if self.match(Type.ID):
                        atributos.append(self.previous.text)
                    else:
                        raise ValueError("Se esperaba un ID después de ',' en el SELECT")
            else:
                raise ValueError("Se esperaba '*' o un identificador en el SELECT.")

            if not self.match(Type.FROM):
                raise ValueError("Se esperaba un 'FROM' despues de los ID's en el SELECT")

            if not self.match(Type.ID):
                raise ValueError("Se esperaba el nombre de la tabla.")

            table = self.previous.text
            query = None

            if self.match(Type.WHERE):
                query = self.ParseLogicExp()

            return SelectStatement(atributos, table, query)


        elif self.match(Type.INSERT):
            if not self.match(Type.INTO):
                raise ValueError("Se esperaba 'INTO' después de 'INSERT'")

            if not self.match(Type.ID):
                raise ValueError("Se esperaba el nombre de la tabla en 'INSERT'")

            table = self.previous.text

            if not self.match(Type.VALUES):
                raise ValueError("Se esperaba 'VALUES' después del nombre de la tabla")

            if not self.match(Type.LPAREN):
                raise ValueError("Se esperaba '(' después de 'VALUES'")

            values = []
            while True:
                if self.match(Type.STRING) or self.match(Type.NUMBER) or self.match(Type.TRUE) or self.match(Type.FALSE):
                    values.append(self.previous.text)

                else:
                    raise ValueError("Se esperaba un valor literal en VALUES")

                if not self.match(Type.COMA):
                    break

            if not self.match(Type.RPAREN):
                raise ValueError("Se esperaba ')' después de los valores")

            return InsertStatement(table, values)


        elif self.match(Type.DELETE):
            if not self.match(Type.FROM):
                raise ValueError("Se esperaba 'FROM' después de 'DELETE'")

            if not self.match(Type.ID):
                raise ValueError("Se esperaba el nombre de la tabla")

            table = self.previous.text
            condition = None

            if self.match(Type.WHERE):
                condition = self.ParseLogicExp()

            return DeleteStatement(table, condition)

        elif self.match(Type.CREATE):
            if not self.match(Type.TABLE):
                raise ValueError("Se esperaba 'TABLE' después de 'CREATE'")

            if not self.match(Type.ID):
                raise ValueError("Se esperaba el nombre de la tabla")
            name = self.previous.text

            if self.match(Type.FROM):

                if not self.match(Type.FILE):
                    raise ValueError("Se esperaba 'FILE' después de 'FROM'")
                if not self.match(Type.STRING):
                    raise ValueError("Se esperaba la ruta del archivo entre comillas")
                filepath = self.previous.text
                if not self.match(Type.USING):
                    raise ValueError("Se esperaba 'USING'")
                if not self.match(Type.INDEX):
                    raise ValueError("Se esperaba 'INDEX'")
                if not self.match(Type.ID):
                    raise ValueError("Se esperaba tipo de índice (ej. HASH)")
                index_type = self.previous.text
                if not self.match(Type.LPAREN):
                    raise ValueError("Se esperaba '(' después del tipo de índice")
                if not self.match(Type.STRING):
                    raise ValueError("Se esperaba el campo índice entre comillas")
                index_field = self.previous.text
                if not self.match(Type.RPAREN):
                    raise ValueError("Se esperaba ')' para cerrar índice")
                return CreateTableFromFile(name, filepath, index_type, index_field)

            if not self.match(Type.LPAREN):
                raise ValueError("Se esperaba '(' para definir columnas")

            columns = []
            while True:
                if not self.match(Type.ID):
                    raise ValueError("Se esperaba nombre de columna")
                col_name = self.previous.text

                if not self.match(Type.INT) and not self.match(Type.TEXT) and not self.match(
                        Type.DATE) and not self.match(Type.FLOAT):
                    raise ValueError("Se esperaba tipo de dato")
                col_type = self.previous.type

                columns.append((col_name, col_type))

                if not self.match(Type.COMA):
                    break

            if not self.match(Type.RPAREN):
                raise ValueError("Se esperaba ')' para cerrar definición de columnas")

            return CreateTable(name, columns)

        else:
            raise ValueError(f"Sentencia no reconocida: {self.current.text}")

    def ParseLogicExp(self):
        left = self.ParseCEXP()

        while self.match(Type.AND) or self.match(Type.OR):
            op = self.previous.type
            right = self.ParseCEXP()
            left = BinaryExp(left, op, right)  # Tratamos AND/OR como BinaryExp con tipo logico

        return left

    def ParseCEXP(self):
        if self.match(Type.NOT):
            expr = self.ParseCEXP()
            return BinaryExp(None, Type.NOT, expr)

        left = self.ParseExp()

        if self.match(Type.LESS) or self.match(Type.EQLESS) or self.match(Type.MAYOR) or self.match(Type.EQMAYOR) or self.match(Type.EQ) or self.match(Type.NEQ):
            op: BinaryOp

            if self.previous.type == Type.EQ:
                op = BinaryOp.EQUAL_OP
            elif self.previous.type == Type.LESS:
                op = BinaryOp.LESS_OP
            elif self.previous.type == Type.EQLESS:
                op = BinaryOp.EQLESS_OP
            elif self.previous.type == Type.MAYOR:
                op = BinaryOp.MAYOR_OP
            elif self.previous.type == Type.EQMAYOR:
                op = BinaryOp.EQMAYOR_OP
            else:
                op = BinaryOp.NEQ_OP

            right = self.ParseExp()
            left = BinaryExp(left, op, right)

        return left

    def ParseExp(self):
        left = self.ParseTerm()

        while self.match(Type.PLUS) or self.match(Type.MINUS):
            op: BinaryOp

            if self.previous.type == Type.PLUS:
                op = BinaryOp.PLUS_OP
            else:
                op = BinaryOp.MINUS_OP

            right = self.ParseTerm()
            left = BinaryExp(left, op, right)

        return left


    def ParseTerm(self):
        left = self.ParseFactor()

        while self.match(Type.STAR) or self.match(Type.DIV):
            op: BinaryOp

            if self.previous.type == Type.STAR:
                op = BinaryOp.MUL_OP
            else:
                op = BinaryOp.DIV_OP

            right = self.ParseFactor()
            left = BinaryExp(left, op, right)

        return left


    def ParseFactor(self):
        if self.match(Type.NUMBER):
            return NumberExp(int(self.previous.text))
        elif self.match(Type.ID):
            return IdExp(self.previous.text)
        elif self.match(Type.TRUE):
            return BoolExp(True)
        elif self.match(Type.FALSE):
            return BoolExp(False)
        elif self.match(Type.STRING):
            return StringExp(self.previous.text)
        elif self.match(Type.LPAREN):
            exp = self.ParseCEXP()
            if not self.match(Type.RPAREN):
                raise ValueError("Falto cerrar parentesis")
            return exp
        else:
            raise ValueError(f"Error en el ParseFactor: se encontro {self.current.text}")
