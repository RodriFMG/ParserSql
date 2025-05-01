# scanner.py
from token import Token, Type

def is_space_white(c: str):
    return c in [' ', '\n', '\t', '\r']


class Scanner:
    def __init__(self, code: str):
        self.code = code
        self.first = 0
        self.current = 0

    def next_token(self):
        while self.current < len(self.code) and is_space_white(self.code[self.current]):
            self.current += 1

        if self.current >= len(self.code):
            return Token(Type.EOF)

        c = self.code[self.current]
        self.first = self.current

        #palabras clave
        if c.isalpha():
            while self.current < len(self.code) and self.code[self.current].isalnum():
                self.current += 1
            word = self.code[self.first:self.current].upper()

            keyword_map = {
                "SELECT": Type.SELECT,
                "FROM": Type.FROM,
                "WHERE": Type.WHERE,
                "BETWEEN": Type.BETWEEN,
                "INSERT": Type.INSERT,
                "INTO": Type.INTO,
                "VALUES": Type.VALUES,
                "DELETE": Type.DELETE,
                "CREATE": Type.CREATE,
                "TABLE": Type.TABLE,
                "FILE": Type.FILE,
                "USING": Type.USING,
                "INDEX": Type.INDEX,
                "PRIMARY": Type.PRIMARY,
                "KEY": Type.KEY,
                "INT": Type.INT,
                "TEXT": Type.TEXT,
                "DATE": Type.DATE,
                "ARRAY": Type.ARRAY,
                "FLOAT": Type.FLOAT,
            }

            if word in keyword_map:
                return Token(keyword_map[word], word)
            else:
                return Token(Type.ID, word)

        #numeros
        elif c.isdigit():
            while self.current < len(self.code) and self.code[self.current].isdigit():
                self.current += 1
            return Token(Type.NUMBER, self.code[self.first:self.current])

        #string
        elif c == '"':
            self.current += 1
            start = self.current
            while self.current < len(self.code) and self.code[self.current] != '"':
                self.current += 1
            text = self.code[start:self.current]
            self.current += 1
            return Token(Type.STRING, text)

        #simbolos
        self.current += 1
        symbol_map = {
            '*': Type.STAR,
            '=': Type.EQ,
            ',': Type.COMA,
            ';': Type.SEMICOLON,
            '(': Type.LPAREN,
            ')': Type.RPAREN,
            '[': Type.LBRACKET,
            ']': Type.RBRACKET,
        }

        if c in symbol_map:
            return Token(symbol_map[c], c)

        return Token(Type.ERR, c)

