from Token import Token, Type


def is_space_white(c: str):
    return c == '\n' or c == '\t' or c == ' ' or c == "\r"


class Scanner:

    def __init__(self, code: str):
        self.code = code
        self.first = 0
        self.current = 0

    def next_token(self):

        while self.current < len(self.code) and is_space_white(self.code[self.current]):
            self.current += 1

        if self.current >= len(self.code):
            return Token(Type.END)

        c = self.code[self.current]
        self.first = self.current

        if c.isalpha():
            self.current += 1
            while self.current < len(self.code) and self.code[self.current].isalnum():
                self.current += 1

            word = self.code[self.first:self.current]

            if word == "SELECT":
                return Token(Type.SELECT)
            elif word == "FROM":
                return Token(Type.FROM)
            elif word == "WHERE":
                return Token(Type.WHERE)
            else:
                return Token(Type.ID, word)

        elif not is_space_white(c):
            return Token(Type.END)

        return Token(Type.ERR)
