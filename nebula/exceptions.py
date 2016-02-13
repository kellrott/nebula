

class NotImplementedException(Exception):
    def __init__(self):
        Exception.__init__(self)


class CompileException(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)
        self.msg = msg

