#!/usr/bin/env python


import argparse
import yaml
import parsley
import re


doc_grammar = """
build = statement*:x -> GBuild(x)

statement = assign
    | declare 
    | blank -> None

assign = word:x ws ':=' ws not_end*:y ws '\n' -> GAssign(x,"".join(y))

declare = word:x ws ':' ws word:y ws (',' ws word )*:z '\n' code:c-> GDeclare(x, [y] + z, c)

code = codeline*:x -> '\\n'.join(x)

codeline = ('    '|'\t') not_end*:y '\n' -> "".join(y)

blank = ws '\n'

not_end = anything:x ?(re.search(r'\\n', x) is None) -> x
word = alphanum+:x -> "".join(x)
alphanum = anything:x ?(re.search(r'\w', x) is not None) -> x


bs = ' '+
ws = ' '*

"""

class GBuild:
    def __init__(self, statements):
        self.statements = list(a for a in statements if a is not None)

class GAssign:
    def __init__(self, variable, assignment):
        self.variable = variable
        self.assignment = assignment

    def __str__(self):
        return "%s := %s" % (self.variable, self.assignment)

class GDeclare:

    def __init__(self, name, depends, code):
        self.name = name
        self.depends = depends
        self.code = code

    def __str__(self):
        return "%s := %s (%s)" % (self.name, self.depends, self.code)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    parser.add_argument('task', nargs="?", default=None)

    args = parser.parse_args()

    with open(args.file) as handle:
        text = handle.read()

    parser = parsley.makeGrammar(doc_grammar, {
            're': re,
            'GBuild': GBuild,
            'GAssign': GAssign,
            'GDeclare' : GDeclare
    })

    b = parser(text + "\n").build()

    task = None
    if args.task is not None:
        for i in b.statements:
            if i.name == args.task:
                task = i
    else:
        for i in b.statements:
            if task is None and isinstance(i, GDeclare):
                task = i

    #for i in task.
    #print task

