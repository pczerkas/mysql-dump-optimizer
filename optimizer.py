import os
import sys

import sqlparse


def optimize(statement):
    if statement.startswith("INSERT INTO"):
        return statement

    result = ""
    for parsed in sqlparse.parse(statement):
        result += str(parsed)

    return result


def print_usage():
    print("Usage: optimizer.py input.sql output.sql")


def main():
    if len(sys.argv) != 3:
        print_usage()
        return

    inp = sys.argv[1]
    outp = sys.argv[2]

    if not os.path.isfile(inp):
        print_usage()
        return

    with open(inp, "r") as f_input, open(outp, "w") as f_output:
        statement = ""
        for line in f_input:
            # carrier return are kept in lines
            statement += line

            if statement.endswith(";\n"):
                statement = optimize(statement)
                f_output.write(statement)
                statement = ""


main()

