import os
import sys


def parse_statement(statement):
    """
    :type statement: str
    :return str generator
    """

    result = []
    depth = 0
    text = ""
    in_string = False
    quotes = None

    for char in statement:
        if char == "\n":
            continue

        if char == "(" and not in_string:
            depth += 1
            if depth == 1:
                if text:
                    yield text.strip()
                    text = ""
                yield "("
                continue

        if char == ")" and not in_string:
            depth -= 1
            if depth < 0:
                raise RuntimeError("Watch your parenthesis!")

            if depth == 0 and text:
                yield text.strip()
                text = ""
                yield ")"
                continue

        if char == " " and depth == 0 and not in_string:
            if text:
                yield text
                text = ""
            continue

        if char == "," and not in_string and depth == 1 and text:
            yield text.strip()
            text = ""
            yield ","
            continue

        if char == "'" or char == "`":
            if not in_string:
                quotes = char
                in_string = True
            elif quotes == char:
                in_string = False
                quotes = None

        text += char

    if depth != 0:
        raise RuntimeError("Watch your parenthesis!")

    if text:
        yield text


def optimize_statements(statements):
    table_name = None
    table_indexes = None

    for statement in statements:
        add_indexes = False
        skip_statement = False

        if "DISABLE KEYS" in statement:
            skip_statement = True
        if "ENABLE KEYS" in statement:
            skip_statement = True
            add_indexes = True

        if "UNLOCK TABLES" in statement or "DROP TABLE" in statement:
            add_indexes = True

        if add_indexes and table_name and table_indexes:
            index_spec_list = list(map(lambda ind: "ADD " + ind, table_indexes))
            statement = "ALTER TABLE " + table_name + "\n" + \
                        ",\n".join(index_spec_list) + \
                        ";\n"
            table_name = None
            table_indexes = None

            yield statement

        if skip_statement:
            continue

        if statement.startswith("CREATE TABLE"):
            tokens = list(parse_statement(statement))

            table_name = tokens[2]
            table_indexes = []

            if "(" in tokens:
                start = tokens.index("(")
                end = tokens.index(")")
                before_columns = " ".join(tokens[: start + 1])
                columns = []
                after_columns = " ".join(tokens[end:])

                for col_token in tokens[start + 1: end]:
                    if col_token == ",":
                        continue

                    if col_token.startswith("UNIQUE KEY") or col_token.startswith("KEY"):
                        table_indexes.append(col_token)
                    else:
                        columns.append(col_token)

                statement = before_columns + "\n" + ",\n  ".join(columns) + "\n" + after_columns + "\n"
        yield statement


def parse_statements(lines):
    statement = ""
    for line in lines:
        # carrier return are kept in lines
        statement += line

        if statement.endswith(";\n"):
            yield statement
            statement = ""


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
        statements = parse_statements(f_input)
        statements = optimize_statements(statements)

        for statement in statements:
            f_output.write(statement)


main()
