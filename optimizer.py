import os
import sys
import sqlparse


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
            tokens = []
            for parsed in sqlparse.parse(statement):
                flatten = parsed.flatten()
                tokens.extend(map(str, flatten))

            tokens = list(filter(lambda p: p not in ["  ", " ", "", "\n"], tokens))
            table_name = tokens[2]

            col_def_begin = tokens.index("(")
            col_def_end = - tokens[::-1].index(")") - 1

            columns = []
            indexes = []

            col_def_tokens = tokens[col_def_begin + 1: col_def_end]
            while True:
                parts = None
                has_more = True
                if "," in col_def_tokens:
                    comma_pos = col_def_tokens.index(",")
                    parts = col_def_tokens[: comma_pos]
                    col_def_tokens = col_def_tokens[comma_pos + 1:]
                else:
                    parts = col_def_tokens
                    has_more = False

                parts_str = " ".join(parts)

                if parts_str.startswith("UNIQUE KEY ") or parts_str.startswith("KEY "):
                    indexes.append(parts_str)
                else:
                    columns.append(parts_str)

                if not has_more:
                    break

            before_columns = " ".join(tokens[:col_def_begin])
            after_columns = " ".join(tokens[col_def_end + 1:])
            columns = "  " + ",\n  ".join(columns)

            statement = "\n".join([before_columns, "(", columns, ")", after_columns, ""])
            table_indexes = indexes
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
