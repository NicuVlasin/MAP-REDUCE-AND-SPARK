#!/usr/bin/python

# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import sys
import codecs


def process_first_line(line):
    # 1. We get rig of the end line character
    line = line.replace('\n', '')

    # 2. We strip any white character at the end
    line = line.rstrip()

    # 3. We split the info by tabulator
    words = line.split('\t')

    # 4. We get the name of the city (first word)
    languages = words[0]

    # 3. We return the name of the city
    return languages


# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(input_stream, num_top_entries, output_stream):
    result = {}

    for line in input_stream.readlines():
        content = line.split("\t")
        # print(content)

        if content[0] not in result:
            result[content[0]] = [("", 0), ("", 0), ("", 0), ("", 0), ("", 0)]

        # Separate the language from the number of entries
        no_entries = content[1].split(',')
        length = len(no_entries)
        # print(no_entries)
        # print(length)

        if length > 2:
            no_entries[1] = no_entries[length - 1]
            no_entries[0] = str(no_entries[:length - 1])
            # print(no_entries[1])
            # print(no_entries[0])
        # Take out the brackets
        no_entries[0] = no_entries[0].replace("(", "")
        no_entries[1] = no_entries[1].replace(")", "")

        # print(no_entries[0])
        # print(no_entries[1])

        # print(result[content[0]][4][1])

        if result[content[0]][4][1] < int(no_entries[1]):
                result[content[0]][4] = (no_entries[0], int(no_entries[1]))
                result[content[0]] = sorted(result[content[0]], key=lambda x: x[1], reverse=True)
        # print(result[content[0]][4])
        # print(result[content[0]])
        # print(result)

    for x in result:
        for i in range(0, num_top_entries):
            if len(result[x]) > i and result[x][i][1] > 0:
                output_stream.write(x + "\t(" + result[x][i][0] + ", " + str(result[x][i][1]) + ")\n")

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, num_top_entries):
    # We pick the working mode:

    # Mode 1: Debug --> We pick a file to read test the program on it
    if debug == True:
        my_input_stream = codecs.open(i_file_name, "r", encoding='utf-8')
        my_output_stream = codecs.open(o_file_name, "w", encoding='utf-8')
    # Mode 2: Actual MapReduce --> We pick std.stdin and std.stdout
    else:
        my_input_stream = sys.stdin
        my_output_stream = sys.stdout

    # We launch the Map program
    my_reduce(my_input_stream, num_top_entries, my_output_stream)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Input parameters
    debug = True

    i_file_name = "sort_simulation.txt"
    o_file_name = "reduce_simulation.txt"

    num_top_entries = 5

    my_main(debug, i_file_name, o_file_name, num_top_entries)
