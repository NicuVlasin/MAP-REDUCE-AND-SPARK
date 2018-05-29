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
    words = line.split(' ')

    # 4. We get the name of the city (first word)
    languages = words[0]

    # 3. We return the name of the city
    return languages


# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, languages, num_top_entries, output_stream):
    output = []
    for x in input_stream:
        language = process_first_line(x)
        if any([language.startswith(lan) for lan in languages]):
            output.append(x.split());

    top_5_values = sorted(output, key=lambda x: (x[0], int(x[2])), reverse=True)

    index = 1
    next_language = ""
    for each in top_5_values:
        current_lang = each[0]
        
        if current_lang != next_language:
            entries = each[1]
            visits = int(each[2])
            index += 1
            output_stream.write(current_lang + "\t(" + str(entries) +"," + str(visits) + ")\n")
        if index == 5:
            next_language = current_lang
            index = 0

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, languages, num_top_entries):
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
    my_map(my_input_stream, languages, num_top_entries, my_output_stream)


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

    i_file_name = "pageviews-20180219-100000_0.txt"
    o_file_name = "mapResult.txt"

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, languages, num_top_entries)
