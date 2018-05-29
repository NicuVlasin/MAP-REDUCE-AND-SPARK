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
    #language_project = words[0]

    # 3. We return the name of the city
    return words


# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, per_language_or_project, output_stream):
    language_project_dict = {}

    for line in input_stream:
        words = process_first_line(line)
        language_project = words[0]
        visits = int(words[2])

        if (per_language_or_project == True):
            # Per language.. en.v donald_trump 35
            if '.' in language_project:
                language_project = language_project.split('.')[0]
        else:
            # Per project
            if '.' in language_project:
                language_project = language_project.split('.')[1]

        if language_project not in language_project_dict:
            language_project_dict[language_project] = visits
        else:
            language_project_dict[language_project] = language_project_dict.get(language_project) + visits

    for key,value in language_project_dict.items():
        output_stream.write(str(key) + "\t" + str(value) + "\n")

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, per_language_or_project):
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
    my_map(my_input_stream, per_language_or_project, my_output_stream)

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

    per_language_or_project = True # True for language and False for project

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, per_language_or_project)
