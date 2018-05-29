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


def proces_line(line):
    result = False

    words = line.split(" ")

    code = words[0].split(".")

    for lang in languages:
        if lang == code[0]:
            result = True

    return result


def sort_item(line, n):
    result = ()

    words = line.split(" ")

    first_word = words[0]
    second_word = words[1]
    third_word = int(words[2])

    for index in range(0, n):
        result = (first_word, second_word, third_word)
        return result


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, languages, num_top_entries):
    # 1. We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)

    # Complete the Spark Job

    resultRDD = sc.textFile(dataset_dir)

    sortRDD = resultRDD.filter(proces_line)

    mapRDD = sortRDD.map(lambda x: sort_item(x, num_top_entries))

    top_5_entries = mapRDD.sortBy(lambda x: (x[0]), ascending=False)

    top_5_entries.saveAsTextFile(o_file_dir)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    dataset_dir = "/FileStore/tables/A01_my_dataset/"
    o_file_dir = "/FileStore/tables/A01_my_result/"

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    my_main(dataset_dir, o_file_dir, languages, num_top_entries)