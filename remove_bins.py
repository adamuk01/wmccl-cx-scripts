#!/usr/bin/python3

import string
import argparse

def strip_binary_data(file_path):
    printable_characters = set(string.printable)
    with open(file_path, 'rb') as file:
        content = file.read()

    filtered_content = bytearray(c for c in content if chr(c) in printable_characters)

    with open(file_path, 'wb') as file:
        file.write(filtered_content)

# Example usage:

parser = argparse.ArgumentParser(description="Strip binary chars out of text file.")
parser.add_argument("input_file", help="Path to the input file")
args = parser.parse_args()

file_path = args.input_file
strip_binary_data(file_path)

