import sys

from pathway.runtime.process_entry_point import processing_script


def main():
    print("Setting up the processing environment.")
    processing_script(sys.argv[1:])
