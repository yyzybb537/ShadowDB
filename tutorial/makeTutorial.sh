#!/bin/bash

set -x

# tutorial
g++ -g -O2 -std=c++11 -I../ tutorial_simple_1.cpp -o simple_1
g++ -g -O2 -std=c++11 -I../ tutorial_fork_merge_2.cpp -o fork_merge_2
g++ -g -O2 -std=c++11 -I../ tutorial_debug_3.cpp -o debug_3
g++ -g -O2 -std=c++11 -I../ tutorial_function_4.cpp -o function_4
g++ -g -O2 -std=c++11 -I../ tutorial_orderby_5.cpp -o orderby_5
g++ -g -O2 -std=c++11 -I../ tutorial_warning_callback_6.cpp -o warning_callback_6
