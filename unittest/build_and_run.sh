#!/bin/bash

set -x

#g++ -g -O2 -std=c++11 TestShadowDB.cpp -I../ -lgtest_main -lgtest -lpthread -o TestShadowDB 2>/tmp/log && ./TestShadowDB || vim /tmp/log
g++ -g -O2 -std=c++11 TestShadowDB.cpp -I../ -lgtest_main -lgtest -lpthread -o TestShadowDB && ./TestShadowDB
