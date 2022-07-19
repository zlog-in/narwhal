#!/bin/bash

for i in {0 .. 9}
do
    fab faulty
    fab parsing
done
