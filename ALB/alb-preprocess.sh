#!/bin/bash

Y=2021
for M in {01..12}; do 
    for D in {01..31}; do 
        if [ -d /BigData/ELB/$Y/$M/$D ]; then 
            if [ ! -f /BigData/ELB/output/$Y-$M-$D-output.csv.gz ]; then
                #echo "To Do"
                #echo "./preprocess10.py $Y $M $D"
                ./alb-preprocess.py $Y $M $D
            fi
        fi
    done
done
