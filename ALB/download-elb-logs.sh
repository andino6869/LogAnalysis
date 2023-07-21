#!/bin/bash

Bucket="pc-energy-webappaccesslogsbucketde44a688-nvi6wsn70zdp/AWSLogs/159216450767/elasticloadbalancing/eu-central-1"
for MON in Mar; do
        mkdir -p Jan
        cd $MON
        case $MON in
                Jan) M=01;;
                Feb) M=02;;
                Mar) M=03;;
        esac
        if [ -f $M.log ]; then rm $M.log; fi
        for D in {01..29}; do
                set -x
                if [ ! -d $D ]; then
                        mkdir -p $D 2>&1 | tee -a $M.log
        fi
                #set +x
                #echo "$M-$D" 2>&1 | tee -a $M.log
                #aws s3 cp --recursive --profile pcprod s3://lb-logs-647933830095/prod1/AWSLogs/647933830095/elasticloadbalancing/eu-central-1/2022/$M/$D/ $D/
                #if [ `ls $D/ | wc -l` -eq 0 ]; then
                        echo "$M-$D" 2>&1 | tee -a $M.log
                        aws s3 cp --recursive --profile encore.prod s3://$Bucket/2023/$M/$D/ $D/ 2>&1 | tee -a $M.log
                #fi
        done
        cd -
done
