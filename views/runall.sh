#!/bin/sh

a=1
b=$1
while [ "$a" -lt 361 ]    # this is loop1
do
   sbatch --exclusive runjob1.slurm $a $b
   a=`expr $a + 1`
   sleep .25
done