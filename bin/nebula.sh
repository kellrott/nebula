#!/bin/bash


BDIR="$(cd `dirname $0`; cd ../; pwd)"

export 

export PYTHONPATH=$BDIR
if [ -e $BDIR/venv ]; then 
    . $BDIR/venv/bin/activate
fi
    
$BDIR/bin/nebula $*