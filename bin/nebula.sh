#!/bin/bash


BDIR="$(cd `dirname $0`; cd ../; pwd)"

export PYTHONPATH=$BDIR

$BDIR/bin/nebula $*