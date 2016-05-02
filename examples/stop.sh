#!/usr/bin/env bash

if [ -f "demo.pid" ]
then
    kill -9 $(cat demo.pid)
    rm demo.pid
fi