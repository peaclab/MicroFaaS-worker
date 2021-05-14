#!/bin/sh

# Enable sysrq trigger
echo 1 > /proc/sys/kernel/sysrq

# Start worker, with output to syslog
cd /root
micropython worker.py | logger -s -p 3

# Failsafe Shutdown (if worker.py doesn't do this for us)
sleep 120
echo o > /proc/sysrq-trigger
