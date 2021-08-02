#!/bin/sh

# Enable sysrq trigger
echo 1 > /proc/sys/kernel/sysrq

# Start worker, with output to syslog
cd /root/MicroFaaS
micropython worker.py | logger -s -p 3 &

# Failsafe Reboot (if worker.py doesn't do this for us)
sleep 180
echo b > /proc/sysrq-trigger
