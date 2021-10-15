#!/bin/sh

# Enable sysrq trigger
echo 1 > /proc/sys/kernel/sysrq

# Start worker, with output to syslog
echo "in start worker and about to run worker.py"
cd /root/MicroFaaS-worker
/sbin/micropython /root/MicroFaaS-worker/worker.py > /dev/ttyS0
# | logger -s -p 3

# Failsafe Reboot (if worker.py doesn't do this for us)
sleep 180
echo b > /proc/sysrq-trigger
