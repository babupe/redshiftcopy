#!/bin/bash
echo "*/5 * * * * ./configManager.py -d logstreamerconfig -q logstreamerconfig" > /tmp/rscopycrontab
echo "*/15 * * * * ./copyToRs.py -d logstreamerconfig" >> /tmp/rscopycrontab
crontab /tmp/rscopycrontab
