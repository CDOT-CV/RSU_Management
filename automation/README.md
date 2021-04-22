# SNMP RSU Configuration Automation for Message Forwarding
This directory contains a script that is used for configuring a large number of CDOT Kapsch RSUs autonomously by providing it a list of RSU IPs and some other arguments.

## What the script does
The current script can be run manually from a command line shell. It has been written in a way that can easily be transformed to be invoked in other methods, such as a REST API, but these are not supported as is.

The script can currently perform two main tasks:
- Configure an RSU's operate mode. "Standby" and "Operate" are currently the only two supported states.
- Create and overwrite the configuration of an index on the forward message table of an RSU using SNMP.
  - The script currently is only supporting UDP message forwarding

## Running the script
To run the `configrsu_msgfwd.py` script you must provide it some information so that it can do its job.
- A text file containing a list of RSU IP addresses this can look like the following:
```
10.0.0.1
10.0.0.2
10.0.0.3
10.0.0.4
```
- The destination IP. Example: `10.0.1.5`
- The UDP port the RSU should forward the packets to at the destination IP. Example: `46800`
- The RSU index the SNMP configuration should be written to on the RSU. Example: `20`

These will be provided to the script in the form of arguments.

Check out the example shell command to run the script in a terminal:
```
python3 configrsu_msgfwd.py /home/user/RSU_Management/tests/test_files/snmp_test.csv 10.0.1.5 46800 20
```
