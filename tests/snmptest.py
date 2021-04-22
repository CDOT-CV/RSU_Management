import sys
sys.path.append("../automation/")
import csv
import os
import pytest
from unittest import TestCase

import configrsu_msgfwd

def test_ip_to_hex_little_endian():
  ip = '8.8.8.8'
  hex = configrsu_msgfwd.ip_to_hex(ip, True)
  assert hex == '00000000000000000000ffff08080808'

def test_ip_to_hex_big_endian():
  ip = '8.8.8.8'
  hex = configrsu_msgfwd.ip_to_hex(ip, False)
  assert hex == '08080808000000000000000000000000'

def test_rsu_status_off():
  rsu_ip = '0.0.0.0'
  
  # Since there is no snmp server, simply running the shell command successfully should suffice
  try:
    configrsu_msgfwd.set_rsu_status(rsu_ip, False)
    assert True
  except Exception as e:
    print('Encountered issue: {}'.format(e))
    assert False

def test_rsu_status_on():
  rsu_ip = '0.0.0.0'
  
  # Since there is no snmp server, simply running the shell command successfully should suffice
  try:
    configrsu_msgfwd.set_rsu_status(rsu_ip, True)
    assert True
  except Exception as e:
    print('Encountered issue: {}'.format(e))
    assert False

def test_main():
  file = 'test_files/snmp_test.csv'
  dest_ip = '8.8.8.8'
  udp_port = 46800
  rsu_index = 20
  
  # Since there is no snmp server, simply running the shell commands successfully should suffice
  try:
    with open(file, newline='') as csvfile:
      doc = csv.reader(csvfile, delimiter=',')
      configrsu_msgfwd.main(doc, dest_ip, udp_port, rsu_index)
    assert True
  except Exception as e:
    print('Encountered issue: {}'.format(e))
    assert False