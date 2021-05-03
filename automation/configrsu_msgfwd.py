import csv
import os
import sys

snmp_authstring = '-u {user} -a SHA -A {pw} -x AES -X {pw} -l authNoPriv'.format(user=os.getenv('SNMP_USERNAME'), pw=os.getenv('SNMP_PASSWORD'))

def ip_to_hex(ip, endian):
  hex_dest_ip = ''
  for octet in ip.split('.'):
    if len(hex(int(octet))[2:]) == 1:
      hex_dest_ip += '0'
    hex_dest_ip += hex(int(octet))[2:]
  
  if endian == 0:
    return '000000000000000000000000' + hex_dest_ip
  else:
    return hex_dest_ip + '000000000000000000000000'

def set_rsu_status(rsu_ip, operate):
  if operate:
    os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuMode.0 i 4'.format(auth=snmp_authstring, rsuip=rsu_ip))
  else:
    os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuMode.0 i 2'.format(auth=snmp_authstring, rsuip=rsu_ip))

def config_msgfwd(rsu_ip, dest_ip, udp_port, rsu_index, endian):
  # Put RSU in standby
  set_rsu_status(rsu_ip, operate=False)
  
  # Create a hex version of destIP using the specified endian type
  hex_dest_ip = ip_to_hex(dest_ip, endian)
  
  print('Running SNMP config on {}'.format(rsu_ip))
  
  # Perform configurations
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdPsid.{index} s " "'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index))
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdDestIpAddr.{index} x {destip}'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index, destip=hex_dest_ip))
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdDestPort.{index} i {port}'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index, port=udp_port))
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdProtocol.{index} i 2'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index))
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdRssi.{index} i -100'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index))
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdMsgInterval.{index} i 1'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index))
  # Start datetime, hex value 0C1F07B21100 is Dec 31, 1970 17:00
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdDeliveryStart.{index} x 0C1F07B21100'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index))
  # Stop datetime, hex value 0C1F07B21100 is Dec 31, 2036 17:00
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdDeliveryStop.{index} x 0C1F07F41100'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index))
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdEnable.{index} i 1'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index))

  # Put RSU in run mode
  set_rsu_status(rsu_ip, operate=True)
  
  os.system('snmpwalk -v 3 {auth} {rsuip} 1.0.15628.4.1 | grep 4.1.7'.format(auth=snmp_authstring, rsuip=rsu_ip))

def main(rsu_csv, dest_ip, udp_port, rsu_index, endian):
  for row in rsu_csv: 
    config_msgfwd(row[0], str(dest_ip), str(udp_port), str(rsu_index), int(endian))

if __name__ == '__main__':
  with open(sys.argv[1], newline='') as csvfile:
    doc = csv.reader(csvfile, delimiter=',')
	# rsu_csv, dest_ip, udp_port, rsu_index, endian_type
    main(doc, sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    