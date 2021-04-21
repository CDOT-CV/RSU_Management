import argparse
import csv
import os

os.environ['SNMP_USERNAME'] = 'authOnlyUser'
os.environ['SNMP_PASSWORD'] = 'herbertherbert'

snmp_authstring = '-u {user} -a SHA -A {pw} -x AES -X {pw} -l authNoPriv'.format(user=os.getenv('SNMP_USERNAME'), pw=os.getenv('SNMP_PASSWORD'))

def ipToHex(ip, little_endian):
  hex_dest_ip = ''
  for octet in ip.split('.'):
    if len(hex(int(octet))[2:]) == 1:
      hex_dest_ip += '0'
    hex_dest_ip += hex(int(octet))[2:]
  
  if little_endian:
    return '00000000000000000000ffff' + hex_dest_ip
  return hex_dest_ip + '000000000000000000000000'

def setRsuStatus(rsu_ip, operate):
  if operate:
    os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuMode.0 i 4'.format(auth=snmp_authstring, rsuip=rsu_ip))
  else:
    os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuMode.0 i 2'.format(auth=snmp_authstring, rsuip=rsu_ip))

def config_msgfwd(rsu_ip, dest_ip, udp_port, rsu_index):
  # Put RSU in standby
  setRsuStatus(rsu_ip, operate=False)
  
  # Create a little endian hex version of destIP
  hex_dest_ip = ipToHex(dest_ip, little_endian=True)
  
  # Perform configurations
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdPsid.{index} s " "'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index))
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdDestIpAddr.{index} x {destip}'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index, destip=hex_dest_ip))
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdDestPort.{index} i {port}'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index, port=udp_port))
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdProtocol.{index} i 2'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index))
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdRssi.{index} i -100'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index))
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdMsgInterval.{index} i 1'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index))
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdDeliveryStart.{index} x 0C1F07B21100'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index))
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdDeliveryStop.{index} x 0C1F07F41100'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index))
  os.system('snmpset -v 3 {auth} {rsuip} RSU-MIB:rsuDsrcFwdEnable.{index} i 1'.format(auth=snmp_authstring, rsuip=rsu_ip, index=rsu_index))

  # Put RSU in run mode
  setRsuStatus(rsu_ip, operate=True)
  
  os.system('snmpwalk -v 3 -u {auth} {rsuip} 1.0.15628.4.1 | grep 4.1.7'.format(auth=snmp_authstring, rsuip=rsu_ip))

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument(
    "--rsu_list_file",
	type=str,
    required=True,
    help="The CSV file filepath for the list of RSU IPs which will be configured.")
  parser.add_argument(
    "--dest_ip",
    required=True,
    help="The IP the RSU will forward messages to.")
  parser.add_argument(
    "--udp_port",
    required=True,
    help="The port the RSU will forward messages to.")
  parser.add_argument(
    "--rsu_index",
    required=True,
    help="The index the SNMP configuration will be configured on.")
  args = parser.parse_args()
  
  with open(args.rsu_list_file, newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
      config_msgfwd(row[0], args.dest_ip, args.udp_port, args.rsu_index)