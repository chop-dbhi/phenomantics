'''
Created on Jun 17, 2013

@author: masinoa
'''

import paramiko
import os
from scp import SCPClient

#returns an paramiko SSHClient connected to instance
#Assumes SSH key-pair is properly configured
def ec2SSH(instanceIP, pem_key_path, username='ec2-user'):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(instanceIP, username=username, key_filename=os.path.expanduser(pem_key_path))
    return ssh

def enable_ssh(connection, ip, sec_grp):
    #authorize ssh access from ip address
    #this will enable ssh login and scp using the instance private_dns if the instance is in the genesis group
    #ssh -v -i /path/to/key_pair ec2-user@private_dns
    #scp -i /path/to/key_pair /path/to/file ec2-user@private_dns:/destination/path
    if connection:
        #create group if it doesn't exist
        if not is_ssh_enabled(connection, ip, sec_grp):
            cidr = '{0}/32'.format(ip)
            sec_grp.authorize(ip_protocol='tcp', from_port=22, to_port=22, cidr_ip=cidr)
        
def disable_ssh(connection, ip, sec_grp):
    if connection:
        if is_ssh_enabled(connection, ip, sec_grp):
            cidr = '{0}/32'.format(ip)
            sec_grp.revoke('tcp', 22, 22, cidr_ip=cidr)

def is_ssh_enabled(connection, ip, sec_grp):
    enabled = False
    for rule in sec_grp.rules:
        b = True
        b = b and rule.__dict__['from_port']=='22'
        b = b and rule.__dict__['to_port']=='22'
        b = b and rule.__dict__['ip_protocol']=='tcp'
        g = False
        for grant in rule.__dict__['grants']:
            g = g or str(grant)=='{0}/32'.format(str(ip))
        b = b and g 
        if b: enabled = True
    return enabled 

def scp(ssh, fromPath, toPath):
    client = SCPClient(ssh.get_transport()) 
    client.put(fromPath, toPath)
    
def mkdir(ssh, dirName):
    sftp = paramiko.SFTPClient.from_transport(ssh.get_transport())
    sftp.mkdir(dirName)
        
