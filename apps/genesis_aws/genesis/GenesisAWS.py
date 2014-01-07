'''
Created on Jun 14, 2013

@author: masinoa
'''
import boto.ec2 as bec2
from boto.s3.connection import S3Connection
import urllib2
import time
import SSH
from genesis import ConfigTemplate
import SimpleLogger as log
import sys

#USEFUL CONSTANTS --------------------------------------
#BOTO REGION IDS
US_EAST_1 = 'us-east-1'

#BOTO INSTANCE TYPE IDS full list available at http://boto.readthedocs.org/en/latest/ref/ec2.html#module-boto.ec2.connection
T1_MICRO = 't1.micro'
M1_SMALL = 'm1.small'
M1_LARGE = 'm1.large'
CC28XLARGE = 'cc2.8xlarge'
CR18XLARGE = 'cr1.8xlarge'

#AMIs WITH GENESIS PRELOADED
AMI_LINUX_EBS_64_GENESIS_HPOPAIR_ORACLE = 'ami-18175071'

#AMI IDS FOR USE-EAST REGION, full list see http://aws.amazon.com/amazon-linux-ami/
#AWS Linux AMI Cluster Compute EBS-Backed 64 bit, requires at least M3 Extra Large m3.xlarge
AMI_LINUX_CC_EBS_64 = 'ami-a73758ce'

#AWS Linux AMI EBS-Backed 64 bit, t1 micro compatible
#INCLUDES OPEN JDK
AMI_LINUX_EBS_64 = 'ami-05355a6c'

DEFAULT_SECURITY_GROUP = 'default' #note this has no access from machines not in this security group, i.e. you cannot ssh into from your local machine
GENESIS_GROUP = 'genesis_security_group'

#********************SETTINGS********************
KEY_PAIR_NAME = 'Masino' 

#bucket name must be unique
S3_BUCKET_NAME = 'chop_cbmi_genesis_output'

#if true, EC2 instances will terminate automatically when genesis is done
TERMINATE_ON_COMPLETE = True

#control of which genesis jobs to run, k=1,2 N=100000 sf=1,2,4
#use sf=5 to run all models selected value(s) of k and N - this is the most efficient option
#k=1,2,3,4,5 N=100000 sf=5
GENESIS_ARGS = 'k=1,2,3,4,5,6,7,8,9,10 N=100000 sf=5'

#instance options 
PREP_AMI_ONLY = False
NUMBER_WORKERS = 14
WORKER_ACTORS_PER_INSTANCE = 1
NUMBER_JOB_MANAGERS = 1
INSTANCE_TYPE = M1_LARGE
AMI = AMI_LINUX_EBS_64_GENESIS_HPOPAIR_ORACLE
SECURITY_GROUP=GENESIS_GROUP

#file settings
#if true genesis jar and dirs will be created remotely, otherwise assumed to exist on AMI
#IT IS STRONGLY RECOMMENDED TO CREATE AN AMI WITH THE GENESIS JAR PRELOADED TO AVOID MULTIPLE UPLOADS
UPLOAD_GENESIS_JAR = False
UPLOAD_HPO_PAIRWISE_SIMS = False 
CREATE_REMOTE_DIRS = False
GENESIS_REMOTE_PATH = '/home/ec2-user/genesis'
LOCAL_GENESIS_JAR = "/Users/masinoa/chop/dev/production/clinseq_U01/varprior/phenomantics/apps/genesis_aws/lib/genesis.jar"
LOCAL_HPO_PAIRWISE_SIMS = "/Users/masinoa/chop/dev/production/clinseq_U01/varprior/phenomantics/apps/genesis_aws/lib/hpoPairSims.tar.gz"

#control flow
SLEEP_INTERVAL_MINUTES = 1 #minutes to sleep between instance boot checks
WORKER_JAVA_OPTS = ['Xmx4g', 'Xms3g']
JOB_MANAGER_JAVA_OPTS = ['Xmx2g', 'Xms1g']    
#END SETTINGS -------------------------------------------



def create_reservation(connection, ami_id, instance_type, key_pair_name, *security_groups):
    if connection: return connection.run_instances(ami_id, 
                                                   key_name=key_pair_name, 
                                                   instance_type=instance_type, 
                                                   security_groups=security_groups,
                                                   instance_initiated_shutdown_behavior='terminate') 
    
def terminate_instances(connection, ids):
    if connection: connection.terminate_instances(instance_ids=ids)
    
def terminate_all_running_instances(connection):
    terminate_instances(connection, running_instance_ids(connection))
    
def running_instances(connection):
    if connection:
        running = []
        for instance in instances(connection):
            if instance.state=='running': running.append(instance)
        return running
            
def instances(connection):
    if connection:
        reservations = connection.get_all_instances()
        instances = []
        for r in reservations:
            for instance in r.instances:
                instances.append(instance)
        return instances

def running_instance_ids(connection):
    if connection:
        ids = []
        for instance in running_instances(connection): ids.append(instance.id)
        return ids
    
def ip(instance): return instance.ip_address 

def private_ip(instance): return instance.private_ip_address

def public_dns(instance): return instance.public_dns_name

def private_dns(instance): return instance.private_dns_name

def connect(region):
    return bec2.connect_to_region(region) 

def create_genesis_security(connection):
    if connection:
        current_groups = connection.get_all_security_groups()
        #check if group already exists
        for group in current_groups:
            if group.name==GENESIS_GROUP: return group 
        #group wasn't found so create it
        g = connection.create_security_group(GENESIS_GROUP, 'Genesis Application Security Group')
        return g
        
def getMyIp(): return urllib2.urlopen('http://ipecho.net/plain').read()

def main():
    #take care of S3 setup
    s3 = S3Connection()
    if s3.lookup(S3_BUCKET_NAME) is None:
        s3.create_bucket(S3_BUCKET_NAME)
    s3.close()
    
    #spool up on ec2
    ec2 = connect(US_EAST_1)
    SSH.enable_ssh(ec2, getMyIp(), create_genesis_security(ec2))
    numberInstances = max(NUMBER_WORKERS, NUMBER_JOB_MANAGERS)
    
    #create the instances
    print 'Creating instances ...'
    instances = []
    for i in range(numberInstances):
        #reservation = create_reservation(ec2, AMI_LINUX_EBS_64, T1_MICRO, KEY_PAIR_NAME, GENESIS_GROUP)
        reservation = create_reservation(ec2, AMI, INSTANCE_TYPE, KEY_PAIR_NAME, SECURITY_GROUP)
        for instance in reservation.instances:
                instances.append(instance)
        log.info('Created Instance, type:{0}, vpc_id: {1} '.format(instance.instance_type, instance.vpc_id))
    
    #give AWS time to create instance
    minutes = SLEEP_INTERVAL_MINUTES # sleep time to allow instances to boot
    instancesBooted = False
    while not instancesBooted:
        print 'Sleeping for {0} minutes to allow instances to boot ...'.format(minutes)
        time.sleep(minutes * 60)
        instancesBooted = True
        for instance in instances:
            instance.update()
            instancesBooted = instancesBooted and instance.state == 'running'
    for instance in instances:
        log.info('Instance Booted, type:{0}, public_ip:{1}, private_ip:{2}, public_dns:{3}, private_dns:{4}'.
                 format(instance.instance_type, ip(instance), private_ip(instance),
                        public_dns(instance), private_dns(instance)))
    
    print 'All instances booted ...'
    
    print 'Checking ssh ready, may have to wait for instances to finish initialization ...'
    sshReady = False
    while not sshReady:
        try:
            for instance in instances:
                ssh = SSH.ec2SSH(ip(instance), '~/.ec2/{0}.pem'.format(KEY_PAIR_NAME))
                ssh.close()
            sshReady = True
        except IOError:
            sshReady = False
            print 'ssh not ready, sleeping {0} minutes'.format(minutes)
            time.sleep(minutes*60)
    print 'All instances passed ssh check ...'
        
    #assign worker and jobManager instances
    workerInstances = []
    jobManagerInstances = []
    for instance in instances:
        if len(workerInstances)<NUMBER_WORKERS: workerInstances.append(instance)
        if len(jobManagerInstances)<NUMBER_JOB_MANAGERS: jobManagerInstances.append(instance)
    genesisInstace = instances[0] #put genesis on last instance
    
    #upload files to each instance
    try:
        with open("../conf/notifications.conf") as notificationsFile: notifications = notificationsFile.read()
    except IOError:
        log.warn("../conf/notifications.conf file not present. Notifications will not be sent")
        notifications = None
    
    try:
        with open("../conf/aws.conf") as awscredsFile: awscreds = awscredsFile.read()
    except IOError:
        log.warn("../conf/aws.conf file not present. Output will not be sent S3")
        awscreds = None
        
    s3Config = {'use':'true', 'bucketName':S3_BUCKET_NAME}
    instanceIDs = [i.id for i in instances]
    print 'uploading files ...'   
    for instance in instances:
        pip = private_ip(instance)
        print 'uploading to instance {0}'.format(public_dns(instance))
        #form application conf settings
        worker = {'ip':pip, 'numWorkers':WORKER_ACTORS_PER_INSTANCE}
        genesis = {}
        genesis['hostname'] = private_ip(genesisInstace)
        genesis['jobManagers'] = []
        for jm in jobManagerInstances:
            genesis['jobManagers'].append({'ip':private_ip(jm)})
        jobmanager = {'ip':pip}
        jobmanager['workers']=[]
        for w in workerInstances:
            jobmanager['workers'].append({'ip':private_ip(w)})
        ec2Config={'terminate':TERMINATE_ON_COMPLETE, 'genesisid':genesisInstace.id, 'instanceIDs':instanceIDs}
        #create application.conf template file
        ConfigTemplate.createAppTemp(worker=worker, jobmanager=jobmanager, genesis=genesis, notifications=notifications,
                                     awscreds=awscreds, s3=s3Config, ec2=ec2Config)
        
        #open ssh client
        ssh = SSH.ec2SSH(ip(instance), '~/.ec2/{0}.pem'.format(KEY_PAIR_NAME)) 
        #mkdirs    
        if (UPLOAD_GENESIS_JAR or UPLOAD_HPO_PAIRWISE_SIMS) and CREATE_REMOTE_DIRS:
            stdin, stdout, stderr = ssh.exec_command('rm -rf {0}'.format(GENESIS_REMOTE_PATH))
            SSH.mkdir(ssh, GENESIS_REMOTE_PATH)
            for d in ["data", "logs", "conf"]: SSH.mkdir(ssh, '{0}/{1}'.format(GENESIS_REMOTE_PATH, d)) 
        
        if UPLOAD_GENESIS_JAR:
            #upload genesis jar
            SSH.scp(ssh, LOCAL_GENESIS_JAR, GENESIS_REMOTE_PATH)
            log.info('Uplaoded genesis.jar to {0}/{1}'.format(ip(instance), GENESIS_REMOTE_PATH))
        
        if UPLOAD_HPO_PAIRWISE_SIMS:
            #upload hpo pairwise sims
            SSH.scp(ssh, LOCAL_HPO_PAIRWISE_SIMS, GENESIS_REMOTE_PATH)
            log.info('Uplaoded hpo pairwise sims to {0}/{1}'.format(ip(instance), GENESIS_REMOTE_PATH))
        
        if not PREP_AMI_ONLY:
            #upload config file
            SSH.scp(ssh, '../temp/application.conf', '{0}/conf'.format(GENESIS_REMOTE_PATH))
            log.info('Uplaoded application.conf to {0}/{1}/conf'.format(ip(instance), GENESIS_REMOTE_PATH))
        
        ssh.close()
        
    if PREP_AMI_ONLY:
        log.info('Quitting application, preparing AMI only run')
        print 'Quitting application, prep only run'
        sys.exit()
    
    #temp file clean up
    #ConfigTemplate.removeAppTempDir()
    
    #start worker apps
    print 'starting worker applications ...'
    for instance in workerInstances:
        ssh = SSH.ec2SSH(ip(instance), '~/.ec2/{0}.pem'.format(KEY_PAIR_NAME))
        channel = ssh.invoke_shell()
        stdin = channel.makefile('wb')
        stdout = channel.makefile('rb')
        java_opts = reduce(lambda opts, o: '{0}-{1} '.format(opts, o), WORKER_JAVA_OPTS, '')
        stdin.write('''cd {0}
        nohup java {1} -jar genesis.jar 3 1>/dev/null 2>./logs/error.txt &
        exit
        '''.format(GENESIS_REMOTE_PATH, java_opts))
        print stdout.read()
        stdout.close()
        stdin.close()
        ssh.close()
        log.info("Started worker app on {0}".format(public_dns(instance)))
    
    print 'Sleep {0} minutes to let worker apps start'.format(str(minutes))
    time.sleep(minutes*60) 
       
    #start jobmanager apps
    print 'starting jobManager applications ...'
    for instance in jobManagerInstances:
        ssh = SSH.ec2SSH(ip(instance), '~/.ec2/{0}.pem'.format(KEY_PAIR_NAME))
        channel = ssh.invoke_shell()
        stdin = channel.makefile('wb')
        stdout = channel.makefile('rb')
        java_opts = reduce(lambda opts, o: '{0}-{1} '.format(opts, o), JOB_MANAGER_JAVA_OPTS, '')
        stdin.write('''cd {0}
        nohup java {1} -jar genesis.jar 2 1>/dev/null 2>./logs/error.txt &
        exit
        '''.format(GENESIS_REMOTE_PATH, java_opts))
        print stdout.read()
        stdout.close()
        stdin.close()
        ssh.close()
        log.info("Started jobManager app on {0}".format(public_dns(instance)))
    
    print 'Sleep {0} minutes to let jobManager apps start'.format(str(minutes))
    time.sleep(minutes*60)
    
    #start genesis app
    ssh = SSH.ec2SSH(ip(genesisInstace), '~/.ec2/{0}.pem'.format(KEY_PAIR_NAME))
    channel = ssh.invoke_shell()
    stdin = channel.makefile('wb')
    stdout = channel.makefile('rb')
    print 'starting genesis application with args: {0} ...'.format(GENESIS_ARGS)
    stdin.write('''cd {0};
    nohup java -jar genesis.jar 1 {1} 1>/dev/null 2>./logs/error.txt &
    exit
    '''.format(GENESIS_REMOTE_PATH, GENESIS_ARGS))
    print stdout.read()
    stdout.close()
    stdin.close()
    ssh.close()
    log.info("Started Genesis app on {0} with args {1}".format(public_dns(genesisInstace), GENESIS_ARGS))
    
    print 'Genesis deployment complete'

if __name__ == '__main__':
    main()