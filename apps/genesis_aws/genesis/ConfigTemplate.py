'''
Created on Jun 17, 2013

@author: masinoa
'''

from jinja2 import Environment, PackageLoader
import os
import shutil

def application(worker=None, jobmanager=None, genesis=None, notifications=None, awscreds=None, s3=None, ec2=None):
    env = Environment(loader=PackageLoader('genesis', 'templates'), trim_blocks=True, lstrip_blocks=True)
    template = env.get_template('application.conf')
    if notifications: _notifications = notifications
    else: _notifications = 'sendNotifications=false'
    if awscreds and s3: _s3 = s3
    else: _s3 = {'use':'false', 'bucketName':'none'}
    return template.render(worker=worker, jobmanager=jobmanager, genesis=genesis, notifications=_notifications,
                           awscreds=awscreds, s3=_s3, ec2=ec2)

def createAppTemp(worker=None, jobmanager=None, genesis = None, notifications=None, awscreds=None, s3=None, ec2=None, tempDir = '../temp'):
    if not os.path.exists(tempDir): os.makedirs(tempDir)
    f = open('{0}/application.conf'.format(tempDir), 'w')
    f.write(application(worker, jobmanager, genesis, notifications, awscreds, s3, ec2))
    f.close()
    
def removeAppTempDir(tempDir = '../temp'):
    if os.path.exists(tempDir): shutil.rmtree(tempDir)