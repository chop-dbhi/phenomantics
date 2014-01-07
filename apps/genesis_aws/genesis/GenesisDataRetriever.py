'''
Created on Jul 2, 2013

@author: masinoa
'''

from boto.s3.connection import S3Connection
import GenesisAWS
import os
import tarfile

BUCKET_NAME = GenesisAWS.S3_BUCKET_NAME
KVALS = [4]
FILE_REGEX = 'AllSimFunctions_k_sid_{1}'
TARGET_DIR = '/Users/masinoa/chop/dev/production/clinseq_U01/varprior/phenomantics/etl/gp_dist_ingest/data'

def deleteAll(connection):
    bucket = connection.get_bucket(BUCKET_NAME)
    bucket_list = bucket.list()
    for l in bucket_list:
        print 'Deleting file: {0}'.format(str(l.key))
        bucket.delete_key(l.key) 

def downloadAll(connection):
    bucket = connection.get_bucket(BUCKET_NAME)
    bucket_list = bucket.list()
    for l in bucket_list:
        keyString = str(l.key)
        print 'Retrieving file: {0}'.format(keyString)
        l.get_contents_to_filename("{0}/{1}".format(TARGET_DIR,keyString))
        
def untarUnzipAll():
    cwd = os.getcwd()
    os.chdir(TARGET_DIR)
    for p in os.listdir(TARGET_DIR):
        if p.endswith('.tar.gz'):
            print 'extracting {0}'.format(p)
            f = tarfile.open('{0}/{1}'.format(TARGET_DIR, p), 'r:gz')
            try: f.extractall()
            finally: f.close()
    os.chdir(cwd)
            

def main():
    #conn = S3Connection()
    #downloadAll(conn)
    #deleteAll(conn)
    untarUnzipAll()
    print "Done."
    

if __name__ == '__main__':
    main()