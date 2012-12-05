from cStringIO import StringIO

import argparse
import contextlib
import errno
import glob
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.parallel import parallel
from ..orchestra import run

log = logging.getLogger(__name__)

###################
# This installeds and configures Hadoop, but requires that Ceph is already installed and running.
##################

## Check that there is exactly one master and at least one slave configured
@contextlib.contextmanager
def validate_config(ctx, config):
    log.info('Vaidating Hadoop configuration')
    slaves = ctx.cluster.only(teuthology.is_type('hadoop.slave'))

    if (len(slaves.remotes) < 1):
        raise Exception("At least one hadoop.slave must be specified")
    else:
        log.info(str(len(slaves.remotes)) + " slaves specified")

    masters = ctx.cluster.only(teuthology.is_type('hadoop.master'))
    if (len(masters.remotes) == 1):
        pass
    else:
        raise Exception("Exactly one hadoop.master must be specified. Currently there are " + str(len(masters.remotes)))

    try: 
        yield

    finally:
        pass

## Add required entries to conf/hadoop-env.sh
def write_hadoop_env(ctx, config):
    hadoopEnvFile = "/tmp/cephtest/hadoop/conf/hadoop-env.sh"

    tmpFile = StringIO()
    tmpFile.write('''
export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp/cephtest/binary/usr/local/lib:/usr/lib
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/tmp/cephtest/binary/usr/local/lib/libcephfs.jar:/tmp/cephtest/hadoop/build/hadoop-core*.jar
export HADOOP_NAMENODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_NAMENODE_OPTS"
export HADOOP_SECONDARYNAMENODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_SECONDARYNAMENODE_OPTS"
export HADOOP_DATANODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_DATANODE_OPTS"
export HADOOP_BALANCER_OPTS="-Dcom.sun.management.jmxremote $HADOOP_BALANCER_OPTS"
export HADOOP_JOBTRACKER_OPTS="-Dcom.sun.management.jmxremote $HADOOP_JOBTRACKER_OPTS"
    ''')
    tmpFile.seek(0)

    hadoopNodes = ctx.cluster.only(teuthology.is_type('hadoop'))
    for remote, roles_for_host in hadoopNodes.remotes.iteritems():
        teuthology.write_file(remote=remote, path=hadoopEnvFile, data=tmpFile)
        tmpFile.seek(0)
        log.info("wrote file: " + hadoopEnvFile + " to host: " + str(remote))


    #with open(hadoopEnvFile, "a") as fileToWrite:
        #fileToWrite.write("export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64\n")
        #fileToWrite.write("export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp/cephtest/binary/usr/local/lib:/usr/lib\n")
        #fileToWrite.write("export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/tmp/cephtest/binary/usr/local/lib/libcephfs.jar:/tmp/cephtest/hadoop/build/hadoop-core*.jar\n")

    #push_file_to_all_hosts(ctx, hadoopEnvFile)

## Copies a file to all hosts in the config
## TODO: only push to hosts that serve at least one hadoop role
#def push_file_to_all_hosts(ctx, fileToPush):
#
#    with open(fileToPush, "r") as fileToWrite:
#        conf_fp = StringIO(fileToWrite.read())
#
#    conf_fp.seek(0)
#
#    # now push it to each host
#    writes = ctx.cluster.run(
#        args=[
#            'python',
#            '-c',
#            'import shutil, sys; shutil.copyfileobj(sys.stdin, file(sys.argv[1], "wb"))',
#            fileToPush,
#            ],
#        stdin=run.PIPE,
#        wait=False,
#        )
#    teuthology.feed_many_stdins_and_close(conf_fp, writes)
#    run.wait(writes)


## Add required entries to conf/core-site.xml
def write_core_site(ctx, config):
    coreSiteFile = "/tmp/cephtest/hadoop/conf/core-site.xml" 
    tmpFile = StringIO()
    tmpFile.write(
'''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!-- Put site-specific property overrides in this file.  -->
<configuration>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/tmp/hadoop/tmp</value>
    </property>"
    <property>
        <name>fs.default.name</name>
    ''')

    ## sort out if Hadoop should run on HDFS or ceph
    if config.get('hdfs'):
        tmpFile.write('\t\t<value>hdfs://{master_ip}:54310/</value>'.format(master_ip=get_hadoop_master_ip(ctx)))
    else:
        tmpFile.write("\t\t<value>ceph:///</value>")

    tmpFile.write('''
    </property>
    <property>
        <name>ceph.conf.file</name>
        <value>/tmp/cephtest/ceph.conf</value>
    </property>
</configuration>
    ''')
    tmpFile.seek(0)

    hadoopNodes = ctx.cluster.only(teuthology.is_type('hadoop'))
    for remote, roles_for_host in hadoopNodes.remotes.iteritems():
        teuthology.write_file(remote=remote, path=coreSiteFile, data=tmpFile)
        tmpFile.seek(0)
        log.info("wrote file: " + coreSiteFile + " to host: " + str(remote))

## finds the hadoop.master in the ctx and then pulls out just the IP address
def get_hadoop_master_ip(ctx):
    # figure out which IP is the master
    master = ctx.cluster.only(teuthology.is_type('hadoop.master'))

    assert 1 == len(master.remotes.items()), 'There must be exactly 1 hadoop.master configured'
    for remote, roles_for_host in master.remotes.iteritems():
        m = re.search('\w+@([\d\.]+)', str(remote))
        return m.group(1)


## Add required entries to conf/mapred-site.xml
def write_mapred_site(ctx):
    mapredSiteFile = "/tmp/cephtest/hadoop/conf/mapred-site.xml"
    tmpFile = StringIO()
    tmpFile.write(
'''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!-- Put site-specific property overrides in this file. -->
<configuration>
    <property>
        <name>mapred.job.tracker</name>
    ''')

    master_ip = get_hadoop_master_ip(ctx)

    log.info('adding host {remote} as jobtracker'.format(remote=master_ip))
    tmpFile.write('\t\t<value>{remote}:54311</value>'.format(remote=master_ip))

    tmpFile.write('''
    </property>
</configuration> 
    ''')

    tmpFile.seek(0)

    hadoopNodes = ctx.cluster.only(teuthology.is_type('hadoop'))
    for remote, roles_for_host in hadoopNodes.remotes.iteritems():
        teuthology.write_file(remote=remote, path=mapredSiteFile, data=tmpFile)
        tmpFile.seek(0)
        log.info("wrote file: " + mapredSiteFile + " to host: " + str(remote))

## Add required entries to conf/hdfs-site.xml
def write_hdfs_site(ctx):
    hdfsSiteFile = "/tmp/cephtest/hadoop/conf/hdfs-site.xml"
    tmpFile = StringIO()
    tmpFile.write(
'''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!-- Put site-specific property overrides in this file. -->
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
    ''')

    tmpFile.seek(0)

    hadoopNodes = ctx.cluster.only(teuthology.is_type('hadoop'))
    for remote, roles_for_host in hadoopNodes.remotes.iteritems():
        teuthology.write_file(remote=remote, path=hdfsSiteFile, data=tmpFile)
        tmpFile.seek(0)
        log.info("wrote file: " + hdfsSiteFile + " to host: " + str(remote))

## Add required entries to conf/slaves 
## These nodes host TaskTrackers and DataNodes
def write_slaves(ctx):
    log.info('Setting up slave nodes...')

    slavesFile = "/tmp/cephtest/hadoop/conf/slaves"
    tmpFile = StringIO()

    slaves = ctx.cluster.only(teuthology.is_type('hadoop.slave'))
    for remote, roles_for_host in slaves.remotes.iteritems():
        m = re.search('\w+@([\d\.]+)', str(remote))
        if m:
            log.info('adding host {remote} to slaves'.format(remote=m.group(1)))
            tmpFile.write('{remote}\n'.format(remote=m.group(1)))
        else:
            raise Exception('no IP or hostname address in {remote}'.format(remote=remote))

        #push_file_to_all_hosts(ctx, slavesFile)

    tmpFile.seek(0)

    hadoopNodes = ctx.cluster.only(teuthology.is_type('hadoop'))
    for remote, roles_for_host in hadoopNodes.remotes.iteritems():
        teuthology.write_file(remote=remote, path=slavesFile, data=tmpFile)
        tmpFile.seek(0)
        log.info("wrote file: " + slavesFile + " to host: " + str(remote))

## Add required entries to conf/masters 
## These nodes host JobTrackers and Namenodes
def write_master(ctx):
    mastersFile = "/tmp/cephtest/hadoop/conf/masters"
    tmpFile = StringIO()
    master = ctx.cluster.only(teuthology.is_type('hadoop.master'))

    for remote, roles_for_host in master.remotes.iteritems():
        m = re.search('\w+@([\d\.]+)', str(remote))
        if m:
            log.info('adding host {remote} to master'.format(remote=m.group(1)))
            tmpFile.write('{remote}\n'.format(remote=m.group(1)))
        else:
            raise Exception('no IP address in {remote}'.format(remote=remote))

    tmpFile.seek(0)

    hadoopNodes = ctx.cluster.only(teuthology.is_type('hadoop'))
    for remote, roles_for_host in hadoopNodes.remotes.iteritems():
        teuthology.write_file(remote=remote, path=mastersFile, data=tmpFile)
        tmpFile.seek(0)
        log.info("wrote file: " + mastersFile + " to host: " + str(remote))


## Call the various functions that configure Hadoop
def _configure_hadoop(ctx, config):
    log.info('writing out config files')

    write_hadoop_env(ctx, config)
    write_core_site(ctx, config)
    write_mapred_site(ctx)
    write_hdfs_site(ctx)
    write_slaves(ctx)
    write_master(ctx)



@contextlib.contextmanager
def configure_hadoop(ctx, config):
    _configure_hadoop(ctx,config)

    log.info('config.get(hdfs): {hdfs}'.format(hdfs=config.get('hdfs')))

    if config.get('hdfs'):
        log.info('hdfs option specified. Setting up hdfs')

        # let's run this from the master
        master = ctx.cluster.only(teuthology.is_type('hadoop.master'))

        assert 1 == len(master.remotes.items()), 'There must be exactly 1 hadoop.master configured'
        for remote, roles_for_host in master.remotes.iteritems():
            #run.wait(
            remote.run(
                args=["/tmp/cephtest/hadoop/bin/hadoop","namenode","-format"],
                wait=True,
                    #checkStatus=False,
            #    )
            )

    log.info('done setting up hadoop')

    try: 
        yield

    finally:
        log.info('Removing hdfs directory')
        run.wait(
            ctx.cluster.run(
                args=[
                    'rm',
                    '-rf',
                    '/tmp/hadoop',
                    ],
                wait=False,
                ),
            )

@contextlib.contextmanager
def run_wordcount(ctx, config):
    log.info('running wordcount')
    for myJar in glob.glob("/tmp/cephtest/hadoop/build/hadoop-examples*.jar"):
        subprocess.call(["/tmp/cephtest/hadoop/bin/hadoop", "jar",\
        myJar, "wordcount", "wordcount_input", "wordcount_output"])

    log.info('done running wordcount')

    try: 
        yield

    finally:
        log.info('no need to unload data.')

def _start_hadoop(remote, config):
    if config.get('hdfs'):
        remote.run(
            args=['/tmp/cephtest/hadoop/bin/start-dfs.sh', ],
            wait=True,
        )
        log.info('done starting hdfs')

    remote.run(
        args=['/tmp/cephtest/hadoop/bin/start-mapred.sh', ], 
        wait=True,
    )
    log.info('done starting mapred')


def _stop_hadoop(remote, config):
    remote.run(
        args=['/tmp/cephtest/hadoop/bin/stop-mapred.sh', ],
        wait=True,
    )

    if config.get('hdfs'):
        remote.run(
            args=['/tmp/cephtest/hadoop/bin/stop-dfs.sh', ],
            wait=True,
        )

    log.info('done stopping hadoop')

@contextlib.contextmanager
def start_hadoop(ctx, config):
    # Find the master server and run start / stop commands there
    master = ctx.cluster.only(teuthology.is_type('hadoop.master'))

    for remote, roles_for_host in master.remotes.iteritems():
        #parse out the IP address from remote
        m = re.search('\w+@([\d\.]+)', str(remote))
        if m:
            log.info('Starting hadoop on {remote}'.format(remote=m.group(1)))
            _start_hadoop(remote, config)
        else:
            raise Exception('no IP address in {remote}, not a valid hadoop.master'.format(remote=remote))

    try: 
        yield

    finally:
        log.info('Running stop-mapred.sh on {remote}'.format(remote=m.group(1)))
        _stop_hadoop(remote, config)



# download and untar the most recent hadoop binaries into /tmp/cephtest/hadoop
def _download_hadoop_binaries(remote, hadoop_url):
    log.info('_download_hadoop_binaries: path %s' % hadoop_url)
    fileName = 'hadoop.tgz'
    remote.run(
        args=[
            'mkdir', '-p', '-m0755', '/tmp/cephtest/hadoop',
            run.Raw('&&'),
            'echo',
            '{fileName}'.format(fileName=fileName),
            run.Raw('|'),
            'wget',
            '-nv',
            '-O-',
            '--base={url}'.format(url=hadoop_url),
            # need to use --input-file to make wget respect --base
            '--input-file=-',
            run.Raw('|'),
            'tar', '-xzf', '-', '-C', '/tmp/cephtest/hadoop',
            ],
        )

@contextlib.contextmanager
def binaries(ctx, config):
    path = config.get('path')
    tmpdir = None

    if path is None:
        # fetch from gitbuilder gitbuilder
        log.info('Fetching and unpacking hadoop binaries from gitbuilder...')
        sha1, hadoop_bindir_url = teuthology.get_ceph_binary_url(
            package='hadoop',
            branch=config.get('branch'),
            tag=config.get('tag'),
            sha1=config.get('sha1'),
            flavor=config.get('flavor'),
            format=config.get('format'),
            dist=config.get('dist'),
            arch=config.get('arch'),
            )
        log.info('hadoop_bindir_url %s' % (hadoop_bindir_url))
        ctx.summary['ceph-sha1'] = sha1
        if ctx.archive is not None:
            with file(os.path.join(ctx.archive, 'ceph-sha1'), 'w') as f:
                f.write(sha1 + '\n')

    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            p.spawn(_download_hadoop_binaries, remote, hadoop_bindir_url)

    try:
        yield
    finally:
        log.info('Removing hadoop binaries...')
        run.wait(
            ctx.cluster.run(
                args=[
                    'rm',
                    '-rf',
                    '--',
                    '/tmp/cephtest/hadoop',
                    ],
                wait=False,
                ),
            )

## A Hadoop NameNode will stay in safe mode for 30 seconds by default.
## This method blocks until the NameNode is out of safe mode.
@contextlib.contextmanager
def out_of_safemode(ctx, config):

    if config.get('hdfs'):
        log.info('Waiting for the Namenode to exit safe mode...')

        master = ctx.cluster.only(teuthology.is_type('hadoop.master'))
        for remote, roles_for_host in master.remotes.iteritems():
            remote.run(
                args=["/tmp/cephtest/hadoop/bin/hadoop","dfsadmin","-safemode", "wait"],
                wait=True,
                #checkStatus=False,
            )

    else:
        pass

    try:
        yield
    finally:
        pass

@contextlib.contextmanager
def task(ctx, config):
    """
    Set up and tear down a Hadoop cluster.

    This depends on either having ceph installed prior to hadoop, like so:

    roles:
    - [mon.0, mds.0, osd.0, hadoop.master.0]
    - [mon.1, osd.1, hadoop.slave.0]
    - [mon.2, hadoop.slave.1]

    tasks:
    - ceph:
    - hadoop:

    Or if you want to use HDFS under Hadoop, this will configure Hadoop
    for HDFS and start it along with MapReduce. Note that it does not 
    require Ceph be installed.

    roles:
    - [hadoop.master.0]
    - [hadoop.slave.0]
    - [hadoop.slave.1]

    tasks:
    - hadoop:
        hdfs: True

    This task requires exactly one hadoop.master be specified 
    and at least one hadoop.slave.

    This does *not* do anything with the Hadoop setup. To run wordcount, 
    you could use pexec like so:

    - pexec: 
        hadoop.slave.0:
          - mkdir -p /tmp/hadoop_input
          - wget http://ceph.com/qa/hadoop_input_files.tar -O /tmp/hadoop_input/files.tar
          - cd /tmp/hadoop_input/; tar -xf /tmp/hadoop_input/files.tar
          - /tmp/cephtest/hadoop/bin/hadoop fs -mkdir wordcount_input 
          - /tmp/cephtest/hadoop/bin/hadoop fs -put /tmp/hadoop_input/*txt wordcount_input/ 
          - /tmp/cephtest/hadoop/bin/hadoop jar /tmp/cephtest/hadoop/build/hadoop-example*jar wordcount wordcount_input wordcount_output  
          - rm -rf /tmp/hadoop_input

    """
    dist = 'precise'
    format = 'jar'
    arch = 'x86_64'
    flavor = 'basic'
    branch = 'cephfs_branch-1.0' # hadoop branch to acquire

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        "task hadoop only supports a dictionary for configuration"

    with contextutil.nested(
        lambda: validate_config(ctx=ctx, config=config),
        lambda: binaries(ctx=ctx, config=dict(
                branch=branch,
                tag=config.get('tag'),
                sha1=config.get('sha1'),
                path=config.get('path'),
                flavor=flavor,
                dist=config.get('dist', dist),
                format=format,
                arch=arch
                )),
        lambda: configure_hadoop(ctx=ctx, config=config),
        lambda: start_hadoop(ctx=ctx, config=config),
        lambda: out_of_safemode(ctx=ctx, config=config),
        ):
        yield

