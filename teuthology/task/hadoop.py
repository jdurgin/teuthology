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

@contextlib.contextmanager
def validate_config(ctx, config):
    log.info('Vaidating Hadoop configuration')
    slaves = ctx.cluster.only('hadoop-slave')

    #for remote, roles_for_host in slaves.remotes.iteritems():
    if (len(slaves.remotes) < 1):
        log.info("At least one hadoop-slave must be specified")
    else:
        log.info(str(len(slaves.remotes)) + " slaves specified")

    try: 
        yield

    finally:
        pass

def set_java_home():
    os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-6-openjdk-amd64'

def edit_hadoop_env(ctx):
    hadoopEnvFile = "/tmp/cephtest/hadoop/conf/hadoop-env.sh"
    with open(hadoopEnvFile, "a") as fileToWrite:
        fileToWrite.write("export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64\n")
        fileToWrite.write("export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp/cephtest/binary/usr/local/lib:/usr/lib\n")
        fileToWrite.write("export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/tmp/cephtest/binary/usr/local/lib/libcephfs.jar\n")
        fileToWrite.close()
        push_file_to_all_hosts(ctx, hadoopEnvFile)


def push_file_to_all_hosts(ctx, fileToPush):
    with open(fileToPush, "r") as fileToWrite:
        conf_fp = StringIO(fileToWrite.read())
        fileToWrite.close() # close now, as the file will be overwritten
        #fileToWrite.write(conf_fp)
        conf_fp.seek(0)
        # now push it to each host
        writes = ctx.cluster.run(
            args=[
                'python',
                '-c',
                'import shutil, sys; shutil.copyfileobj(sys.stdin, file(sys.argv[1], "wb"))',
                fileToPush,
                ],
            stdin=run.PIPE,
            wait=False,
            )
        teuthology.feed_many_stdins_and_close(conf_fp, writes)
        run.wait(writes)


def write_core_site(ctx):
    coreSiteFile = "/tmp/cephtest/hadoop/conf/core-site.xml" 
    with open(coreSiteFile, "w") as fileToWrite:
        fileToWrite.write("<?xml version=\"1.0\"?>\n")
        fileToWrite.write("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n")
        fileToWrite.write("<!-- Put site-specific property overrides in this file. -->\n")
        fileToWrite.write("<configuration>\n")
        fileToWrite.write("\t<property>\n")
        fileToWrite.write("\t\t<name>hadoop.tmp.dir</name>\n")
        fileToWrite.write("\t\t<value>/tmp/hadoop/tmp</value>\n")
        fileToWrite.write("\t</property>\n")
        fileToWrite.write("\t<property>\n")
        fileToWrite.write("\t\t<name>fs.default.name</name>\n")
        fileToWrite.write("\t\t<value>ceph:///</value>\n")
        fileToWrite.write("\t</property>\n")
        fileToWrite.write("\t<property>\n")
        fileToWrite.write("\t\t<name>ceph.conf.file</name>\n")
        fileToWrite.write("\t\t<value>/tmp/cephtest/ceph.conf</value>\n")
        fileToWrite.write("\t</property>\n")
        #fileToWrite.write("\t<property>\n")
        #fileToWrite.write("\t\t<name>fs.ceph.monAddr</name>\n")
        #fileToWrite.write("\t\t<value>192.168.141.130:6789</value>\n")
        #fileToWrite.write("\t</property>\n")
        #fileToWrite.write("\t<property>\n")
        #fileToWrite.write("\t\t<name>fs.ceph.libDir</name>\n")
        #fileToWrite.write("\t\t<value>/tmp/cephtest/binary/usr/local/lib</value>\n")
        #fileToWrite.write("\t</property>\n")
        fileToWrite.write("</configuration>\n")
        fileToWrite.close()
        log.info("wrote file: " + coreSiteFile)

        push_file_to_all_hosts(ctx, coreSiteFile)

def write_mapred_site(ctx):
    mapredSiteFile = "/tmp/cephtest/hadoop/conf/mapred-site.xml"
    with open(mapredSiteFile, "w") as fileToWrite:
        fileToWrite.write("<?xml version=\"1.0\"?>\n")
        fileToWrite.write("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n")
        fileToWrite.write("<!-- Put site-specific property overrides in this file. -->\n")
        fileToWrite.write("<configuration>\n")
        fileToWrite.write("\t<property>\n")
        fileToWrite.write("\t\t<name>mapred.job.tracker</name>\n")

        # figure out which IP is the master
        master = ctx.cluster.only('hadoop-master')

        for remote, roles_for_host in master.remotes.iteritems():
            m = re.search('\w+@([\d\.]+)', str(remote))
            if m:
              log.info('adding host {remote} as jobtracker'.format(remote=m.group(1)))
              fileToWrite.write('\t\t<value>{remote}:54311</value>\n'.format(remote=m.group(1)))
              #fileToWrite.write('{remote}\n'.format(remote=m.group(1)))
            else:
              log.info('no IP address in {remote}'.format(remote=remote))
        fileToWrite.write("\t</property>\n")
        fileToWrite.write("</configuration>\n")
        fileToWrite.close()
        log.info("wrote file: " + mapredSiteFile)

        push_file_to_all_hosts(ctx, mapredSiteFile)

def write_hdfs_site(ctx):
    hdfsSiteFile = "/tmp/cephtest/hadoop/conf/hdfs-site.xml"
    with open(hdfsSiteFile, "w") as fileToWrite:
        fileToWrite.write("<?xml version=\"1.0\"?>\n")
        fileToWrite.write("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n")
        fileToWrite.write("<!-- Put site-specific property overrides in this file. -->\n")
        fileToWrite.write("<configuration>\n")
        fileToWrite.write("\t<property>\n")
        fileToWrite.write("\t\t<name>dfs.replication</name>\n")
        fileToWrite.write("\t\t<value>1</value>\n")
        fileToWrite.write("\t</property>\n")
        fileToWrite.write("</configuration>\n")
        fileToWrite.close()
        log.info("wrote file: " + hdfsSiteFile)

        push_file_to_all_hosts(ctx, hdfsSiteFile)

def write_slaves(ctx):
    log.info('Setting up slave nodes...')

    slavesFile = "/tmp/cephtest/hadoop/conf/slaves"
    with open(slavesFile, "w") as fileToWrite:

        slaves = ctx.cluster.only('hadoop-slave')

        for remote, roles_for_host in slaves.remotes.iteritems():
            m = re.search('\w+@([\d\.]+)', str(remote))
            if m:
              log.info('adding host {remote} to slaves'.format(remote=m.group(1)))
              fileToWrite.write('{remote}\n'.format(remote=m.group(1)))
            else:
              log.info('no IP address in {remote}'.format(remote=remote))

        fileToWrite.close()
        log.info("wrote file: " + slavesFile)

        push_file_to_all_hosts(ctx, slavesFile)

def write_master(ctx):
    mastersFile = "/tmp/cephtest/hadoop/conf/masters"
    with open(mastersFile, "w") as fileToWrite:
        master = ctx.cluster.only('hadoop-master')

        for remote, roles_for_host in master.remotes.iteritems():
            m = re.search('\w+@([\d\.]+)', str(remote))
            if m:
              log.info('adding host {remote} to master'.format(remote=m.group(1)))
              fileToWrite.write('{remote}\n'.format(remote=m.group(1)))
            else:
              log.info('no IP address in {remote}'.format(remote=remote))

        fileToWrite.close()
        log.info("wrote file: " + mastersFile)

        push_file_to_all_hosts(ctx, mastersFile)

def _configure_hadoop2(ctx, config):
    log.info('writing out config files')

    set_java_home()
    edit_hadoop_env(ctx)
    write_core_site(ctx)
    write_mapred_site(ctx)
    write_hdfs_site(ctx)
    write_slaves(ctx)
    write_master(ctx)

@contextlib.contextmanager
def configure_hadoop2(ctx, config):
    #with parallel() as p:
    #   for remote in ctx.cluster.remotes.iterkeys():
    #       p.spawn(_configure_hadoop2, ctx, remote)
    _configure_hadoop2(ctx,config)

    log.info('formatting hdfs')
    subprocess.call(["/tmp/cephtest/hadoop/bin/hadoop","namenode","-format"])

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
                    '/tmp/hadoop/tmp',
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

@contextlib.contextmanager
def load_data(ctx, config):
    log.info('sleeping to make sure the namenode is out of safemode')
    time.sleep(30)
    log.info('loading wordcount data')
    subprocess.call(["/tmp/cephtest/hadoop/bin/hadoop", "dfs", "-mkdir", "wordcount_input"])
    for myFile in glob.glob("/users/buck/hadoop_wordcount_input/*"):
        subprocess.call(["/tmp/cephtest/hadoop/bin/hadoop", "dfs", "-put",\
        myFile, "./wordcount_input" ])

    log.info('done loading data')

    try: 
        yield

    finally:
        log.info('no need to unload data.')

@contextlib.contextmanager
def start_hadoop(ctx, config):
    log.info('starting hadoop up')
    subprocess.call(["/tmp/cephtest/hadoop/bin/start-mapred.sh"])
    log.info('done starting hadoop')

    try: 
        yield

    finally:
        log.info('stopping hadoop')
        subprocess.call(["/tmp/cephtest/hadoop/bin/stop-mapred.sh"])
        log.info('done stopping hadoop')

def get_hadoop_url(branch):
    # we may need to do something fancier in the future, but for now...
    hadoop_url_base = 'https://github.com/ceph/hadoop-common.git'
    return hadoop_url_base

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

def _download_binaries(remote, url):
    remote.run(
        args=[
            'mkdir', '-p', '-m0755', '/tmp/cephtest/hadoop',
            run.Raw('&&'),
            'uname', '-m',
            run.Raw('|'),
            'sed', '-e', 's/^/ceph./; s/$/.tgz/',
            run.Raw('|'),
            'wget',
            '-nv',
            '-O-',
            '--base={url}'.format(url=url),
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
        log.info('Fetching and unpacking ceph binaries from gitbuilder...')
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

@contextlib.contextmanager
def configure_hadoop(ctx, config):
    path = config.get('path')
    tmpdir = None

    if path is None:
        # fetch from gitbuilder gitbuilder
        log.info('Fetching and unpacking ceph binaries from gitbuilder...')
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

@contextlib.contextmanager
def task(ctx, config):
    """
    Set up and tear down a Hadoop cluster.
    """
    dist = 'precise'
    format = 'jar'
    arch = 'x86_64'
    flavor = 'basic'
    branch = 'cephfs_branch-1.0'

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        "task hadoop only supports a dictionary for configuration"

    with contextutil.nested(
        lambda: validate_config(ctx=ctx, config=config),
        lambda: binaries(ctx=ctx, config=dict(
                #branch=config.get('branch'),
                branch=branch,
                tag=config.get('tag'),
                sha1=config.get('sha1'),
                path=config.get('path'),
                flavor=flavor,
                dist=config.get('dist', dist),
                format=format,
                arch=arch
                )),
        lambda: configure_hadoop2(ctx=ctx, config=config),
        lambda: start_hadoop(ctx=ctx, config=config),
        lambda: load_data(ctx=ctx, config=config),
        lambda: run_wordcount(ctx=ctx, config=config),
        ):
        yield

