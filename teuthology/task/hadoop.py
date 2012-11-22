from cStringIO import StringIO

import argparse
import contextlib
import errno
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
import glob

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.parallel import parallel
from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def hello_world(ctx, config, name_):
	log.info('Hello %s' % name_)

	try: 
		yield
	finally:
		log.info('in hello.finally')

def set_java_home():
    os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-6-openjdk'

def edit_hadoop_env():
    with open("/tmp/cephtest/hadoop/conf/hadoop-env.sh", "a") as fileToWrite:
        fileToWrite.write("export JAVA_HOME=/usr/lib/jvm/java-6-openjdk")

def write_core_site():
    with open("/tmp/cephtest/hadoop/conf/core-site.xml", "w") as fileToWrite:
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
        fileToWrite.write("\t\t<value>hdfs://localhost:54310</value>\n")
        fileToWrite.write("\t</property>\n")
        fileToWrite.write("</configuration>\n")
        fileToWrite.close()
        log.info("wrote file: /tmp/cephtest/hadoop/conf/core-site.xml")

def write_mapred_site():
    with open("/tmp/cephtest/hadoop/conf/mapred-site.xml", "w") as fileToWrite:
        fileToWrite.write("<?xml version=\"1.0\"?>\n")
        fileToWrite.write("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n")
        fileToWrite.write("<!-- Put site-specific property overrides in this file. -->\n")
        fileToWrite.write("<configuration>\n")
        fileToWrite.write("\t<property>\n")
        fileToWrite.write("\t\t<name>mapred.job.tracker</name>\n")
        fileToWrite.write("\t\t<value>localhost:54311</value>\n")
        fileToWrite.write("\t</property>\n")
        fileToWrite.write("</configuration>\n")
        fileToWrite.close()
        log.info("wrote file: /tmp/cephtest/hadoop/conf/mapred-site.xml")

def write_hdfs_site():
    with open("/tmp/cephtest/hadoop/conf/hdfs-site.xml", "w") as fileToWrite:
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
        log.info("wrote file: /tmp/cephtest/hadoop/conf/hdfs-site.xml")


@contextlib.contextmanager
def configure_hadoop2(ctx, config):
    log.info('writing out config files')

    set_java_home()
    edit_hadoop_env()
    write_core_site()
    write_mapred_site()
    write_hdfs_site()

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
    for myFile in glob.glob("/tmp/wordcount_input/*"):
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
    subprocess.call(["/tmp/cephtest/hadoop/bin/start-all.sh"])
    log.info('done starting hadoop up')

    try: 
        yield

    finally:
        log.info('stopping hadooop')
        subprocess.call(["/tmp/cephtest/hadoop/bin/stop-all.sh"])
        log.info('done stopping hadoop up')

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
		lambda: hello_world(ctx=ctx, config=config, name_='joe'),
        lambda: configure_hadoop2(ctx=ctx, config=config),
        lambda: start_hadoop(ctx=ctx, config=config),
        lambda: load_data(ctx=ctx, config=config),
        lambda: run_wordcount(ctx=ctx, config=config),
        ):
	yield

