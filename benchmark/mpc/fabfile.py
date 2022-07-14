from fabric import Connection, ThreadingGroup
from fabric import task
import subprocess


@task
def benchmarking(ctx):
    hosts = ThreadingGroup('mpc-0','mpc-1','mpc-2','mpc-3','mpc-4','mpc-5','mpc-6','mpc-7','mpc-8','mpc-9')
    hosts.put('/home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/config.json', remote  = '/home/zhan/narwhal/')
    hosts.run('docker stop hotstuff')
    hosts.run('docker start narwhal')
    hosts.run('docker cp narwhal/config.json narwhal:/home/narwhal/benchmark/')
    hosts.run('docker exec -t narwhal bash ben.sh')

@task
def container(ctx):
    hosts = ThreadingGroup('mpc-0','mpc-1','mpc-2','mpc-3','mpc-4','mpc-5','mpc-6','mpc-7','mpc-8','mpc-9')
    hosts.run('rm -rf narwhal/logs/')
    hosts.run('mkdir -p narwhal/logs')
    hosts.run('docker stop hotstuff')
    hosts.put('/home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/mpc/ben.sh', remote='/home/zhan/narwhal')
    hosts.put('/home/z/Sync/Study/DSN/Marc/Code/narwahl/benchmark/mpc/update.sh', remote='/home/zhan/narwhal')

    hosts.run('docker rm -f narwhal')
    hosts.run('docker run -itd --name narwhal -p 9000-9049:9000-9049 --mount type=bind,source=/home/zhan/narwhal/logs,destination=/home/narwhal/benchmark/logs image_narwhal')
    hosts.run('docker cp index.txt narwhal:/home/narwhal/benchmark/')
    hosts.run('docker cp narwhal/ben.sh narwhal:/home/narwhal/benchmark/')
    hosts.run('docker cp narwhal/update.sh narwhal:/home/narwhal/benchmark/')
    hosts.run('docker exec -t narwhal bash update.sh')



@task
def parsing(ctx):
    subprocess.call(['bash', '../parsing.sh'])

@task
def build(ctx):
    hosts = ThreadingGroup('mpc-0','mpc-1','mpc-2','mpc-3','mpc-4','mpc-5','mpc-6','mpc-7','mpc-8','mpc-9')
    hosts.run('rm -rf narwhal/')
    hosts.run('mkdir  narwhal/')
    #hosts.run('docker stop hotstuff')
    hosts.put('/home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/mpc/Dockerfile', remote='/home/zhan/narwhal')
    hosts.run('docker rm -f narwhal')
    hosts.run('docker rmi image_narwhal')
    hosts.run('docker build -f /home/zhan/narwhal/Dockerfile -t image_narwhal .')
