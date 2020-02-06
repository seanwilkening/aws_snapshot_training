import boto3
import botocore
import click

session = boto3.Session(profile_name='shotty')
ec2 = session.resource('ec2')

def filter_instances(project):
    instances = []

    if project:
        filters = [{'Name':'tag:Project', 'Values':[project]}]
        instances = ec2.instances.filter(Filters=filters)
    else:
        instances = ec2.instances.all()

    return instances

def has_pending_snapshot(volume):
    snapshots = list(volume.snapshots.all())
    return snapshots and snapshots[0].state == 'pending'

@click.group()
def cli():
    """Shotty manages snapshots"""

@cli.group()
def instances():
    """Commands for instances"""

@instances.command('list')
@click.option('--project', default=None,
    help='Only instances for project (tag Project:<name>)')
def list_instances(project):
    "List EC2 instances"

    instances = filter_instances(project)

    for i in instances:
        tags = {t['Key']: t['Value'] for t in i.tags or []}
        print(', '.join((
            i.id,
            i.instance_type,
            i.placement['AvailabilityZone'],
            i.state['Name'],
            i.public_dns_name,
            tags.get('Project','<no project>')
            )))
    return

@instances.command('stop')
@click.option('--project', default=None,
    help='Only instances for project (tag Project:<name>)')
def stop_instances(project):
    "Stop EC2 instances"

    instances = filter_instances(project)

    for i in instances:
        print("Stoping {0}".format(i.id))
        i.stop()

    return

@instances.command('start')
@click.option('--project', default=None,
    help='Only instances for project (tag Project:<name>)')
def start_instances(project):
    "Start EC2 instances"

    instances = filter_instances(project)

    for i in instances:
        print("Starting {0}".format(i.id))
        i.start()

    return

@instances.command('snapshot', help="Create snapshots of all volumes")
@click.option('--project', default=None,
    help='Only instances for project (tag Project:<name>)')
def create_snapshot(project):
    "Create EC2 Snapshots"

    instances = filter_instances(project)

    for i in instances:
        print("Stopping {0}".format(i.id))
        i.stop()
        i.wait_until_stopped()
        print("{0} is stopped".format(i.id))

        for v in i.volumes.all():
            if has_pending_snapshot(v):
                print("Skipping {0} due to pending snapshot".format(v.id))
                continue
            print("Creating snapshot of {0}".format(v.id))
            v.create_snapshot(Description="Created by Snapshot")

        print("Starting {0}".format(i.id))
        i.start()
        i.wait_until_running()
        print("{0} is started".format(i.id))

    return

@cli.group()
def volumes():
    """Commands for instance volumes"""

@volumes.command('list')
@click.option('--project', default=None,
    help='Only volumes for project (tag Project:<name>)')
def list_volumes(project):
    "List EC2 Volumes"

    instances = filter_instances(project)

    for i in instances:
        for v in i.volumes.all():
            print(", ".join((
                v.id,
                i.id,
                v.state,
                str(v.size) + "GiB",
                v.encrypted and "Encrypted" or "Not Encrypted"
            )))
    return

@cli.group()
def snapshots():
    """Commands for volume snapshots"""

@snapshots.command('list')
@click.option('--project', default=None,
    help='Only snapshots for project (tag Project:<name>)')
@click.option('--all', 'list_all', default=False, is_flag=True,help="List all snapshots for each volume, not just the most recent")
def list_snapshots(project, list_all):
    "List EC2 Snapshots"

    instances = filter_instances(project)

    for i in instances:
        for v in i.volumes.all():
                for s in v.snapshots.all():
                    print(", ".join((
                        s.id,
                        v.id,
                        i.id,
                        s.state,
                        s.progress,
                        s.start_time.strftime("%c")
                    )))

                    if s.state == "completed" and not list_all:break
    return



if __name__ == '__main__':
    cli()
