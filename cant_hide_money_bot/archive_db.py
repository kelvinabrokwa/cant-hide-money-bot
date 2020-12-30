import boto3
import click

from cant_hide_money_bot import store
from cant_hide_money_bot.std import Mode

VAULT_NAME = 'chmb'
REGION = 'us-west-2'
VAULT_ARN = 'arn:aws:glacier:us-west-2:247436438659:vaults/chmb'


@click.command()
def main():
    glacier = boto3.client('glacier')
    vault = glacier.get_vault(VAULT_ARN)
    db_path = store.db_path(Mode.PROD)
    with open(db_path, 'rb') as f:
        archive = glacier.upload_archive(vaultName=VAULT_NAME, body=f.read())
    print(archive)


if __name__ == '__main__':
    main()
