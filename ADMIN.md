# This stuff is just for Kelvin's instance running on EC2

```
/opt/apps/pyenv/bin/pip3 install --upgrade --force-reinstall git+https://github.com/kelvinabrokwa/cant-hide-money-bot.git
```

```
sudo systemctl restart cant-hide-money-bot
```

```
journalctl -u cant-hide-money-bot -f
```

```
scp box0:/home/ubuntu/.cant-hide-money-bot/data.prod.db ~/.cant-hide-money-bot
```

```
sudo gunicorn cant_hide_money_bot.json_api:app -b :80
```

```
/opt/apps/pyenv/bin/python -m gunicorn cant_hide_money_bot.json_api:app -b :80
```