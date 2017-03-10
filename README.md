### Getting Started
You can install this package from github using `pip`:
```bash
pip install -e git+git@github.com:scitran/python-client.git#egg=scitran_client
```

Check out [this example](examples/fsl_bet.py) to see how to use it! you can run the example locally too
```bash
git clone git@github.com:scitran/python-client.git
cd python-client
virtualenv env
env/bin/pip install .
env/bin/python examples/fsl_bet.py
```

### Contributing
Want to run changes to this code locally? It's pretty easy to get it added to an existing env. the `--upgrade` flag
ensures that your changes will get picked up.
```
pip install --upgrade $WORKSPACE/python-client
```
