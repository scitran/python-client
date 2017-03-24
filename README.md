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

See our [API Documentation](https://scitran.github.io/python-client/docs/scitran_client/) for more detailed information
about the methods available for the Scitran Client and Flywheel Analyzer.

### Authenticating

The first time you attempt to connect to a Flywheel server, you will have to set up
your credentials. You'll see a message like this:

```bash
> ScitranClient()
You can find your API key by visiting https://flywheel-cni.scitran.stanford.edu/#/profile and scrolling to the bottom of the page.
If your key is blank, then click "Generate API Key"
Enter your API key here: ...
```

After entering your API key, your authentication configuration file at `~/.scitran_client/auth.json`
will be set up. If you are trying to access a flywheel instance besides https://flywheel.scitran.stanford.edu, open an issue on this repo.


### Example
```python
from scitran_client import ScitranClient
client = ScitranClient()
print client.request('projects').json()[0]['label']
# "ADHD Visual Cuing Study"
```
See the [examples](examples) directory for more!


### Contributing
Want to run changes to this code locally? It's pretty easy to get it added to an existing env. the `--upgrade` flag
ensures that your changes will get picked up.
```bash
pip install --upgrade $WORKSPACE/python-client
```

Lint your code with
```bash
make lint
```

Test your code with
```bash
make test
```

Publish a new version of the docs with
```bash
make publish_docs
```
