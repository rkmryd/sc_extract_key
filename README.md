# SC_EXTRACT_KEYS

A utility that will extract the SC keys. Both versions launch headless Chrome, attach to its built-in debugger via CDP (Chrome DevTools Protocol), set breakpoints in the player's decryption code, and evaluate the key values at runtime. Implemented in JS and Python.

## JavaScript

You need node.js LTS and Chrome/Chromium.

### Setup JS

Get dependencies by running
```bash
npm install
```

### Running JS

```
node extract_mmp_keys.js <model_name>
```
providing a `<model_name>` that is currently online.

Watch the ouptput for the key.

## Python

You need Python 3.10+ and Chrome/Chromium.

### Setup Python

Recommended (uv, creates local .venv with Python 3.10):
```bash
uv venv --python 3.10 .venv
uv pip install --python .venv/bin/python -r requirements.txt
```

Activate the environment:
```bash
source .venv/bin/activate
```

Alternative (already on Python 3.10+):
```bash
pip install -r requirements.txt
```

### Running Python

```
python3 extract_mmp_keys.py <model_name>
```

Or run via uv:
```bash
uv run --python .venv/bin/python extract_mmp_keys.py <model_name>
```

providing a `<model_name>` that is currently online.

Watch the ouptput for the key.

## License

MIT
