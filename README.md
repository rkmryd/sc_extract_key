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

Get the websockets requirement
```
pip install -r requirements.txt
```

### Running Python

```
python3 extract_mmp_keys.py <model_name>
```

providing a `<model_name>` that is currently online.

Watch the ouptput for the key.

## License

MIT
