## Docker

Use the provided `Dockerfile` to build an image that already has the Python dependencies installed and can run the app with `marimo run app.py`.

1. Build the image:

	```bash
	docker build -t aster .
	```

2. Run the app (on port 8080):

	```bash
	docker run --rm -p 8080:8080 aster
	```

3. Open [http://localhost:8080](http://localhost:8080) in your browser and upload the `sync.db` and zipped `App.InFocus` folder as described in the UI.

If you need to work with files outside the container, mount a host directory and point the UI file picker to it.
