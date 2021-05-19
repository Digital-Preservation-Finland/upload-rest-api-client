github:
	python3 -mvenv venv; \
	    source venv/bin/activate; \
            pip install --upgrade pip setuptools; \
            pip install -r requirements.txt; \
            pip install .; \
            if [ ! -f ~/.upload.cfg ]; then \
                cp include/upload.cfg ~/.upload.cfg; \
            fi
test:
	python -m pytest tests
