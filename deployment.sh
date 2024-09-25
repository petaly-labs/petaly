
# TEST ON TEST.PyPi INDEX
# upload
python3 -m twine upload --repository testpypi dist/*
# install without dependencies
python3 -m pip install --index-url https://test.pypi.org/simple/ --no-deps petaly

# PyPi Live Index
python3 -m twine upload dist/*
python3 -m pip install petaly
