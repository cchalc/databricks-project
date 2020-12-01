clean-pyc:
		find . | grep -E "(__pycache__|\.pyc|\.pyo)" | xargs rm -rf

generate-test-data:
		docker-compose exec spark_testing python scripts/generate_test_data.py

launch-test-server:
		docker-compose up -d

run-test: launch-test-server clean-pyc
		docker-compose exec spark_testing pytest -s $(testfile)

run-tests: launch-test-server clean-pyc
		docker-compose exec spark_testing pytest -s

wheel:
		python src/setup.py bdist_wheel
