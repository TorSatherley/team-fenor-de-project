PROJECT_NAME = project-fenor
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PROFILE = default
PIP:=pip


## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)  

################################################################################################################
# Set Up
## Install pip-audit
pip-audit:
	$(call execute_in_env, $(PIP) install pip-audit)
## Install bandit
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install black
black:
	$(call execute_in_env, $(PIP) install black)

## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install coverage)

## Set up dev requirements (bandit, black & coverage)
dev-setup: bandit black coverage pip-audit

## Run the security test (bandit)
security-test:
	$(call execute_in_env, bandit -lll */*.py *c/*/*.py)

	$(call execute_in_env, pip-audit)

## Run the black code check


##### note - add utils / other file paths as and when created ####


run-black:
	$(call execute_in_env, black  ./src/*.py ./test/*.py)

## Run the unit tests
unit-test:
	@echo ">>> About to create environment: $(SECRET_NAME)..."
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -vvvrP)


## Run the coverage check
check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --cov=src test/)

## Run all checks
run-checks: security-test run-black unit-test check-coverage