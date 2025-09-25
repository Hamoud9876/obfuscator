PROJECT_NAME = GDPR Obfuscator Project
PYTHON_INTERPRETER = python
PIP = pip
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
ACTIVATE_ENV := source venv/bin/activate

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PYTHON_INTERPRETER) -m venv venv; \
	)

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef


## Build the environment requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install pip-tools)
	$(call execute_in_env, pip-compile requirements.in)
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)


# Set Up
## Install bandit
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install pip-audit
pip-audit:
	$(call execute_in_env, $(PIP) install pip-audit)

## Install black
black:
	$(call execute_in_env, $(PIP) install black)

# ## Install flake8
flake8:
	$(call execute_in_env, $(PIP) install flake8)

## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install pytest-cov)

## Set up dev requirements (bandit, pip-audit, black & coverage)
dev-setup: bandit pip-audit black coverage flake8

# Build / Run

## Run the security test (bandit + safety)
security-test:
	$(call execute_in_env, bandit -lll */*.py */*src/*.py)

## Run the security test pip-audit
pip-audit-check:
	$(call execute_in_env, pip-audit)

## Run the black code check
run-black:
	$(call execute_in_env, black --line-length 79 .)

## Run the flake8
run-flake8:
	$(call execute_in_env, flake8 src/ utils/)

## Run the unit tests
unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -vvvrP test/)

## Run the coverage check
check-coverage:
	$(call execute_in_env, PYTHONPATH=. pytest --cov=src --cov=utils test/)

	
## Run all checks
run-checks: security-test run-black unit-test check-coverage pip-audit-check run-flake8