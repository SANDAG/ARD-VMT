@ECHO OFF
cd ..\

if not exist ".venv\" (
    echo "No virtual environment found, setting one up."
    uv venv
)

if not exist "data\" (
    echo "No data folder exists, creating one."
    mkdir data
)

if not exist "src\vmt\" (
    mkdir "src\vmt\"
    copy NUL "__init__.py"
)

echo "Installing repository as package."
uv pip install -e .

if not exist "printout\" (
    mkdir "printout\"
)

::