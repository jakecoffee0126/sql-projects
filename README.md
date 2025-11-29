"# sql-with-python" 

Why virtual environments matter

- we won't be able to push the venv folder the github, because the folder is too
large, so this is why we will have the requirements.txt, see 'For requirements.txt workflow' below. Before we push to Github, create the .gitignore file and push the venv in. see below

*** .gitignore ***

Steps to ignore venv before pushing to GitHub
Create a .gitignore file in the root of your project (same level as your venv folder and requirements.txt).

Bash: 
Linux: `touch .gitignore`
Windows:　`echo. > .gitignore`


-- you’re on Windows, and touch is a Linux/macOS command. On Windows, you can create a .gitignore file in a few different ways


Add venv/ to .gitignore Open .gitignore and add:

code: `venv/`

This tells Git to ignore the entire virtual environment folder.

Check if Git is already tracking venv If you accidentally added it before, you need to remove it from Git’s index:

Bash: `git rm -r --cached venv`

(This removes it from Git tracking but keeps the folder locally.)


-- if you are getting this error:
fatal: pathspec 'venv' did not match any files

check few steps below:

1. Add the correct folder name to .gitignore. 
For example:
    `venv/`
    `.venv/`
    `env/`

2. If Git already tracked it If you see the folder in git status, then remove it from the index:
`git rm -r --cached venv`

3. If Git never tracked it
Then you don’t need git rm. Just having it in .gitignore is enough — Git will skip it from now on.





Commit the changes

Bash:
`git add .gitignore`
`git commit -m "Ignore venv folder"`

Push to GitHub

Bash:
`git push origin main`

(Replace main with your branch name if different.)

***  End .gitignore ***

Why virtual environments matter
Isolation of dependencies: Each project can have its own set of packages without interfering with others. For example, one project might need pymysql==1.0.2 while another needs pymysql==0.9.3.

Avoiding version conflicts: If you install everything globally, upgrading one library could break another project. Virtual environments prevent that.

Cleaner system Python: Your system Python stays untouched, so you don’t risk breaking OS tools that rely on it.

Reproducibility: You can freeze the exact versions of packages in a requirements.txt file, making it easy to recreate the same environment elsewhere.

Easy cleanup: If a project is done, you can just delete its virtual environment folder without affecting anything else.

How it works
A virtual environment is basically a self-contained folder with:

Its own Python interpreter

Its own pip

Its own installed packages

You activate it, and then any pip install goes into that environment only.


# Create a virtual environment named venv
`python -m venv venv`

# Activate it
# On macOS/Linux:
*source venv/bin/activate*
# On Windows:
*venv\Scripts\activate*

# Now install packages inside it
`pip install pymysql mysql-connector-python`


When you’re done, you can deactivate with:
deactivate

====================================== 

*** For requirements.txt workflow ***

Step 1: Create a virtual environment

# Create a virtual environment named venv
`python -m venv venv`

# Activate it
# On macOS/Linux:
*source venv/bin/activate*
# On Windows:
*venv\Scripts\activate*


Step 2: Install your dependencies

Inside the activated environment, install the packages you need:
`pip` install pymysql mysql-connector-python`
(You can add any other libraries your project requires.)



Step 3: Freeze dependencies into requirements.txt

Generate a list of all installed packages and their versions:
terminal run:`pip freeze > requirements.txt`

This creates a file like:
pymysql==1.1.0
mysql-connector-python==9.0.0


Step 4: Recreate the environment elsewhere

On another machine (or later), you can recreate the exact environment:

`python -m venv venv`

# Activate it
# On macOS/Linux:
*source venv/bin/activate*
# On Windows:
*venv\Scripts\activate*

`pip install -r requirements.txt`


Typical project structure
>
- my_project/
    - venv/                # virtual environment (ignored in git)
    - requirements.txt     # pinned dependencies
    - main.py              # your Python script
    - README.md            # project notes


Workflow summary
Activate venv → source venv/bin/activate

Install packages → pip install <package>

Save versions → pip freeze > requirements.txt

Recreate later → pip install -r requirements.txt
