## Getting Started
Welcome to the scheduler App! These instructions are written for OSX. If you're using a Windows or Linux computer, you may have to adjust some commands slightly.

This application is built with Pyspark which utilizes Java 8 or Java 11

## Getting set up
* Ensure you have Java 8 or Java 11. If you're on Mac or Linux you can check your Java version with the `Java -version` command in Terminal. For Windows, open the cmd prompt and use the `Java -version` command. If you do not have Java 8 or Java 11 JDK installed, download one of these versions using your preferred method. Here is a resource if you want to download directly: `https://www.oracle.com/java/technologies/javase-jdk11-downloads.html`.

### Windows
* Note that Oracle JDK 11 does not update system environment variables, so you have to manually update PATH and/or JAVA_HOME after installation. Open Command Prompt window under administrator privilege and type the following command:
`setx -m JAVA_HOME "C:\Program Files\Java\jdk-11.0.7"`

* If the PATH environment variable does not contain an entry to `JAVA_HOME\bin`, type the following command:
`setx -m PATH "%JAVA_HOME%\bin;%PATH%"`

* Then open another command prompt window and type `java –version`. You should now have java jdk 11!

### Mac
* Download Homebrew if you don't already have it: `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"`
See: `https://brew.sh/` for reference.
* Once Homebrew is installed, type: `brew install java11` to get java jdk 11

### Linux
* Download the relevant tarball file from `https://www.oracle.com/java/technologies/javase-jdk11-downloads.html`. For Linux x64 systems `jdk-11.interim.update.patch_linux-x64_bin.tar.gz`
* Change the directory to the location where you want to install the JDK, then move the .tar.gz archive file to the current directory.
* Unpack the tarball and install the downloaded JDK: Example `tar zxvf jdk-11.interim.update.patch_linux-x64_bin.tar.gz`
* Delete the .tar.gz file if you want to save disk space.
* Follow the directions for your particular Linux distribution

### Install Requirements
* Ensure that you have python3 installed, preferably 3.6 or above. Follow instructions on how to install python3 if you do not have it. Python should come with pip as well. Check to ensure that you have pip with the `pip --version` command. 
* Clone this repository.
    * Example using https to connect to github: `https://github.com/carter-ja/scheduler.git`
* Change directory to this repository's location
* If desired, follow the instructions here to create and activate a virtual environment: `https://docs.python.org/3/tutorial/venv.html`
    * This will ensure that the Python modules that you install are only installed in this virtual environment
* If you chose to create a virtual environment, ensure it is activated. In the `scheduler/` root directory, type `pip install -r requirements.txt`. This will install all of the python requirements for the project.
* Run the program by running `python __main__.py` from the `scheduler/` root directory.
